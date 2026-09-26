"""授权事实来源与能力票据。

对应需求：

- 任务定义保存**目的、资源范围、计划时区和最长运行时间**（:class:`JobDefinition`）。
- **每次入队都根据当前授权签发不可转让的短期票据**（:meth:`AuthorizationService.issue_ticket`）。
- 执行器**领取时再次校验版本**：授权收缩、租户停用、风险隔离都会让票据失效
  （:meth:`AuthorizationService.verify`）。

授权模型
--------
- 主体（subject，通常是任务创建者）持有若干能力授予（capability + 资源集合）。
- 授权以**版本号追加**：每次变更产生新版本，历史不覆盖；票据签发时钉住版本。
- 资源串支持精确匹配与 ``前缀/*`` 通配（如 ``workspace:w1/*``）。
- 租户三态：active / disabled / quarantined（风险隔离）。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Mapping

from scheduler.calendar import Schedule
from scheduler.clock import Clock


class TenantStatus(str, Enum):
    ACTIVE = "active"
    DISABLED = "disabled"  # 租户停用
    QUARANTINED = "quarantined"  # 风险隔离


class TicketError(RuntimeError):
    """票据校验失败的基类。"""


class TicketExpired(TicketError):
    pass


class TicketInstanceMismatch(TicketError):
    """票据被拿去用于另一个实例 —— 不可转让约束（P-08-02）。"""


class TicketSignatureInvalid(TicketError):
    pass


class AuthorizationShrunk(TicketError):
    """授权在票据签发后被收缩（能力/资源减少或版本变更）。"""


class TenantUnavailable(TicketError):
    """租户已停用或被风险隔离。"""


class CapabilityDenied(TicketError):
    """入队时当前授权完全不包含任务所需能力。"""


@dataclass(frozen=True)
class CapabilityScope:
    capability: str
    resources: frozenset[str]

    @staticmethod
    def _matches(granted: str, required: str) -> bool:
        """资源精确匹配，或 ``前缀/*`` 通配匹配（如 ``workspace:w1/*``）。"""
        if granted == required:
            return True
        if granted.endswith("/*"):
            return required.startswith(granted[:-1])
        return False


@dataclass(frozen=True)
class JobDefinition:
    """任务定义：目的、资源范围、计划时区、最长运行时间。

    定义按 ``revision`` 版本追加；实例钉住其入队时的修订号，历史不被覆盖。
    """

    job_id: str
    tenant_id: str
    owner_subject_id: str
    purpose: str
    required: tuple[CapabilityScope, ...]
    schedule: Schedule
    max_run_seconds: int
    created_at: datetime
    revision: int = 1

    def __post_init__(self) -> None:
        if self.max_run_seconds <= 0:
            raise ValueError("max_run_seconds 必须为正数")
        if not self.purpose.strip():
            raise ValueError("任务目的不能为空")


@dataclass(frozen=True)
class CapabilityTicket:
    """不可转让的短期能力票据。

    - 绑定 ``instance_id``：出示给其他实例会被拒绝。
    - 钉住 ``authz_version``：领取时与当前授权版本复核。
    - 有明确 ``expires_at``：过期票据不能领取，需要重新签发。
    """

    ticket_id: str
    instance_id: str
    subject_id: str
    tenant_id: str
    capabilities: tuple[CapabilityScope, ...]
    authz_version: int
    issued_at: datetime
    expires_at: datetime
    signature: str = field(repr=False, default="")

    def _signing_payload(self) -> bytes:
        body = {
            "tid": self.ticket_id,
            "iid": self.instance_id,
            "sid": self.subject_id,
            "ten": self.tenant_id,
            "caps": [
                {"c": c.capability, "r": sorted(c.resources)} for c in self.capabilities
            ],
            "ver": self.authz_version,
            "iat": self.issued_at.isoformat(),
            "exp": self.expires_at.isoformat(),
        }
        return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def to_token(self) -> str:
        """编码为可在执行器与调度器之间传递的不透明令牌。"""
        payload = base64.urlsafe_b64encode(self._signing_payload()).rstrip(b"=")
        return f"{payload.decode('ascii')}.{self.signature}"


class AuthorizationService:
    """内存版版本化授权服务；签名密钥由调用方注入。"""

    def __init__(self, clock: Clock, signing_secret: bytes = b"dev-secret") -> None:
        self._clock = clock
        self._secret = signing_secret
        self._versions: dict[str, list[tuple[int, frozenset[CapabilityScope]]]] = {}
        self._tenants: dict[str, TenantStatus] = {}

    # ---- 授权管理（版本追加，不覆盖历史）----
    def register_tenant(self, tenant_id: str, status: TenantStatus = TenantStatus.ACTIVE) -> None:
        self._tenants[tenant_id] = status

    def set_tenant_status(self, tenant_id: str, status: TenantStatus) -> None:
        self._tenants[tenant_id] = status

    def update_authorization(
        self, subject_id: str, tenant_id: str, grants: Mapping[str, list[str] | set[str]]
    ) -> int:
        """发布主体授权的新版本，返回新版本号。

        ``grants`` 将能力映射到资源列表；收缩（删除能力/资源）只需发布更小的集合。
        """
        self.register_tenant(tenant_id, self._tenants.get(tenant_id, TenantStatus.ACTIVE))
        history = self._versions.setdefault(subject_id, [])
        version = (history[-1][0] + 1) if history else 1
        scopes = frozenset(
            CapabilityScope(capability, frozenset(resources))
            for capability, resources in grants.items()
        )
        history.append((version, scopes))
        return version

    def current_version(self, subject_id: str) -> int:
        history = self._versions.get(subject_id)
        if not history:
            raise CapabilityDenied(f"主体没有任何授权版本：{subject_id}")
        return history[-1][0]

    def _grants_at(self, subject_id: str, version: int | None = None) -> tuple[int, frozenset[CapabilityScope]]:
        history = self._versions.get(subject_id)
        if not history:
            raise CapabilityDenied(f"主体没有任何授权版本：{subject_id}")
        if version is None:
            return history[-1]
        for ver, scopes in history:
            if ver == version:
                return ver, scopes
        raise CapabilityDenied(f"授权版本不存在：{subject_id}@{version}")

    def _tenant_gate(self, tenant_id: str) -> None:
        status = self._tenants.get(tenant_id, TenantStatus.ACTIVE)
        if status is TenantStatus.DISABLED:
            raise TenantUnavailable(f"租户已停用：{tenant_id}")
        if status is TenantStatus.QUARANTINED:
            raise TenantUnavailable(f"租户处于风险隔离：{tenant_id}")

    # ---- 票据签发与校验 ----
    def issue_ticket(
        self,
        ticket_id: str,
        instance_id: str,
        job: JobDefinition,
        ttl: timedelta = timedelta(minutes=10),
    ) -> CapabilityTicket:
        """按**当前**授权版本为一次入队签发短期票据。

        票据只包含任务所需的资源，但任务所需的**每一项能力及其全部资源**都必须
        被当前授权覆盖；任何一项缺失都抛出 :class:`CapabilityDenied`（收缩不留
        半成品权限；此时实例尚未产生任何外部效果，应直接取消）。
        """
        self._tenant_gate(job.tenant_id)
        version, grants = self._grants_at(job.owner_subject_id)
        granted_map = {scope.capability: scope.resources for scope in grants}
        granted_for_job: list[CapabilityScope] = []
        for needed in job.required:
            allowed = granted_map.get(needed.capability, frozenset())
            missing = {
                resource
                for resource in needed.resources
                if not any(CapabilityScope._matches(g, resource) for g in allowed)
            }
            if missing:
                raise CapabilityDenied(
                    f"当前授权版本 v{version} 缺少任务 {job.job_id} 的能力 "
                    f"{needed.capability} 资源：{sorted(missing)}"
                )
            granted_for_job.append(needed)
        now = self._clock.now()
        ticket = CapabilityTicket(
            ticket_id=ticket_id,
            instance_id=instance_id,
            subject_id=job.owner_subject_id,
            tenant_id=job.tenant_id,
            capabilities=tuple(granted_for_job),
            authz_version=version,
            issued_at=now,
            expires_at=now + ttl,
        )
        signature = hmac.new(
            self._secret, ticket._signing_payload(), hashlib.sha256
        ).hexdigest()
        return CapabilityTicket(**{**ticket.__dict__, "signature": signature})

    def parse_token(self, token: str) -> CapabilityTicket:
        try:
            payload_b64, signature = token.split(".", 1)
            padding = "=" * (-len(payload_b64) % 4)
            raw = base64.urlsafe_b64decode(payload_b64 + padding)
            body = json.loads(raw)
        except (ValueError, json.JSONDecodeError) as exc:
            raise TicketSignatureInvalid("票据格式无法解析") from exc
        caps = tuple(
            CapabilityScope(item["c"], frozenset(item["r"])) for item in body["caps"]
        )
        ticket = CapabilityTicket(
            ticket_id=body["tid"],
            instance_id=body["iid"],
            subject_id=body["sid"],
            tenant_id=body["ten"],
            capabilities=caps,
            authz_version=body["ver"],
            issued_at=datetime.fromisoformat(body["iat"]),
            expires_at=datetime.fromisoformat(body["exp"]),
            signature=signature,
        )
        expected = hmac.new(
            self._secret, ticket._signing_payload(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise TicketSignatureInvalid("票据签名不匹配")
        return ticket

    def verify(
        self,
        token: str,
        *,
        instance_id: str,
        at: datetime | None = None,
        enforce_ttl: bool = True,
    ) -> CapabilityTicket:
        """执行器领取时的复核：签名、实例绑定、有效期、授权版本与租户状态。

        任何一项不满足都抛出对应的 :class:`TicketError` 子类，调度器据此取消实例。
        ``enforce_ttl=False`` 用于运行期心跳：运行期存活由租约约束，但授权收缩与
        租户停用/隔离仍然必须生效。
        """
        ticket = self.parse_token(token)
        now = at or self._clock.now()
        if ticket.instance_id != instance_id:
            raise TicketInstanceMismatch(
                f"票据绑定实例 {ticket.instance_id}，不能用于 {instance_id}"
            )
        if enforce_ttl and now >= ticket.expires_at:
            raise TicketExpired(f"票据已于 {ticket.expires_at.isoformat()} 过期")
        self._tenant_gate(ticket.tenant_id)
        current_version, current_grants = self._grants_at(ticket.subject_id)
        if current_version != ticket.authz_version:
            # 授权有新版本：必须确认票据中的每项能力/资源仍被当前版本完整授予，
            # 否则即视为收缩（授权扩张不影响已签发票据，仍可运行）。
            current_map = {s.capability: s.resources for s in current_grants}
            for scope in ticket.capabilities:
                allowed = current_map.get(scope.capability, frozenset())
                for resource in scope.resources:
                    if not any(CapabilityScope._matches(g, resource) for g in allowed):
                        raise AuthorizationShrunk(
                            f"授权 v{ticket.authz_version}→v{current_version} 已收缩："
                            f"{scope.capability}:{resource}"
                        )
        return ticket
