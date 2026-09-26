"""领域模型：任务定义、授权与只读查询视图。"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class InstanceState(str, Enum):
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    LEASED = "leased"
    RUNNING = "running"
    CANCELLING = "cancelling"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    DEAD_LETTERED = "dead_lettered"


class CompensationState(str, Enum):
    PENDING = "pending"
    DONE = "done"
    FAILED = "failed"


class MisfirePolicy(str, Enum):
    NONE = "none"
    SCHEDULED = "scheduled"
    BACKFILL = "backfill"


class GapPolicy(str, Enum):
    FORWARD = "forward"
    SKIP = "skip"


class OverlapPolicy(str, Enum):
    EARLY = "early"
    LATE = "late"


@dataclass(frozen=True)
class Capability:
    """单项能力：动作 + 资源范围。资源范围以 (类型, 作用域, 标识) 描述。"""

    action: str
    resource_type: str
    scope: str
    resource_id: str | None = None

    def fingerprint_parts(self) -> tuple[str, str, str, str]:
        return (self.action, self.resource_type, self.scope, self.resource_id or "*")


@dataclass(frozen=True)
class Authorization:
    """某版本的授权事实。新版本通过 :meth:`revised` 追加，不改写历史。

    ``active=False`` 表示租户停用或整体风险隔离；``capabilities`` 为该版本
    生效时的完整授权范围（收缩即新的更小集合）。
    """

    tenant_id: str
    subject_id: str
    version: int
    capabilities: frozenset[Capability]
    active: bool = True

    @property
    def fingerprint(self) -> str:
        payload = sorted(cap.fingerprint_parts() for cap in self.capabilities)
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode()).hexdigest()
        return f"auth-v{self.version}-{digest[:16]}"

    def revised(
        self,
        capabilities: frozenset[Capability] | None = None,
        active: bool | None = None,
    ) -> "Authorization":
        return Authorization(
            tenant_id=self.tenant_id,
            subject_id=self.subject_id,
            version=self.version + 1,
            capabilities=self.capabilities if capabilities is None else capabilities,
            active=self.active if active is None else active,
        )

    def covers(self, required: Capability) -> bool:
        return self.active and required in self.capabilities

    def covers_all(self, required: frozenset[Capability]) -> bool:
        return self.active and required <= self.capabilities


@dataclass(frozen=True)
class JobSpec:
    """任务定义的不可变版本（定义变更通过新版本追加）。"""

    job_id: str
    tenant_id: str
    subject_id: str
    """任务创建者/主体：票据按该主体的当前授权签发。"""
    version: int
    purpose: str
    """任务目的，供值班员追溯“为什么运行”。"""
    cron: str
    timezone: str
    required_capabilities: frozenset[Capability]
    max_runtime_seconds: int
    misfire_policy: MisfirePolicy = MisfirePolicy.SCHEDULED
    max_backfill: int = 3
    gap_policy: GapPolicy = GapPolicy.FORWARD
    overlap_policy: OverlapPolicy = OverlapPolicy.EARLY
    enabled: bool = True

    def validate(self) -> None:
        if self.max_runtime_seconds <= 0:
            raise ValueError("max_runtime_seconds 必须为正")
        if self.max_backfill < 0:
            raise ValueError("max_backfill 不能为负")
        if not self.required_capabilities:
            raise ValueError("任务必须声明至少一项能力范围")
        if not self.purpose.strip():
            raise ValueError("任务必须保存目的说明")


@dataclass(frozen=True)
class LeaseGrant:
    """领取成功后返回给执行器的凭证与上下文。"""

    lease_id: str
    instance_id: str
    token: str
    generation: int
    deadline: datetime
    job: JobSpec
    capabilities: frozenset[Capability]
    auth_version: int


@dataclass(frozen=True)
class LeaseToken:
    """执行器在后续调用中出示的令牌。"""

    lease_id: str
    generation: int
    secret: str

    @classmethod
    def parse(cls, raw: str) -> "LeaseToken":
        lease_id, generation, secret = raw.split(".", 2)
        return cls(lease_id=lease_id, generation=int(generation), secret=secret)

    def render(self) -> str:
        return f"{self.lease_id}.{self.generation}.{self.secret}"


@dataclass(frozen=True)
class EffectView:
    effect_key: str
    action: str
    resource: str
    recorded_at: datetime
    generation: int


@dataclass(frozen=True)
class CompensationView:
    effect_key: str
    action: str
    state: CompensationState
    attempts: int
    last_error: str | None
    updated_at: datetime


@dataclass(frozen=True)
class TimelineEvent:
    event_id: str
    event_type: str
    occurred_at: datetime
    actor_id: str
    detail: dict = field(default_factory=dict)


@dataclass(frozen=True)
class InstanceAudit:
    """值班员视图：一次实例为何运行、用了哪些能力、补偿是否完成。"""

    instance_id: str
    job_id: str
    tenant_id: str
    purpose: str
    state: InstanceState
    scheduled_for_utc: datetime
    window_key: str
    definition_version: int
    ticket_capabilities: frozenset[Capability]
    auth_version_at_issue: int
    auth_fingerprint: str
    lease_generation: int | None
    effects: tuple[EffectView, ...]
    compensations: tuple[CompensationView, ...]
    timeline: tuple[TimelineEvent, ...]

    @property
    def compensation_complete(self) -> bool:
        return bool(self.compensations) and all(
            item.state is CompensationState.DONE for item in self.compensations
        )
