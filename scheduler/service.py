"""能力票据调度服务核心。

状态全部由 :mod:`scheduler.eventlog` 中的事件重放得到（事件溯源），因此调度器
重启后可以恢复：任务下一触发点、未决（可能已超时）的租约、死信实例与补偿进度。

实例生命周期
------------
``queued`` → ``leased`` → ``running`` → ``completed``
     │           │            │
     │           └─ 领取/心跳复核失败、授权收缩、租户停用/隔离、超过最长运行时间：
     │                · 尚无已确认外部效果 → ``cancelled``
     │                · 已有部分效果       → ``compensating``
     │                                       → ``compensated``（全部补偿完成）
     │                                       → ``dead_lettered``（补偿无法完成，
     │                                          人工处理后 requeue 回到补偿链）
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from scheduler.authz import (
    AuthorizationService,
    AuthorizationShrunk,
    CapabilityDenied,
    CapabilityScope,
    JobDefinition,
    TenantUnavailable,
    TicketError,
    TicketExpired,
)
from scheduler.calendar import MissPolicy, Schedule
from scheduler.clock import Clock
from scheduler.eventlog import EventStore


class InstanceStatus(str, Enum):
    QUEUED = "queued"
    LEASED = "leased"
    RUNNING = "running"
    CANCELLING = "cancelling"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    COMPENSATED = "compensated"
    DEAD_LETTERED = "dead_lettered"


TERMINAL_STATUSES = {
    InstanceStatus.COMPLETED,
    InstanceStatus.CANCELLED,
    InstanceStatus.COMPENSATED,
    InstanceStatus.DEAD_LETTERED,
}


class SchedulingError(RuntimeError):
    pass


class LeaseUnavailable(SchedulingError):
    """租约仍被其他执行器有效持有。"""


class LeaseLost(SchedulingError):
    """租约已过期并被重新领取（或被撤销），原执行器必须停止提交副作用。"""


class InvalidState(SchedulingError):
    pass


@dataclass
class Effect:
    effect_id: str
    idempotency_key: str
    resource: str
    summary: str
    recorded_at: datetime
    executor_id: str
    compensated: bool = False


@dataclass
class LeaseView:
    lease_id: str
    executor_id: str
    attempt: int
    claimed_at: datetime
    expires_at: datetime
    revoked: bool = False


@dataclass
class CompensationStep:
    effect_id: str
    action: str
    note: str
    at: datetime
    by: str


@dataclass
class Compensation:
    compensation_id: str
    started_at: datetime
    reason: str
    steps: dict[str, CompensationStep] = field(default_factory=dict)
    completed_at: datetime | None = None


@dataclass
class InstanceState:
    instance_id: str
    job_id: str
    tenant_id: str
    window_key: str
    trigger_at: datetime
    purpose: str
    revision: int
    status: InstanceStatus
    enqueued_at: datetime
    ticket_token: str | None = None
    ticket_attempt: int = 0
    authz_version_at_enqueue: int | None = None
    capabilities: tuple[CapabilityScope, ...] = ()
    cancel_reason: str | None = None
    effects: dict[str, Effect] = field(default_factory=dict)
    lease: LeaseView | None = None
    lease_attempts: int = 0
    compensation: Compensation | None = None
    dead_letter_reason: str | None = None
    run_started_at: datetime | None = None
    trail: list[dict[str, Any]] = field(default_factory=list)

    @property
    def has_confirmed_effects(self) -> bool:
        return bool(self.effects)


class SchedulerService:
    """门面服务：调度、签票、租约、副作用、补偿、死信与审计。"""

    def __init__(
        self,
        clock: Clock,
        authz: AuthorizationService,
        store: EventStore | None = None,
        *,
        lease_timeout: timedelta = timedelta(minutes=5),
        ticket_ttl: timedelta = timedelta(minutes=10),
    ) -> None:
        self._clock = clock
        self._authz = authz
        self._store = store or EventStore()
        self._lease_timeout = lease_timeout
        self._ticket_ttl = ticket_ttl
        self._lock = threading.RLock()
        self._jobs: dict[str, JobDefinition] = {}
        self._windows: dict[tuple[str, str], datetime] = {}
        self._instances: dict[str, InstanceState] = {}
        self._replay()

    # ================= 重放与恢复 =================
    def _replay(self) -> None:
        for event in self._store:
            self._apply(event, persist=False)

    def _emit(self, event_type: str, aggregate_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        event = self._store.append(
            event_type, aggregate_id, payload, occurred_at=self._clock.now()
        )
        self._apply(event, persist=False)
        return event

    def _apply(self, event: dict[str, Any], *, persist: bool) -> None:
        etype = event["event_type"]
        at = datetime.fromisoformat(event["occurred_at"])
        p = event["payload"]
        handler = getattr(self, f"_on_{etype.replace('.', '_')}", None)
        if handler is not None:
            handler(p, at)

    # ---- 事件投影 ----
    def _on_job_defined(self, p: dict[str, Any], at: datetime) -> None:
        self._jobs[p["job_id"]] = self._job_from_payload(p, at, revision=p["revision"])

    def _on_job_updated(self, p: dict[str, Any], at: datetime) -> None:
        self._jobs[p["job_id"]] = self._job_from_payload(p, at, revision=p["revision"])

    def _on_window_opened(self, p: dict[str, Any], at: datetime) -> None:
        self._windows[(p["job_id"], p["window_key"])] = datetime.fromisoformat(p["trigger_at"])

    def _on_instance_enqueued(self, p: dict[str, Any], at: datetime) -> None:
        inst = InstanceState(
            instance_id=p["instance_id"],
            job_id=p["job_id"],
            tenant_id=p["tenant_id"],
            window_key=p["window_key"],
            trigger_at=datetime.fromisoformat(p["trigger_at"]),
            purpose=p["purpose"],
            revision=p["revision"],
            status=InstanceStatus.QUEUED,
            enqueued_at=at,
            authz_version_at_enqueue=p.get("authz_version"),
        )
        inst.trail.append({"at": at, "event": "instance.enqueued", "detail": p})
        self._instances[inst.instance_id] = inst

    def _on_ticket_issued(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        inst.ticket_token = p["token"]
        inst.ticket_attempt = p["attempt"]
        inst.authz_version_at_enqueue = p["authz_version"]
        inst.capabilities = tuple(
            CapabilityScope(c["capability"], frozenset(c["resources"]))
            for c in p["capabilities"]
        )
        inst.trail.append({"at": at, "event": "ticket.issued", "detail": p})

    def _on_ticket_issue_denied(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        inst.trail.append({"at": at, "event": "ticket.issue_denied", "detail": p})

    def _on_lease_claimed(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        inst.lease_attempts = p["attempt"]
        inst.lease = LeaseView(
            lease_id=p["lease_id"],
            executor_id=p["executor_id"],
            attempt=p["attempt"],
            claimed_at=at,
            expires_at=datetime.fromisoformat(p["lease_expires_at"]),
        )
        if inst.run_started_at is None:
            inst.run_started_at = at
        inst.status = InstanceStatus.LEASED
        inst.trail.append({"at": at, "event": "lease.claimed", "detail": p})

    def _on_lease_reclaimed(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        if inst.lease is not None:
            inst.lease.revoked = True
        inst.lease_attempts = p["attempt"]
        inst.lease = LeaseView(
            lease_id=p["lease_id"],
            executor_id=p["executor_id"],
            attempt=p["attempt"],
            claimed_at=at,
            expires_at=datetime.fromisoformat(p["lease_expires_at"]),
        )
        # 重领是新执行器的新一轮运行，最长运行时间预算按新 attempt 重新起算。
        inst.run_started_at = at
        inst.status = InstanceStatus.LEASED
        inst.trail.append({"at": at, "event": "lease.reclaimed", "detail": p})

    def _on_lease_heartbeat(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        if inst.lease is not None:
            inst.lease.expires_at = datetime.fromisoformat(p["lease_expires_at"])
        inst.trail.append({"at": at, "event": "lease.heartbeat", "detail": p})

    def _on_effect_recorded(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        inst.status = InstanceStatus.RUNNING
        effect = Effect(
            effect_id=p["effect_id"],
            idempotency_key=p["idempotency_key"],
            resource=p["resource"],
            summary=p["summary"],
            recorded_at=at,
            executor_id=p["executor_id"],
        )
        inst.effects[p["idempotency_key"]] = effect
        inst.trail.append({"at": at, "event": "effect.recorded", "detail": p})

    def _on_cancel_requested(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        inst.trail.append({"at": at, "event": "cancel.requested", "detail": p})

    def _on_instance_cancelled(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        inst.status = InstanceStatus.CANCELLED
        inst.cancel_reason = p["reason"]
        if inst.lease is not None:
            inst.lease.revoked = True
        inst.trail.append({"at": at, "event": "instance.cancelled", "detail": p})

    def _on_compensation_started(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        inst.status = InstanceStatus.COMPENSATING
        inst.cancel_reason = p["reason"]
        if inst.lease is not None:
            inst.lease.revoked = True
        inst.compensation = Compensation(
            compensation_id=p["compensation_id"], started_at=at, reason=p["reason"]
        )
        inst.trail.append({"at": at, "event": "compensation.started", "detail": p})

    def _on_compensation_step_recorded(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        assert inst.compensation is not None
        step = CompensationStep(
            effect_id=p["effect_id"],
            action=p["action"],
            note=p.get("note", ""),
            at=at,
            by=p["by"],
        )
        inst.compensation.steps[p["effect_id"]] = step
        for effect in inst.effects.values():
            if effect.effect_id == p["effect_id"]:
                effect.compensated = True
                break
        inst.trail.append(
            {"at": at, "event": "compensation.step.recorded", "detail": p}
        )

    def _on_compensation_completed(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        assert inst.compensation is not None
        inst.compensation.completed_at = at
        inst.status = InstanceStatus.COMPENSATED
        inst.trail.append({"at": at, "event": "compensation.completed", "detail": p})

    def _on_instance_completed(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        inst.status = InstanceStatus.COMPLETED
        inst.trail.append({"at": at, "event": "instance.completed", "detail": p})

    def _on_instance_dead_lettered(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        inst.status = InstanceStatus.DEAD_LETTERED
        inst.dead_letter_reason = p["reason"]
        inst.trail.append({"at": at, "event": "instance.dead_lettered", "detail": p})

    def _on_instance_requeued(self, p: dict[str, Any], at: datetime) -> None:
        inst = self._instances[p["instance_id"]]
        inst.status = InstanceStatus.COMPENSATING
        inst.dead_letter_reason = None
        inst.trail.append({"at": at, "event": "instance.requeued", "detail": p})

    @staticmethod
    def _job_from_payload(p: dict[str, Any], at: datetime, *, revision: int) -> JobDefinition:
        sp = p["schedule"]
        schedule = Schedule(
            cron=sp["cron"],
            timezone=sp["timezone"],
            miss_policy=MissPolicy(sp.get("miss_policy", MissPolicy.CATCH_UP_LATEST.value)),
            gap_skip=sp.get("gap_skip", "after"),
        )
        required = tuple(
            CapabilityScope(item["capability"], frozenset(item["resources"]))
            for item in p["required"]
        )
        return JobDefinition(
            job_id=p["job_id"],
            tenant_id=p["tenant_id"],
            owner_subject_id=p["owner_subject_id"],
            purpose=p["purpose"],
            required=required,
            schedule=schedule,
            max_run_seconds=p["max_run_seconds"],
            created_at=at,
            revision=revision,
        )

    # ================= 任务定义 =================
    def register_job(self, job: JobDefinition) -> None:
        with self._lock:
            if job.job_id in self._jobs:
                raise InvalidState(f"任务已存在：{job.job_id}")
            self._emit("job.defined", job.job_id, self._job_payload(job))

    def update_job(self, job_id: str, **changes: Any) -> JobDefinition:
        """发布任务定义的新修订版本（目的/范围/计划/最长运行时间），历史保留。

        已排队、已领取的实例继续钉住旧修订；只有新窗口的实例使用新修订。
        """
        with self._lock:
            old = self._jobs[job_id]
            data = {
                "purpose": changes.get("purpose", old.purpose),
                "required": changes.get("required", old.required),
                "schedule": changes.get("schedule", old.schedule),
                "max_run_seconds": changes.get("max_run_seconds", old.max_run_seconds),
            }
            new = JobDefinition(
                job_id=old.job_id,
                tenant_id=old.tenant_id,
                owner_subject_id=old.owner_subject_id,
                purpose=data["purpose"],
                required=tuple(
                    s if isinstance(s, CapabilityScope) else CapabilityScope(
                        s["capability"], frozenset(s["resources"])
                    )
                    for s in data["required"]
                ),
                schedule=data["schedule"],
                max_run_seconds=data["max_run_seconds"],
                created_at=old.created_at,
                revision=old.revision + 1,
            )
            self._emit("job.updated", job_id, self._job_payload(new))
            return new

    @staticmethod
    def _job_payload(job: JobDefinition) -> dict[str, Any]:
        return {
            "job_id": job.job_id,
            "tenant_id": job.tenant_id,
            "owner_subject_id": job.owner_subject_id,
            "purpose": job.purpose,
            "required": [
                {"capability": s.capability, "resources": sorted(s.resources)}
                for s in job.required
            ],
            "schedule": {
                "cron": job.schedule.cron,
                "timezone": job.schedule.timezone,
                "miss_policy": job.schedule.miss_policy.value,
                "gap_skip": job.schedule.gap_skip,
            },
            "max_run_seconds": job.max_run_seconds,
            "revision": job.revision,
        }

    def get_job(self, job_id: str) -> JobDefinition:
        return self._jobs[job_id]

    # ================= 调度：开窗、错过触发、补跑 =================
    def tick(self) -> list[str]:
        """推进调度：为到期窗口入队，并清理票据已失效的排队实例。

        返回本次新入队（或入队即取消）的实例 ID 列表。幂等：重复 tick 安全。
        """
        with self._lock:
            now = self._clock.now()
            touched: list[str] = []
            for job_id, job in list(self._jobs.items()):
                cursor = self._last_window_cursor(job_id, job.created_at)
                for trigger_at, window_key in job.schedule.missed_windows(cursor, now):
                    instance_id = self._open_and_enqueue(job, trigger_at, window_key)
                    if instance_id is not None:
                        touched.append(instance_id)
            # 排队实例的票据失效主动发现（尚未产生外部效果 → 直接取消）。
            for inst in list(self._instances.values()):
                if inst.status is InstanceStatus.QUEUED:
                    self._revoke_if_ticket_invalid(inst)
                elif inst.status in (InstanceStatus.LEASED, InstanceStatus.RUNNING):
                    # 已领取实例也要在收缩/停用/隔离的下一个 tick 立即失效，
                    # 不必等待执行器心跳或租约超时。
                    self._revoke_active_if_unauthorized(inst)
            return touched

    def _last_window_cursor(self, job_id: str, fallback: datetime) -> datetime:
        triggers = [t for (jid, _), t in self._windows.items() if jid == job_id]
        return max(triggers) if triggers else fallback

    def _open_and_enqueue(
        self, job: JobDefinition, trigger_at: datetime, window_key: str
    ) -> str | None:
        # P-08-01：同一任务与计划窗口只能生成一个实例。
        if (job.job_id, window_key) in self._windows:
            return None
        instance_id = f"inst-{job.job_id}-{window_key}"
        self._emit(
            "window.opened",
            job.job_id,
            {"job_id": job.job_id, "window_key": window_key, "trigger_at": trigger_at.isoformat()},
        )
        self._emit(
            "instance.enqueued",
            instance_id,
            {
                "instance_id": instance_id,
                "job_id": job.job_id,
                "tenant_id": job.tenant_id,
                "window_key": window_key,
                "trigger_at": trigger_at.isoformat(),
                "purpose": job.purpose,
                "revision": job.revision,
                "authz_version": None,  # 签票成功后补齐
            },
        )
        if not self._issue_ticket_for(instance_id, job):
            # 入队时绝不可能已有外部效果：直接取消，等待值班员审计。
            self._cancel_without_effects(
                instance_id, "capability_denied", "入队时当前授权不包含任务所需能力"
            )
        return instance_id

    def _issue_ticket_for(self, instance_id: str, job: JobDefinition) -> bool:
        """按当前授权为实例签发新票据并记录事件。

        成功返回 True；被拒（授权不含所需能力、租户停用/隔离）只记录
        ``ticket.issue_denied`` 事件并返回 False——取消还是补偿由调用方根据
        是否已有外部效果决定（入队时必无效果；重领时可能有部分效果）。
        """
        inst = self._instances[instance_id]
        try:
            ticket = self._authz.issue_ticket(
                ticket_id=f"ticket-{uuid.uuid4().hex[:12]}",
                instance_id=instance_id,
                job=job,
                ttl=self._ticket_ttl,
            )
        except (CapabilityDenied, TenantUnavailable) as exc:
            inst.ticket_attempt += 1
            self._emit(
                "ticket.issue_denied",
                instance_id,
                {"instance_id": instance_id, "reason": str(exc)},
            )
            return False
        inst.ticket_attempt += 1
        self._emit(
            "ticket.issued",
            instance_id,
            {
                "ticket_id": ticket.ticket_id,
                "instance_id": instance_id,
                "attempt": inst.ticket_attempt,
                "authz_version": ticket.authz_version,
                "capabilities": [
                    {"capability": c.capability, "resources": sorted(c.resources)}
                    for c in ticket.capabilities
                ],
                "issued_at": ticket.issued_at.isoformat(),
                "expires_at": ticket.expires_at.isoformat(),
                "token": ticket.to_token(),
            },
        )
        return True

    # ================= 领取、心跳、重领 =================
    def claim(self, instance_id: str, executor_id: str) -> LeaseView:
        """执行器领取实例。成功返回租约；票据复核失败则取消/补偿并抛出异常。

        - 租约仍被他人有效持有 → :class:`LeaseUnavailable`。
        - 租约已超时 → 原子地**重新领取**：旧租约作废，按当前授权重签票据并复核，
          保证授权收缩、租户停用/隔离后旧执行器不能借重领复活。
        """
        with self._lock:
            inst = self._require(instance_id)
            now = self._clock.now()
            if inst.status in TERMINAL_STATUSES:
                raise InvalidState(f"实例已处于终态 {inst.status.value}：{instance_id}")
            if inst.status in (InstanceStatus.CANCELLING, InstanceStatus.COMPENSATING):
                raise InvalidState("实例正在取消/补偿中，不能领取")
            if inst.lease is not None and not inst.lease.revoked and inst.lease.expires_at > now:
                if inst.lease.executor_id != executor_id:
                    raise LeaseUnavailable(
                        f"租约由 {inst.lease.executor_id} 持有至 {inst.lease.expires_at.isoformat()}"
                    )
                return inst.lease  # 同一执行器重复领取是幂等的
            reclaim = inst.lease is not None
            return self._issue_lease(inst, executor_id, reclaim)

    def _issue_lease(self, inst: InstanceState, executor_id: str, reclaim: bool) -> LeaseView:
        """复核授权后发放租约。

        - 首次领取：复用入队时签发的票据（短期、未过期），直接复核。
        - 票据过期 或 租约超时重领：按**当前**授权为同一实例重签票据后再复核；
          重签被拒或复核失败时，无效果直接取消，已有部分效果进入补偿链。
        """
        job = self._jobs[inst.job_id]
        token = inst.ticket_token
        need_reissue = reclaim or token is None
        if token is not None and not reclaim:
            try:
                self._authz.verify(token, instance_id=inst.instance_id)
            except TicketExpired:
                need_reissue = True
            except TicketError as exc:
                self._handle_claim_failure(inst, self._failure_reason(exc), str(exc))
                raise
        if need_reissue:
            issued = self._issue_ticket_for(inst.instance_id, job)
            inst = self._instances[inst.instance_id]  # 投影后重新取
            if not issued:
                # 重签被拒：无效果直接取消，有部分效果进入补偿链。
                self._handle_claim_failure(
                    inst, "capability_denied", "当前授权拒绝为该实例签发票据"
                )
                raise CapabilityDenied("当前授权拒绝为该实例签发票据")
            try:
                ticket = self._authz.verify(
                    inst.ticket_token, instance_id=inst.instance_id
                )
            except TicketError as exc:
                self._handle_claim_failure(inst, self._failure_reason(exc), str(exc))
                raise
        else:
            ticket = self._authz.verify(inst.ticket_token, instance_id=inst.instance_id)
        attempt = inst.lease_attempts + 1
        lease_id = f"lease-{inst.instance_id}-{attempt}"
        expires_at = self._clock.now() + self._lease_timeout
        payload = {
            "lease_id": lease_id,
            "instance_id": inst.instance_id,
            "executor_id": executor_id,
            "attempt": attempt,
            "lease_expires_at": expires_at.isoformat(),
            "previous_executor_id": inst.lease.executor_id if reclaim else None,
            "authz_version": ticket.authz_version,
        }
        self._emit("lease.reclaimed" if reclaim else "lease.claimed", inst.instance_id, payload)
        return self._instances[inst.instance_id].lease  # type: ignore[return-value]

    def _handle_claim_failure(self, inst: InstanceState, reason: str, detail: str) -> None:
        """领取时复核失败：无效果直接取消，有部分效果进入补偿链。"""
        self._emit(
            "cancel.requested",
            inst.instance_id,
            {"instance_id": inst.instance_id, "reason": reason, "detail": detail, "at_claim": True},
        )
        if inst.has_confirmed_effects:
            self._start_compensation(inst.instance_id, reason)
        else:
            self._cancel_without_effects(inst.instance_id, reason, detail)

    def heartbeat(self, instance_id: str, lease_id: str) -> datetime:
        """执行器续租。每次心跳都再次复核授权；失败即撤销租约并取消/补偿。

        旧执行器在租约被重领后续心跳会收到 :class:`LeaseLost`，从而不会继续
        提交副作用（P-08-03 的执行侧保障）。
        """
        with self._lock:
            inst = self._require(instance_id)
            self._require_active_lease(inst, lease_id)
            # 最长运行时间（任务定义的一部分）：超时即终止本次执行。
            job = self._jobs[inst.job_id]
            if inst.run_started_at is not None and (
                self._clock.now() - inst.run_started_at
            ).total_seconds() > job.max_run_seconds:
                reason = "max_runtime_exceeded"
                detail = f"超过最长运行时间 {job.max_run_seconds}s"
                self._emit(
                    "cancel.requested",
                    instance_id,
                    {"instance_id": instance_id, "reason": reason, "detail": detail,
                     "at_heartbeat": True},
                )
                if inst.has_confirmed_effects:
                    self._start_compensation(instance_id, reason)
                else:
                    self._cancel_without_effects(instance_id, reason, detail)
                raise LeaseLost(detail)
            try:
                assert inst.ticket_token is not None
                self._authz.verify(
                    inst.ticket_token, instance_id=instance_id, enforce_ttl=False
                )
            except TicketError as exc:
                reason = self._failure_reason(exc)
                self._emit(
                    "cancel.requested",
                    instance_id,
                    {"instance_id": instance_id, "reason": reason, "detail": str(exc),
                     "at_heartbeat": True},
                )
                if inst.has_confirmed_effects:
                    self._start_compensation(instance_id, reason)
                else:
                    self._cancel_without_effects(instance_id, reason, str(exc))
                raise LeaseLost(f"授权复核失败，租约已撤销：{exc}") from exc
            expires_at = self._clock.now() + self._lease_timeout
            self._emit(
                "lease.heartbeat",
                instance_id,
                {"instance_id": instance_id, "lease_id": lease_id,
                 "lease_expires_at": expires_at.isoformat()},
            )
            return expires_at

    @staticmethod
    def _failure_reason(exc: TicketError) -> str:
        if isinstance(exc, AuthorizationShrunk):
            return "authz_shrunk"
        if isinstance(exc, TenantUnavailable):
            return "tenant_quarantined" if "隔离" in str(exc) else "tenant_disabled"
        if isinstance(exc, TicketExpired):
            return "ticket_expired"
        return "ticket_invalid"

    def _revoke_if_ticket_invalid(self, inst: InstanceState) -> None:
        """排队实例的授权主动巡检。

        票据自然过期不在此处理（领取时会重签）；只有授权收缩、租户停用/隔离
        才取消一个尚未运行的排队实例。
        """
        if not inst.ticket_token:
            return
        try:
            self._authz.verify(
                inst.ticket_token, instance_id=inst.instance_id, enforce_ttl=False
            )
        except TicketExpired:
            return
        except TicketError as exc:
            reason = self._failure_reason(exc)
            self._cancel_without_effects(inst.instance_id, reason, str(exc))

    def _revoke_active_if_unauthorized(self, inst: InstanceState) -> None:
        """已领取（leased/running）实例的授权主动巡检。

        票据自然过期不算撤销（执行器可在租约边界重签）；授权收缩、租户停用/隔离
        立即作废旧租约：无效果直接取消，已有部分效果进入补偿链。
        """
        if not inst.ticket_token:
            return
        try:
            self._authz.verify(
                inst.ticket_token, instance_id=inst.instance_id, enforce_ttl=False
            )
        except TicketExpired:
            return
        except TicketError as exc:
            reason = self._failure_reason(exc)
            self._emit(
                "cancel.requested",
                inst.instance_id,
                {"instance_id": inst.instance_id, "reason": reason,
                 "detail": str(exc), "at_scheduler_tick": True},
            )
            if inst.has_confirmed_effects:
                self._start_compensation(inst.instance_id, reason)
            else:
                self._cancel_without_effects(inst.instance_id, reason, str(exc))

    # ================= 外部效果（幂等）=================
    def record_effect(
        self,
        instance_id: str,
        lease_id: str,
        idempotency_key: str,
        resource: str,
        summary: str,
    ) -> Effect:
        """登记一个**已确认**的外部效果。

        以 ``idempotency_key`` 幂等：租约超时后被另一执行器重新领取，只要复用
        同一个幂等键，就不会重复登记/提交同一副作用（P-08-03）。
        """
        with self._lock:
            inst = self._require(instance_id)
            self._require_active_lease(inst, lease_id)
            existing = inst.effects.get(idempotency_key)
            if existing is not None:
                return existing
            # 效果只能落在票据实际授予的资源上。
            allowed_resources = {r for scope in inst.capabilities for r in scope.resources}
            if not any(
                CapabilityScope._matches(granted, resource)
                for granted in allowed_resources
            ):
                raise InvalidState(f"效果资源不在票据授权范围内：{resource}")
            effect_id = f"effect-{uuid.uuid4().hex[:12]}"
            self._emit(
                "effect.recorded",
                instance_id,
                {
                    "effect_id": effect_id,
                    "instance_id": instance_id,
                    "lease_id": lease_id,
                    "idempotency_key": idempotency_key,
                    "resource": resource,
                    "summary": summary,
                    "executor_id": inst.lease.executor_id,
                },
            )
            return self._instances[instance_id].effects[idempotency_key]

    def complete(self, instance_id: str, lease_id: str) -> None:
        with self._lock:
            inst = self._require(instance_id)
            self._require_active_lease(inst, lease_id)
            self._emit("instance.completed", instance_id, {"instance_id": instance_id})

    # ================= 取消与补偿 =================
    def request_cancel(self, instance_id: str, reason: str = "manual", detail: str = "") -> None:
        """值班员主动取消。无效果直接取消；有部分效果进入可审计补偿链。"""
        with self._lock:
            inst = self._require(instance_id)
            if inst.status in TERMINAL_STATUSES:
                return
            self._emit(
                "cancel.requested",
                instance_id,
                {"instance_id": instance_id, "reason": reason, "detail": detail,
                 "by": "operator"},
            )
            if inst.has_confirmed_effects:
                self._start_compensation(instance_id, reason)
            else:
                self._cancel_without_effects(instance_id, reason, detail)

    def _cancel_without_effects(self, instance_id: str, reason: str, detail: str) -> None:
        inst = self._instances[instance_id]
        if inst.status in TERMINAL_STATUSES:
            return
        self._emit(
            "instance.cancelled",
            instance_id,
            {"instance_id": instance_id, "reason": reason, "detail": detail},
        )

    def _start_compensation(self, instance_id: str, reason: str) -> None:
        inst = self._instances[instance_id]
        if inst.compensation is not None or inst.status in TERMINAL_STATUSES - {
            InstanceStatus.DEAD_LETTERED
        }:
            return
        pending = [e.effect_id for e in inst.effects.values() if not e.compensated]
        compensation_id = f"comp-{uuid.uuid4().hex[:12]}"
        self._emit(
            "compensation.started",
            instance_id,
            {
                "compensation_id": compensation_id,
                "instance_id": instance_id,
                "reason": reason,
                "effect_ids": [e.effect_id for e in inst.effects.values()],
                "pending_effect_ids": pending,
            },
        )

    def compensate_effect(
        self, instance_id: str, effect_id: str, action: str, note: str = "", by: str = "operator"
    ) -> None:
        """登记某一已确认效果的补偿动作；全部效果补偿完则补偿链完成。"""
        with self._lock:
            inst = self._require(instance_id)
            if inst.compensation is None:
                raise InvalidState("实例没有进行中的补偿链")
            effect_ids = {e.effect_id for e in inst.effects.values()}
            if effect_id not in effect_ids:
                raise InvalidState(f"效果不属于该实例：{effect_id}")
            self._emit(
                "compensation.step.recorded",
                instance_id,
                {
                    "instance_id": instance_id,
                    "compensation_id": inst.compensation.compensation_id,
                    "effect_id": effect_id,
                    "action": action,
                    "note": note,
                    "by": by,
                },
            )
            inst = self._instances[instance_id]
            if all(e.compensated for e in inst.effects.values()):
                self._emit(
                    "compensation.completed",
                    instance_id,
                    {"instance_id": instance_id,
                     "compensation_id": inst.compensation.compensation_id},
                )

    def dead_letter(self, instance_id: str, reason: str) -> None:
        """补偿暂时无法完成（如下游不可用、需要人工介入）时进入死信，保留审计。"""
        with self._lock:
            inst = self._require(instance_id)
            if inst.status not in (InstanceStatus.COMPENSATING, InstanceStatus.CANCELLING):
                raise InvalidState("只有补偿中的实例可以进入死信")
            self._emit(
                "instance.dead_lettered",
                instance_id,
                {"instance_id": instance_id, "reason": reason},
            )

    def requeue_dead_letter(self, instance_id: str) -> None:
        """人工处理后把死信实例重新投回补偿链。"""
        with self._lock:
            inst = self._require(instance_id)
            if inst.status is not InstanceStatus.DEAD_LETTERED:
                raise InvalidState("只有死信实例可以重新投回")
            self._emit(
                "instance.requeued",
                instance_id,
                {"instance_id": instance_id},
            )

    # ================= 查询与审计 =================
    def _require(self, instance_id: str) -> InstanceState:
        inst = self._instances.get(instance_id)
        if inst is None:
            raise InvalidState(f"未知实例：{instance_id}")
        return inst

    def _require_active_lease(self, inst: InstanceState, lease_id: str) -> None:
        if inst.lease is None or inst.lease.lease_id != lease_id or inst.lease.revoked:
            raise LeaseLost("租约已失效或被重新领取")
        if inst.lease.expires_at <= self._clock.now():
            raise LeaseLost(
                f"租约已于 {inst.lease.expires_at.isoformat()} 超时，"
                "请停止执行并由新执行器重新领取"
            )

    def get_instance(self, instance_id: str) -> InstanceState:
        return self._require(instance_id)

    def list_instances(self, job_id: str | None = None) -> list[InstanceState]:
        items = self._instances.values()
        if job_id is not None:
            items = [i for i in items if i.job_id == job_id]
        return sorted(items, key=lambda i: i.enqueued_at)

    def list_dead_letters(self) -> list[InstanceState]:
        return [i for i in self._instances.values() if i.status is InstanceStatus.DEAD_LETTERED]

    def list_reclaimable_leases(self) -> list[InstanceState]:
        """租约已超时、等待执行器重新领取的未决实例（重启后同样可查）。"""
        now = self._clock.now()
        return [
            i
            for i in self._instances.values()
            if i.lease is not None
            and not i.lease.revoked
            and i.lease.expires_at <= now
            and i.status in (InstanceStatus.LEASED, InstanceStatus.RUNNING)
        ]

    def next_fire_times(self) -> dict[str, datetime]:
        """每个任务的下一触发点（UTC），由计划与已开窗游标推导，重启即恢复。"""
        now = self._clock.now()
        result: dict[str, datetime] = {}
        for job_id, job in self._jobs.items():
            cursor = self._last_window_cursor(job_id, job.created_at)
            result[job_id] = job.schedule.next_after(cursor)
        return result

    def explain_instance(self, instance_id: str) -> dict[str, Any]:
        """值班员审计视图：为何运行、用了哪些能力、效果与补偿、完整事件轨迹。"""
        inst = self._require(instance_id)
        job = self._jobs.get(inst.job_id)
        comp = inst.compensation
        return {
            "instance_id": inst.instance_id,
            "status": inst.status.value,
            "why": {
                "job_id": inst.job_id,
                "purpose": inst.purpose,
                "tenant_id": inst.tenant_id,
                "owner_subject_id": job.owner_subject_id if job else None,
                "window_key": inst.window_key,
                "scheduled_trigger_at": inst.trigger_at.isoformat(),
                "enqueued_at": inst.enqueued_at.isoformat(),
                "definition_revision": inst.revision,
                "authz_version_at_enqueue": inst.authz_version_at_enqueue,
            },
            "capabilities_used": [
                {"capability": s.capability, "resources": sorted(s.resources)}
                for s in inst.capabilities
            ],
            "lease": None
            if inst.lease is None
            else {
                "lease_id": inst.lease.lease_id,
                "executor_id": inst.lease.executor_id,
                "attempt": inst.lease.attempt,
                "claimed_at": inst.lease.claimed_at.isoformat(),
                "expires_at": inst.lease.expires_at.isoformat(),
                "revoked": inst.lease.revoked,
            },
            "effects": [
                {
                    "effect_id": e.effect_id,
                    "idempotency_key": e.idempotency_key,
                    "resource": e.resource,
                    "summary": e.summary,
                    "recorded_at": e.recorded_at.isoformat(),
                    "executor_id": e.executor_id,
                    "compensated": e.compensated,
                }
                for e in inst.effects.values()
            ],
            "cancel_reason": inst.cancel_reason,
            "compensation": None
            if comp is None
            else {
                "compensation_id": comp.compensation_id,
                "reason": comp.reason,
                "started_at": comp.started_at.isoformat(),
                "completed_at": comp.completed_at.isoformat() if comp.completed_at else None,
                "steps": [
                    {
                        "effect_id": s.effect_id,
                        "action": s.action,
                        "note": s.note,
                        "by": s.by,
                        "at": s.at.isoformat(),
                    }
                    for s in comp.steps.values()
                ],
            },
            "dead_letter_reason": inst.dead_letter_reason,
            "trail": [
                {"at": str(t["at"].isoformat()), "event": t["event"], "detail": t["detail"]}
                for t in inst.trail
            ],
        }

    def recovery_summary(self) -> dict[str, Any]:
        """调度器重启后一页看清：下一触发点、未决超时租约、死信、补偿中实例。"""
        return {
            "jobs": [
                {"job_id": jid, "next_fire_at": nxt.isoformat()}
                for jid, nxt in sorted(self.next_fire_times().items())
            ],
            "pending_leases": [
                {"instance_id": i.instance_id, "lease_id": i.lease.lease_id,
                 "executor_id": i.lease.executor_id,
                 "expires_at": i.lease.expires_at.isoformat()}
                for i in self.list_reclaimable_leases()
            ],
            "dead_letters": [
                {"instance_id": i.instance_id, "reason": i.dead_letter_reason}
                for i in self.list_dead_letters()
            ],
            "compensating": [
                i.instance_id
                for i in self._instances.values()
                if i.status is InstanceStatus.COMPENSATING
            ],
        }

    def close(self) -> None:
        self._store.close()
