"""能力票据调度核心服务。

所有写操作都在单个 ``begin immediate`` 事务内完成裁决、状态迁移与事件追加；
时间由调用方以 ``now`` 注入，便于对停机恢复、夏令时等场景做确定性测试。
"""
from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .errors import LeaseLost, TicketInvalid
from .models import (
    Authorization,
    Capability,
    CompensationState,
    CompensationView,
    EffectView,
    GapPolicy,
    InstanceAudit,
    InstanceState,
    JobSpec,
    LeaseGrant,
    LeaseToken,
    MisfirePolicy,
    OverlapPolicy,
    TimelineEvent,
)
from .store import Store, iso, parse_iso
from .timeeval import (
    CronSpec,
    ResolvedTrigger,
    UTC,
    enumerate_windows,
    next_window,
    window_key,
)

TICKET_TTL = timedelta(minutes=10)
TICKET_REISSUE_BEFORE_EXPIRY = timedelta(seconds=30)


def _utc(now: datetime) -> datetime:
    if now.tzinfo is None:
        raise ValueError("now 必须带时区")
    return now.astimezone(UTC)


def _secret_hash(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


class SchedulerService:
    def __init__(self, store: Store, lease_seconds: int = 60):
        self.store = store
        self.lease_seconds = lease_seconds

    # ------------------------------------------------------------------ 定义

    def register_job(self, spec: JobSpec, now: datetime) -> JobSpec:
        spec.validate()
        # 校验时区与 cron 可解析。
        ZoneInfo(spec.timezone)
        CronSpec.parse(spec.cron)
        at = _utc(now)
        payload = json.dumps(self._job_payload(spec), ensure_ascii=False, sort_keys=True)
        with self.store.transaction() as conn:
            existing = conn.execute(
                "select version from job_definitions where job_id=? order by version desc limit 1",
                (spec.job_id,),
            ).fetchone()
            expected = 0 if existing is None else existing["version"]
            if spec.version != expected + 1:
                raise ValueError(
                    f"任务 {spec.job_id} 下一个定义版本必须是 {expected + 1}，收到 {spec.version}"
                )
            conn.execute(
                "insert into job_definitions values(?, ?, ?, ?, ?, ?)",
                (spec.job_id, spec.tenant_id, spec.version, payload, int(spec.enabled), iso(at)),
            )
            if existing is None:
                trigger = self._compute_next(spec, at)
                conn.execute(
                    "insert into schedule_state(job_id, last_window_utc, next_window_utc, updated_at)"
                    " values(?, NULL, ?, ?)",
                    (spec.job_id, iso(trigger.scheduled_utc), iso(at)),
                )
            else:
                # 定义变更：下一触发点按新计划重算，未开窗窗口自然作废。
                trigger = self._compute_next(spec, at)
                conn.execute(
                    "update schedule_state set next_window_utc=?, last_window_utc=?, updated_at=?"
                    " where job_id=?",
                    (iso(trigger.scheduled_utc), None, iso(at), spec.job_id),
                )
            self.store.append_event(
                conn,
                "job.version_published",
                spec.job_id,
                at,
                "scheduler",
                detail={"version": spec.version, "cron": spec.cron, "timezone": spec.timezone,
                        "enabled": spec.enabled},
            )
            if not spec.enabled:
                # 定义被停用：租户内最新定义已停用的任务，其未决实例立即分流
                # （无效果取消、有效果补偿）；其他仍启用的任务不受影响。
                self._contraction_scan(conn, at, spec.tenant_id, None, reason="job_disabled")
        return spec

    def set_tenant_status(
        self, tenant_id: str, status: str, now: datetime, reason: str | None = None
    ) -> int:
        """变更租户状态（停用/风险隔离/恢复）。

        非 active 时同事务处理该租户所有未终结实例，返回处理数量。
        """

        if status not in ("active", "disabled", "quarantined"):
            raise ValueError("租户状态只能是 active/disabled/quarantined")
        at = _utc(now)
        affected = 0
        with self.store.transaction() as conn:
            conn.execute(
                "insert into tenants(tenant_id, status, reason, updated_at) values(?, ?, ?, ?)"
                " on conflict(tenant_id) do update set status=excluded.status,"
                " reason=excluded.reason, updated_at=excluded.updated_at",
                (tenant_id, status, reason, iso(at)),
            )
            self.store.append_event(
                conn,
                "authorization.revised",
                tenant_id,
                at,
                "scheduler",
                detail={"tenant_status": status, "reason": reason},
            )
            if status != "active":
                affected = self._contraction_scan(
                    conn, at, tenant_id, None, reason=f"tenant_{status}"
                )
        return affected

    def put_authorization(self, auth: Authorization, now: datetime) -> int:
        """登记新版本授权；若相对上一版发生收缩或停用，同事务处理受影响实例。"""

        at = _utc(now)
        payload = json.dumps(
            {
                "tenant_id": auth.tenant_id,
                "subject_id": auth.subject_id,
                "version": auth.version,
                "active": auth.active,
                "capabilities": [c.__dict__ for c in sorted(auth.capabilities, key=lambda c: c.fingerprint_parts())],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        affected = 0
        with self.store.transaction() as conn:
            latest = conn.execute(
                "select payload from authorizations where tenant_id=? and subject_id=?"
                " order by version desc limit 1",
                (auth.tenant_id, auth.subject_id),
            ).fetchone()
            expected = 0 if latest is None else json.loads(latest["payload"])["version"]
            if auth.version != expected + 1:
                raise ValueError(
                    f"授权下一个版本必须是 {expected + 1}，收到 {auth.version}"
                )
            conn.execute(
                "insert into authorizations values(?, ?, ?, ?, ?, ?, ?)",
                (
                    auth.tenant_id,
                    auth.subject_id,
                    auth.version,
                    int(auth.active),
                    auth.fingerprint,
                    payload,
                    iso(at),
                ),
            )
            self.store.append_event(
                conn,
                "authorization.revised",
                f"{auth.tenant_id}:{auth.subject_id}",
                at,
                "scheduler",
                instance_id=None,
                detail={"version": auth.version, "active": auth.active, "fingerprint": auth.fingerprint},
            )
            previous = None if latest is None else json.loads(latest["payload"])
            contracted = (
                latest is None
                or not auth.active
                or not auth.capabilities
                >= frozenset(Capability(**c) for c in previous["capabilities"])
            )
            if contracted:
                affected = self._contraction_scan(
                    conn, at, auth.tenant_id, auth.subject_id, reason="authorization_revised"
                )
        return affected

    # ------------------------------------------------------------ 开窗/补跑

    def tick(self, now: datetime) -> dict[str, list[str]]:
        """推进调度：开窗、按补跑策略补实例、租约软/硬超时、cancelling 收敛。

        返回各类动作产生的实例/租约 id，便于观测。票据到期本身不单独扫描：
        领取时按当前授权重新裁决（重签或拒绝）。
        """

        at = _utc(now)
        opened: list[str] = []
        enqueued: list[str] = []
        expired_leases: list[str] = []

        with self.store.transaction() as conn:
            jobs = conn.execute(
                "select job_id, version, payload from job_definitions j"
                " where enabled=1 and version=("
                " select max(version) from job_definitions where job_id=j.job_id)"
            ).fetchall()
            for row in jobs:
                spec = self._spec_from_payload(row["job_id"], row["payload"], row["version"])
                state = conn.execute(
                    "select last_window_utc, next_window_utc from schedule_state where job_id=?",
                    (spec.job_id,),
                ).fetchone()
                next_at = parse_iso(state["next_window_utc"])
                if next_at > at:
                    continue
                # 缺口前移最多一个小时：扫描下界回退两小时，保证名义 cron 时间
                # 落在扫描范围内；再以水位严格过滤，避免重复或预支历史窗口。
                first_run = state["last_window_utc"] is None
                watermark = next_at if first_run else parse_iso(state["last_window_utc"])
                horizon = at + timedelta(minutes=1)
                windows = enumerate_windows(
                    CronSpec.parse(spec.cron),
                    ZoneInfo(spec.timezone),
                    next_at - timedelta(hours=2),
                    horizon,
                    spec.gap_policy.value,
                    spec.overlap_policy.value,
                )
                comparator = (lambda w: w.scheduled_utc >= watermark) if first_run else (
                    lambda w: w.scheduled_utc > watermark
                )
                due = [
                    w
                    for w in windows
                    if comparator(w) and w.scheduled_utc <= at
                ]
                if not due:
                    continue
                self._open_windows(conn, spec, due, at)
                # 补跑策略裁决哪些窗口需要实例。
                chosen = self._apply_misfire(conn, spec, due, at)
                for resolved in chosen:
                    instance_id = self._enqueue(conn, spec, resolved, at, actor="scheduler")
                    if instance_id:
                        enqueued.append(instance_id)
                opened.extend(window_key(spec.job_id, w.scheduled_utc) for w in due)
                # 水位推进到所有已裁决窗口（含 gap_skip 与补跑放弃的窗口）。
                last_utc = max(w.scheduled_utc for w in due)
                following = self._compute_next(spec, last_utc)
                conn.execute(
                    "update schedule_state set last_window_utc=?, next_window_utc=?, updated_at=?"
                    " where job_id=?",
                    (iso(last_utc), iso(following.scheduled_utc), iso(at), spec.job_id),
                )

            # 软超时（未心跳）与硬超时（超过任务最长运行时间）分别处理：
            # 软超时交还队列等待重领（世代号隔离保证不重复副作用）；
            # 硬超时不得刷新运行预算，按取消语义处理（有效果则进补偿链）。
            for lease in conn.execute(
                "select lease_id, instance_id, generation, hard_deadline from execution_leases"
                " where status='active' and (deadline<=? or hard_deadline<=?)",
                (iso(at), iso(at)),
            ).fetchall():
                conn.execute(
                    "update execution_leases set status='expired', released_at=? where lease_id=?",
                    (iso(at), lease["lease_id"]),
                )
                if parse_iso(lease["hard_deadline"]) <= at:
                    self.store.append_event(
                        conn,
                        "lease.expired",
                        lease["instance_id"],
                        at,
                        "scheduler",
                        instance_id=lease["instance_id"],
                        detail={
                            "lease_id": lease["lease_id"],
                            "generation": lease["generation"],
                            "reason": "hard_deadline",
                        },
                    )
                    self._handle_contraction(
                        conn, lease["instance_id"], at, reason="max_runtime_exceeded"
                    )
                else:
                    inst = conn.execute(
                        "select state from instances where instance_id=?", (lease["instance_id"],)
                    ).fetchone()
                    if inst is not None and inst["state"] in (
                        InstanceState.LEASED.value,
                        InstanceState.RUNNING.value,
                    ):
                        conn.execute(
                            "update instances set state=?, updated_at=? where instance_id=?",
                            (InstanceState.QUEUED.value, iso(at), lease["instance_id"]),
                        )
                    self.store.append_event(
                        conn,
                        "lease.expired",
                        lease["instance_id"],
                        at,
                        "scheduler",
                        instance_id=lease["instance_id"],
                        detail={"lease_id": lease["lease_id"], "generation": lease["generation"]},
                    )
                expired_leases.append(lease["lease_id"])

            # cancelling 扫描：活跃租约已作废的实例在此终结；若期间已有效果则转补偿。
            cancelling = conn.execute(
                "select instance_id from instances where state=?",
                (InstanceState.CANCELLING.value,),
            ).fetchall()
            for row in cancelling:
                active = conn.execute(
                    "select 1 from execution_leases where instance_id=? and status='active'",
                    (row["instance_id"],),
                ).fetchone()
                if active:
                    continue
                effects = conn.execute(
                    "select count(*) as c from effect_receipts where instance_id=?",
                    (row["instance_id"],),
                ).fetchone()["c"]
                if effects == 0:
                    self._cancel_without_effects(conn, row["instance_id"], at, "cancel_confirmed")
                else:
                    self._begin_compensation(conn, row["instance_id"], at, "effects_surfaced_during_cancel")

        return {"windows": opened, "enqueued": enqueued, "expired_leases": expired_leases}

    def _open_windows(
        self, conn, spec: JobSpec, due: list[ResolvedTrigger], at: datetime
    ) -> None:
        for resolved in due:
            key = window_key(spec.job_id, resolved.scheduled_utc)
            suppressed = resolved.kind == "gap_skip"
            existing = conn.execute(
                "select 1 from windows where window_key=?", (key,)
            ).fetchone()
            if existing:
                continue
            conn.execute(
                "insert into windows(window_key, job_id, scheduled_utc, local_time,"
                " resolution, instance_id, suppressed, created_at)"
                " values(?, ?, ?, ?, ?, NULL, ?, ?)",
                (
                    key,
                    spec.job_id,
                    iso(resolved.scheduled_utc),
                    resolved.local_time.isoformat(),
                    resolved.kind,
                    int(suppressed),
                    iso(at),
                ),
            )
            self.store.append_event(
                conn,
                "window.suppressed" if suppressed else "window.opened",
                key,
                at,
                "scheduler",
                detail={
                    "job_id": spec.job_id,
                    "local_time": resolved.local_time.isoformat(),
                    "resolution": resolved.kind,
                },
            )

    def _apply_misfire(
        self,
        conn,
        spec: JobSpec,
        due: list[ResolvedTrigger],
        at: datetime,
    ) -> list[ResolvedTrigger]:
        """补跑策略：正常到期 + 停机错过窗口的取舍。"""

        runnable = [w for w in due if w.kind != "gap_skip"]
        if spec.misfire_policy is MisfirePolicy.NONE:
            # 不补跑：只执行“准点”窗口（cron 最小粒度为一分钟，宽限 60 秒），
            # 早于本 tick 超过 60 秒的窗口视为停机错过，记录抑制事件后放弃。
            grace = timedelta(seconds=60)
            result: list[ResolvedTrigger] = []
            for w in runnable:
                if at - w.scheduled_utc < grace:
                    result.append(w)
                else:
                    self.store.append_event(
                        conn,
                        "window.suppressed",
                        window_key(spec.job_id, w.scheduled_utc),
                        at,
                        "scheduler",
                        detail={"reason": "misfire_none", "job_id": spec.job_id},
                    )
            return result
        if spec.misfire_policy is MisfirePolicy.SCHEDULED:
            # 只补最近一个窗口（以最近计划瞬间为依据），跳过已存在实例的。
            for w in reversed(runnable):
                key = window_key(spec.job_id, w.scheduled_utc)
                if conn.execute("select 1 from instances where window_key=?", (key,)).fetchone():
                    continue
                return [w]
            return []
        # backfill：逐窗口补跑，超过上限的旧窗口放弃。
        missing: list[ResolvedTrigger] = []
        for w in runnable:
            key = window_key(spec.job_id, w.scheduled_utc)
            if not conn.execute(
                "select 1 from instances where window_key=?", (key,)
            ).fetchone():
                missing.append(w)
        if len(missing) > spec.max_backfill:
            dropped = missing[: len(missing) - spec.max_backfill]
            for w in dropped:
                self.store.append_event(
                    conn,
                    "window.suppressed",
                    window_key(spec.job_id, w.scheduled_utc),
                    at,
                    "scheduler",
                    detail={"reason": "backfill_limit", "job_id": spec.job_id},
                )
            missing = missing[len(dropped):]
        return missing

    def _enqueue(
        self, conn, spec: JobSpec, resolved: ResolvedTrigger, at: datetime, actor: str
    ) -> str | None:
        key = window_key(spec.job_id, resolved.scheduled_utc)
        # 同一计划窗口只能生成一个实例（窗口行上的实例唯一约束兜底）。
        duplicate = conn.execute(
            "select instance_id from instances where window_key=?", (key,)
        ).fetchone()
        if duplicate:
            return None
        instance_id = "inst-" + uuid.uuid4().hex[:16]
        conn.execute(
            "insert into instances(instance_id, job_id, tenant_id, window_key, state,"
            " scheduled_for_utc, purpose, definition_version, enqueue_attempts,"
            " created_at, updated_at)"
            " values(?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
            (
                instance_id,
                spec.job_id,
                spec.tenant_id,
                key,
                InstanceState.QUEUED.value,
                iso(resolved.scheduled_utc),
                spec.purpose,
                spec.version,
                iso(at),
                iso(at),
            ),
        )
        conn.execute(
            "update windows set instance_id=? where window_key=?", (instance_id, key)
        )
        self.store.append_event(
            conn,
            "instance.enqueued",
            instance_id,
            at,
            actor,
            instance_id=instance_id,
            detail={
                "window_key": key,
                "scheduled_for_utc": iso(resolved.scheduled_utc),
                "definition_version": spec.version,
                "purpose": spec.purpose,
                "misfire": at > resolved.scheduled_utc,
            },
        )
        # 按当前授权签发不可转让的短期票据。
        self._issue_ticket(conn, spec, instance_id, at)
        return instance_id

    def _issue_ticket(self, conn, spec: JobSpec, instance_id: str, at: datetime) -> None:
        auth = self._latest_auth(conn, spec.tenant_id, spec.subject_id)
        ticket_id = "tkt-" + uuid.uuid4().hex[:16]
        expires = at + TICKET_TTL
        if auth is None or not auth.covers_all(spec.required_capabilities):
            # 授权缺失或已收缩：记录一张 denied 票据并立即取消未生效实例。
            scope = frozenset() if auth is None else auth.capabilities
            conn.execute(
                "insert into capability_tickets(ticket_id, instance_id, auth_version,"
                " auth_fingerprint, scope_json, issued_at, expires_at, status, revoke_reason)"
                " values(?, ?, ?, ?, ?, ?, ?, 'denied', ?)",
                (
                    ticket_id,
                    instance_id,
                    0 if auth is None else auth.version,
                    "none" if auth is None else auth.fingerprint,
                    self._scope_json(scope),
                    iso(at),
                    iso(expires),
                    "authorization_missing_or_shrunk",
                ),
            )
            self.store.append_event(
                conn,
                "ticket.denied",
                instance_id,
                at,
                "scheduler",
                instance_id=instance_id,
                detail={"ticket_id": ticket_id, "reason": "authorization_missing_or_shrunk"},
            )
            self._cancel_without_effects(conn, instance_id, at, reason="authorization_shrunk_at_enqueue")
            return
        conn.execute(
            "insert into capability_tickets(ticket_id, instance_id, auth_version,"
            " auth_fingerprint, scope_json, issued_at, expires_at, status)"
            " values(?, ?, ?, ?, ?, ?, ?, 'valid')",
            (
                ticket_id,
                instance_id,
                auth.version,
                auth.fingerprint,
                self._scope_json(auth.capabilities),
                iso(at),
                iso(expires),
            ),
        )
        self.store.append_event(
            conn,
            "ticket.issued",
            instance_id,
            at,
            "scheduler",
            instance_id=instance_id,
            detail={
                "ticket_id": ticket_id,
                "auth_version": auth.version,
                "auth_fingerprint": auth.fingerprint,
                "expires_at": iso(expires),
                "scope": [c.fingerprint_parts() for c in sorted(auth.capabilities, key=lambda c: c.fingerprint_parts())],
            },
        )

    # --------------------------------------------------------------- 领取执行

    def claim(self, instance_id: str, executor_id: str, now: datetime) -> LeaseGrant:
        """执行器领取实例；领取时再次复核版本、授权、租户状态与票据有效期。"""

        at = _utc(now)
        with self.store.transaction() as conn:
            inst = conn.execute("select * from instances where instance_id=?", (instance_id,)).fetchone()
            if inst is None:
                raise KeyError(f"实例不存在：{instance_id}")
            if inst["state"] not in (InstanceState.QUEUED.value, InstanceState.LEASED.value, InstanceState.RUNNING.value):
                raise TicketInvalid(f"实例处于 {inst['state']}，不可领取")
            spec = self._load_spec(conn, inst["job_id"], inst["definition_version"])
            decision = self._recheck(conn, spec, inst, at)
            if decision is not None:
                # 未产生外部效果：直接取消；已有效果：进入补偿链。
                self._handle_contraction(conn, inst["instance_id"], at, reason=decision)
                raise TicketInvalid(f"领取复核未通过：{decision}")
            # 已有活跃租约：若已过软截止，本次领取直接接管（等价于先跑一次
            # 超时扫描），由世代号隔离旧执行者；硬截止已过则按收缩分流。
            active = conn.execute(
                "select * from execution_leases where instance_id=? and status='active'",
                (instance_id,),
            ).fetchone()
            if active is not None:
                if parse_iso(active["hard_deadline"]) <= at:
                    conn.execute(
                        "update execution_leases set status='expired', released_at=? where lease_id=?",
                        (iso(at), active["lease_id"]),
                    )
                    self.store.append_event(
                        conn,
                        "lease.expired",
                        instance_id,
                        at,
                        "scheduler",
                        instance_id=instance_id,
                        detail={
                            "lease_id": active["lease_id"],
                            "generation": active["generation"],
                            "reason": "hard_deadline_at_claim",
                        },
                    )
                    self._handle_contraction(conn, instance_id, at, reason="max_runtime_exceeded")
                    raise TicketInvalid("实例超过最长运行时间，已进入取消/补偿")
                if parse_iso(active["deadline"]) > at:
                    raise TicketInvalid("实例已有活跃租约")
                conn.execute(
                    "update execution_leases set status='expired', released_at=? where lease_id=?",
                    (iso(at), active["lease_id"]),
                )
                conn.execute(
                    "update instances set state=?, updated_at=? where instance_id=?",
                    (InstanceState.QUEUED.value, iso(at), instance_id),
                )
                self.store.append_event(
                    conn,
                    "lease.expired",
                    instance_id,
                    at,
                    "scheduler",
                    instance_id=instance_id,
                    detail={"lease_id": active["lease_id"], "generation": active["generation"]},
                )
            generation_row = conn.execute(
                "select coalesce(max(generation), 0) as g from execution_leases where instance_id=?",
                (instance_id,),
            ).fetchone()
            generation = generation_row["g"] + 1
            lease_id = "lse-" + uuid.uuid4().hex[:16]
            secret = secrets.token_urlsafe(24)
            deadline = at + timedelta(seconds=self.lease_seconds)
            hard_deadline = at + timedelta(seconds=spec.max_runtime_seconds)
            ticket = conn.execute(
                "select * from capability_tickets where instance_id=? order by issued_at desc limit 1",
                (instance_id,),
            ).fetchone()
            conn.execute(
                "insert into execution_leases(lease_id, instance_id, generation, secret_hash,"
                " status, executor_id, lease_seconds, claimed_at, deadline, hard_deadline,"
                " heartbeat_at, released_at)"
                " values(?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, NULL)",
                (
                    lease_id,
                    instance_id,
                    generation,
                    _secret_hash(secret),
                    executor_id,
                    self.lease_seconds,
                    iso(at),
                    iso(deadline),
                    iso(hard_deadline),
                    iso(at),
                ),
            )
            conn.execute(
                "update instances set state=?, updated_at=? where instance_id=?",
                (InstanceState.LEASED.value, iso(at), instance_id),
            )
            self.store.append_event(
                conn,
                "lease.claimed",
                instance_id,
                at,
                executor_id,
                instance_id=instance_id,
                detail={
                    "lease_id": lease_id,
                    "generation": generation,
                    "ticket_id": ticket["ticket_id"],
                    "auth_version": ticket["auth_version"],
                    "deadline": iso(deadline),
                },
            )
            scope = {
                Capability(**item)
                for item in json.loads(ticket["scope_json"])
            }
            return LeaseGrant(
                lease_id=lease_id,
                instance_id=instance_id,
                token=LeaseToken(lease_id, generation, secret).render(),
                generation=generation,
                deadline=deadline,
                job=spec,
                capabilities=frozenset(scope),
                auth_version=ticket["auth_version"],
            )

    def start_running(self, token: str, now: datetime) -> None:
        at = _utc(now)
        with self.store.transaction() as conn:
            lease = self._authenticate(conn, token, at)
            conn.execute(
                "update instances set state=?, updated_at=? where instance_id=?",
                (InstanceState.RUNNING.value, iso(at), lease["instance_id"]),
            )

    def heartbeat(self, token: str, now: datetime) -> datetime:
        """续租；返回新的软截止时间。超过任务最长运行时间则拒绝。

        实例处于 cancelling（优雅取消）时仍允许心跳，执行器应通过
        :meth:`cancel_requested` 轮询并自行完成检查点后调用 :meth:`release`。
        """

        at = _utc(now)
        with self.store.transaction() as conn:
            lease = self._authenticate(conn, token, at, allow_cancelling=True)
            if parse_iso(lease["hard_deadline"]) <= at:
                raise LeaseLost("超过任务最长运行时间，租约不再续期")
            new_deadline = at + timedelta(seconds=lease["lease_seconds"])
            conn.execute(
                "update execution_leases set deadline=?, heartbeat_at=? where lease_id=?",
                (iso(new_deadline), iso(at), lease["lease_id"]),
            )
            self.store.append_event(
                conn,
                "lease.heartbeat",
                lease["instance_id"],
                at,
                lease["executor_id"],
                instance_id=lease["instance_id"],
                detail={"lease_id": lease["lease_id"], "generation": lease["generation"], "deadline": iso(new_deadline)},
            )
            return new_deadline

    def record_effect(
        self, token: str, effect_key: str, action: str, resource: str, now: datetime
    ) -> EffectView:
        """登记一个已确认的外部效果；同键重复提交幂等返回既有回执。

        执行器应在外部副作用实际成功后调用；租约失效（旧世代）一律拒绝，
        防止超时被重领后双提交。
        """

        at = _utc(now)
        with self.store.transaction() as conn:
            lease = self._authenticate(conn, token, at, allow_cancelling=True)
            instance_id = lease["instance_id"]
            existing = conn.execute(
                "select * from effect_receipts where instance_id=? and effect_key=?",
                (instance_id, effect_key),
            ).fetchone()
            if existing is not None:
                return EffectView(
                    effect_key=existing["effect_key"],
                    action=existing["action"],
                    resource=existing["resource"],
                    recorded_at=parse_iso(existing["recorded_at"]),
                    generation=existing["generation"],
                )
            conn.execute(
                "insert into effect_receipts values(?, ?, ?, ?, ?, ?)",
                (instance_id, effect_key, action, resource, iso(at), lease["generation"]),
            )
            view = EffectView(effect_key, action, resource, at, lease["generation"])
            self.store.append_event(
                conn,
                "effect.recorded",
                instance_id,
                at,
                lease["executor_id"],
                instance_id=instance_id,
                detail={
                    "effect_key": effect_key,
                    "action": action,
                    "resource": resource,
                    "generation": lease["generation"],
                },
            )
            return view

    def complete(self, token: str, now: datetime) -> None:
        at = _utc(now)
        with self.store.transaction() as conn:
            lease = self._authenticate(conn, token, at)
            instance_id = lease["instance_id"]
            conn.execute(
                "update instances set state=?, updated_at=? where instance_id=?",
                (InstanceState.COMPLETED.value, iso(at), instance_id),
            )
            conn.execute(
                "update execution_leases set status='released', released_at=? where lease_id=?",
                (iso(at), lease["lease_id"]),
            )
            self.store.append_event(
                conn,
                "instance.completed",
                instance_id,
                at,
                lease["executor_id"],
                instance_id=instance_id,
                detail={"lease_id": lease["lease_id"], "generation": lease["generation"]},
            )

    # ---------------------------------------------------------- 取消与补偿

    def request_cancel(
        self,
        instance_id: str,
        now: datetime,
        actor_id: str = "operator",
        reason: str = "manual",
        immediate: bool = True,
    ) -> None:
        """取消实例。

        - ``immediate=True``（默认）：立即作废活跃租约，无效果直接取消，
          有部分效果进入补偿链；
        - ``immediate=False``：优雅取消，实例进入 cancelling，租约保留到
          截止；执行器通过 :meth:`cancel_requested` 观察到信号，自行检查点后
          调用 :meth:`release`，由系统按效果有无终结或转补偿；长时间不释放
          的租约最终由硬截止（最长运行时间）强制收敛。
        """

        at = _utc(now)
        with self.store.transaction() as conn:
            inst = conn.execute("select * from instances where instance_id=?", (instance_id,)).fetchone()
            if inst is None:
                raise KeyError(f"实例不存在：{instance_id}")
            if inst["state"] in (
                InstanceState.COMPLETED.value,
                InstanceState.CANCELLED.value,
                InstanceState.DEAD_LETTERED.value,
            ):
                return
            self.store.append_event(
                conn,
                "instance.cancel_requested",
                instance_id,
                at,
                actor_id,
                instance_id=instance_id,
                detail={"reason": reason, "from_state": inst["state"], "immediate": immediate},
            )
            effects = conn.execute(
                "select count(*) as c from effect_receipts where instance_id=?", (instance_id,)
            ).fetchone()["c"]
            running = inst["state"] in (InstanceState.LEASED.value, InstanceState.RUNNING.value)
            if not immediate and running:
                # 优雅取消：保留租约，执行器自行检查点后 release；
                # 是否已有效果由 release/超时扫描统一裁决终结或补偿。
                conn.execute(
                    "update instances set state=?, updated_at=? where instance_id=?",
                    (InstanceState.CANCELLING.value, iso(at), instance_id),
                )
                return
            conn.execute(
                "update execution_leases set status='revoked', released_at=?"
                " where instance_id=? and status='active'",
                (iso(at), instance_id),
            )
            if effects == 0:
                self._cancel_without_effects(conn, instance_id, at, reason)
            else:
                self._begin_compensation(conn, instance_id, at, reason)

    def revoke_pending_for_authorization(
        self, tenant_id: str, subject_id: str | None, now: datetime, reason: str
    ) -> int:
        """显式按授权收缩/风险隔离扫描受影响实例，返回处理数量。

        无效果实例直接取消；有部分效果的进入补偿。通常授权换版或租户状态
        变更已在同事务自动调用本扫描，此方法供强制隔离等运营动作使用。
        """

        at = _utc(now)
        with self.store.transaction() as conn:
            return self._contraction_scan(conn, at, tenant_id, subject_id, reason=reason)

    def _contraction_scan(
        self,
        conn,
        at: datetime,
        tenant_id: str,
        subject_id: str | None,
        reason: str,
    ) -> int:
        """事务内：找出租户（可限定主体）所有未终结实例并按是否已生效分流。

        判定依据是“当前授权是否仍覆盖该实例任务所需能力”，因此移除与任务
        无关的能力不会误伤实例；命中的实例立即作废活跃租约，无效果直接
        取消，有部分效果进入补偿链（已在补偿链中的实例不重复处理）。
        """

        open_states = (
            InstanceState.QUEUED.value,
            InstanceState.LEASED.value,
            InstanceState.RUNNING.value,
            InstanceState.CANCELLING.value,
            InstanceState.COMPENSATING.value,
        )
        rows = conn.execute(
            "select instance_id from instances where tenant_id=? and state in ("
            + ",".join("?" for _ in open_states)
            + ")",
            (tenant_id, *open_states),
        ).fetchall()
        affected = 0
        for row in rows:
            instance_id = row["instance_id"]
            inst = conn.execute(
                "select * from instances where instance_id=?", (instance_id,)
            ).fetchone()
            spec = self._load_spec(conn, inst["job_id"], inst["definition_version"])
            if subject_id is not None and spec.subject_id != subject_id:
                continue
            # 已在补偿链中的实例不受新收缩影响（补偿必须走完）。
            if inst["state"] == InstanceState.COMPENSATING.value:
                continue
            auth = self._latest_auth(conn, tenant_id, spec.subject_id)
            tenant_ok = self._tenant_status(conn, tenant_id) == "active"
            covered = auth is not None and auth.active and auth.covers_all(spec.required_capabilities)
            job_ok = self._latest_enabled(conn, spec.job_id)
            if tenant_ok and covered and job_ok and reason != "manual_quarantine":
                continue
            affected += 1
            conn.execute(
                "update execution_leases set status='revoked', released_at=?"
                " where instance_id=? and status='active'",
                (iso(at), instance_id),
            )
            conn.execute(
                "update capability_tickets set status='revoked', revoke_reason=?"
                " where instance_id=? and status='valid'",
                (reason, instance_id),
            )
            self.store.append_event(
                conn,
                "ticket.revoked",
                instance_id,
                at,
                "scheduler",
                instance_id=instance_id,
                detail={"reason": reason},
            )
            effects = conn.execute(
                "select count(*) as c from effect_receipts where instance_id=?", (instance_id,)
            ).fetchone()["c"]
            if effects == 0:
                self._cancel_without_effects(conn, instance_id, at, reason)
            else:
                self._begin_compensation(conn, instance_id, at, reason)
        return affected

    def _recheck(self, conn, spec: JobSpec, inst, at: datetime) -> str | None:
        """领取时复核；返回 None 通过，否则返回收缩原因。

        复核票据状态、有效期、租户状态，并对照“当前”授权版本重新裁决：
        当前授权仍覆盖任务所需能力时，把票据透明重绑到新版本；
        授权缺失/停用/收缩时才拒绝（扩张与等价修订不影响排队实例）。
        """

        ticket = conn.execute(
            "select * from capability_tickets where instance_id=? order by issued_at desc limit 1",
            (inst["instance_id"],),
        ).fetchone()
        if ticket is None or ticket["status"] != "valid":
            return "ticket_not_valid"
        tenant = self._tenant_status(conn, spec.tenant_id)
        if tenant != "active":
            return f"tenant_{tenant}"
        if not self._latest_enabled(conn, spec.job_id):
            return "job_disabled"
        auth = self._latest_auth(conn, spec.tenant_id, spec.subject_id)
        if auth is None or not auth.active:
            return "authorization_missing_or_inactive"
        if not auth.covers_all(spec.required_capabilities):
            return "authorization_shrunk"
        if auth.version != ticket["auth_version"] or parse_iso(ticket["expires_at"]) - at < TICKET_REISSUE_BEFORE_EXPIRY:
            # 授权已换版（仍覆盖）或票据临近到期：重绑当前版本并续短寿。
            self._reissue_ticket(conn, spec, inst["instance_id"], ticket, auth, at)
        return None

    def _reissue_ticket(self, conn, spec, instance_id, old_ticket, auth: Authorization, at) -> None:
        ticket_id = "tkt-" + uuid.uuid4().hex[:16]
        expires = at + TICKET_TTL
        conn.execute(
            "update capability_tickets set status='expired' where ticket_id=?",
            (old_ticket["ticket_id"],),
        )
        conn.execute(
            "insert into capability_tickets(ticket_id, instance_id, auth_version,"
            " auth_fingerprint, scope_json, issued_at, expires_at, status)"
            " values(?, ?, ?, ?, ?, ?, ?, 'valid')",
            (
                ticket_id,
                instance_id,
                auth.version,
                auth.fingerprint,
                self._scope_json(auth.capabilities),
                iso(at),
                iso(expires),
            ),
        )
        self.store.append_event(
            conn,
            "ticket.issued",
            instance_id,
            at,
            "scheduler",
            instance_id=instance_id,
            detail={
                "ticket_id": ticket_id,
                "reissued_for": old_ticket["ticket_id"],
                "auth_version": auth.version,
                "auth_fingerprint": auth.fingerprint,
                "expires_at": iso(expires),
            },
        )

    def _handle_contraction(self, conn, instance_id: str, at: datetime, reason: str) -> None:
        effects = conn.execute(
            "select count(*) as c from effect_receipts where instance_id=?", (instance_id,)
        ).fetchone()["c"]
        conn.execute(
            "update capability_tickets set status='revoked', revoke_reason=?"
            " where instance_id=? and status='valid'",
            (reason, instance_id),
        )
        if effects == 0:
            self._cancel_without_effects(conn, instance_id, at, reason)
        else:
            self._begin_compensation(conn, instance_id, at, reason)

    def _cancel_without_effects(self, conn, instance_id: str, at: datetime, reason: str) -> None:
        conn.execute(
            "update instances set state=?, updated_at=? where instance_id=?",
            (InstanceState.CANCELLED.value, iso(at), instance_id),
        )
        conn.execute(
            "update execution_leases set status='revoked', released_at=?"
            " where instance_id=? and status='active'",
            (iso(at), instance_id),
        )
        self.store.append_event(
            conn,
            "instance.cancelled",
            instance_id,
            at,
            "scheduler",
            instance_id=instance_id,
            detail={"reason": reason, "compensated_effects": 0},
        )

    def _begin_compensation(self, conn, instance_id: str, at: datetime, reason: str) -> None:
        conn.execute(
            "update instances set state=?, updated_at=? where instance_id=?",
            (InstanceState.COMPENSATING.value, iso(at), instance_id),
        )
        # saga 语义：按效果产生的逆序补偿（后产生的先撤销）。
        effects = conn.execute(
            "select effect_key, action from effect_receipts where instance_id=?"
            " order by recorded_at desc, rowid desc",
            (instance_id,),
        ).fetchall()
        for effect in effects:
            existing = conn.execute(
                "select 1 from compensations where instance_id=? and effect_key=?",
                (instance_id, effect["effect_key"]),
            ).fetchone()
            if existing:
                continue
            conn.execute(
                "insert into compensations(instance_id, effect_key, action, state, attempts,"
                " max_attempts, last_error, updated_at) values(?, ?, ?, 'pending', 0, 3, NULL, ?)",
                (instance_id, effect["effect_key"], effect["action"], iso(at)),
            )
        self.store.append_event(
            conn,
            "compensation.scheduled",
            instance_id,
            at,
            "scheduler",
            instance_id=instance_id,
            detail={"reason": reason, "effects": len(effects)},
        )

    def run_compensation(
        self,
        instance_id: str,
        compensator,
        now: datetime,
    ) -> InstanceState:
        """驱动补偿链。

        ``compensator(action, effect_key, resource) -> None`` 在无事务的外部世界
        执行实际撤销动作；抛异常表示本次失败并计入重试。全部成功后实例终结为
        cancelled；重试耗尽则进入死信。
        """

        at = _utc(now)
        while True:
            with self.store.transaction() as conn:
                rows = conn.execute(
                    "select c.* from compensations c join effect_receipts e"
                    " on c.instance_id=e.instance_id and c.effect_key=e.effect_key"
                    " where c.instance_id=? and c.state!='done'"
                    " order by e.recorded_at desc, e.rowid desc",
                    (instance_id,),
                ).fetchall()
                if not rows:
                    pending_state = conn.execute(
                        "select state from instances where instance_id=?", (instance_id,)
                    ).fetchone()
                    if pending_state is None:
                        raise KeyError(instance_id)
                    if pending_state["state"] == InstanceState.COMPENSATING.value:
                        done_count = conn.execute(
                            "select count(*) as c from compensations"
                            " where instance_id=? and state='done'",
                            (instance_id,),
                        ).fetchone()["c"]
                        conn.execute(
                            "update instances set state=?, updated_at=? where instance_id=?",
                            (InstanceState.CANCELLED.value, iso(at), instance_id),
                        )
                        self.store.append_event(
                            conn,
                            "compensation.completed",
                            instance_id,
                            at,
                            "compensator",
                            instance_id=instance_id,
                            detail={"compensated_effects": done_count},
                        )
                        self.store.append_event(
                            conn,
                            "instance.cancelled",
                            instance_id,
                            at,
                            "compensator",
                            instance_id=instance_id,
                            detail={
                                "reason": "compensation_chain_complete",
                                "compensated_effects": done_count,
                            },
                        )
                    return InstanceState.CANCELLED
                row = rows[0]
                if row["attempts"] >= row["max_attempts"]:
                    conn.execute(
                        "update instances set state=?, updated_at=? where instance_id=?",
                        (InstanceState.DEAD_LETTERED.value, iso(at), instance_id),
                    )
                    conn.execute(
                        "update compensations set state='failed', updated_at=?"
                        " where instance_id=? and effect_key=?",
                        (iso(at), instance_id, row["effect_key"]),
                    )
                    self.store.append_event(
                        conn,
                        "compensation.failed",
                        instance_id,
                        at,
                        "compensator",
                        instance_id=instance_id,
                        detail={"effect_key": row["effect_key"], "attempts": row["attempts"], "last_error": row["last_error"]},
                    )
                    self.store.append_event(
                        conn,
                        "instance.dead_lettered",
                        instance_id,
                        at,
                        "compensator",
                        instance_id=instance_id,
                        detail={"blocking_effect": row["effect_key"]},
                    )
                    return InstanceState.DEAD_LETTERED
                effect = conn.execute(
                    "select * from effect_receipts where instance_id=? and effect_key=?",
                    (instance_id, row["effect_key"]),
                ).fetchone()
                conn.execute(
                    "update compensations set attempts=attempts+1, updated_at=?"
                    " where instance_id=? and effect_key=?",
                    (iso(at), instance_id, row["effect_key"]),
                )
                action, key, resource, attempts = effect["action"], effect["effect_key"], effect["resource"], row["attempts"] + 1
            # 外部撤销动作在事务外执行。
            try:
                compensator(action, key, resource)
                failure: Exception | None = None
            except Exception as exc:  # noqa: BLE001 - 补偿器异常是业务输入
                failure = exc
            with self.store.transaction() as conn:
                if failure is None:
                    conn.execute(
                        "update compensations set state='done', last_error=NULL, updated_at=?"
                        " where instance_id=? and effect_key=?",
                        (iso(at), instance_id, key),
                    )
                else:
                    conn.execute(
                        "update compensations set last_error=?, updated_at=?"
                        " where instance_id=? and effect_key=?",
                        (repr(failure), iso(at), instance_id, key),
                    )
                    self.store.append_event(
                        conn,
                        "compensation.retry",
                        instance_id,
                        at,
                        "compensator",
                        instance_id=instance_id,
                        detail={"effect_key": key, "attempt": attempts, "error": repr(failure)},
                    )

    def reopen_dead_letter(self, instance_id: str, now: datetime, actor_id: str = "operator") -> None:
        """死信经审计后重新发起补偿（重置该失败效果的重试预算）。"""

        at = _utc(now)
        with self.store.transaction() as conn:
            inst = conn.execute("select state from instances where instance_id=?", (instance_id,)).fetchone()
            if inst is None or inst["state"] != InstanceState.DEAD_LETTERED.value:
                raise ValueError("只有死信实例可以重新发起补偿")
            conn.execute(
                "update instances set state=?, updated_at=? where instance_id=?",
                (InstanceState.COMPENSATING.value, iso(at), instance_id),
            )
            conn.execute(
                "update compensations set state='pending', attempts=0, last_error=NULL, updated_at=?"
                " where instance_id=? and state='failed'",
                (iso(at), instance_id),
            )
            self.store.append_event(
                conn,
                "instance.reopened",
                instance_id,
                at,
                actor_id,
                instance_id=instance_id,
                detail={"from_state": InstanceState.DEAD_LETTERED.value},
            )

    # ------------------------------------------------------------- 查询/恢复

    def audit(self, instance_id: str) -> InstanceAudit:
        conn = self.store.connection
        inst = conn.execute("select * from instances where instance_id=?", (instance_id,)).fetchone()
        if inst is None:
            raise KeyError(f"实例不存在：{instance_id}")
        spec = self._load_spec(conn, inst["job_id"], inst["definition_version"])
        ticket = conn.execute(
            "select * from capability_tickets where instance_id=? order by issued_at desc limit 1",
            (instance_id,),
        ).fetchone()
        lease = conn.execute(
            "select generation from execution_leases where instance_id=?"
            " order by generation desc limit 1",
            (instance_id,),
        ).fetchone()
        effects = tuple(
            EffectView(
                effect_key=r["effect_key"],
                action=r["action"],
                resource=r["resource"],
                recorded_at=parse_iso(r["recorded_at"]),
                generation=r["generation"],
            )
            for r in conn.execute(
                "select * from effect_receipts where instance_id=? order by recorded_at",
                (instance_id,),
            )
        )
        compensations = tuple(
            CompensationView(
                effect_key=r["effect_key"],
                action=r["action"],
                state=CompensationState(r["state"]),
                attempts=r["attempts"],
                last_error=r["last_error"],
                updated_at=parse_iso(r["updated_at"]),
            )
            for r in conn.execute(
                "select * from compensations where instance_id=? order by updated_at",
                (instance_id,),
            )
        )
        timeline = tuple(
            TimelineEvent(
                event_id=r["event_id"],
                event_type=r["event_type"],
                occurred_at=parse_iso(r["occurred_at"]),
                actor_id=r["actor_id"],
                detail=json.loads(r["detail_json"]),
            )
            for r in conn.execute(
                "select * from event_log where instance_id=? or aggregate_id=?"
                " order by occurred_at, rowid",
                (instance_id, inst["window_key"]),
            )
        )
        scope = {Capability(**item) for item in json.loads(ticket["scope_json"])} if ticket else set()
        return InstanceAudit(
            instance_id=instance_id,
            job_id=inst["job_id"],
            tenant_id=inst["tenant_id"],
            purpose=inst["purpose"],
            state=InstanceState(inst["state"]),
            scheduled_for_utc=parse_iso(inst["scheduled_for_utc"]),
            window_key=inst["window_key"],
            definition_version=inst["definition_version"],
            ticket_capabilities=frozenset(scope),
            auth_version_at_issue=ticket["auth_version"] if ticket else 0,
            auth_fingerprint=ticket["auth_fingerprint"] if ticket else "none",
            lease_generation=lease["generation"] if lease else None,
            effects=effects,
            compensations=compensations,
            timeline=timeline,
        )

    def recover(self, now: datetime) -> dict[str, int]:
        """调度器（重新）启动时调用：恢复未决租约、卡死实例与下一触发点。

        - 上次未正常释放的 active 租约一律按过期处理，实例回到 queued 等待重领；
        - 留在 leased/running 但无活跃租约的实例回到 queued；
        - 重算每个启用任务的下一触发点（不回补超过游标语义的窗口，补跑由 tick 统一处理）。
        """

        at = _utc(now)
        with self.store.transaction() as conn:
            lease_count = 0
            for lease in conn.execute(
                "select lease_id, instance_id, generation, hard_deadline from execution_leases"
                " where status='active'"
            ).fetchall():
                hard_expired = parse_iso(lease["hard_deadline"]) <= at
                conn.execute(
                    "update execution_leases set status='expired', released_at=? where lease_id=?",
                    (iso(at), lease["lease_id"]),
                )
                self.store.append_event(
                    conn,
                    "lease.expired",
                    lease["instance_id"],
                    at,
                    "recovery",
                    instance_id=lease["instance_id"],
                    detail={
                        "lease_id": lease["lease_id"],
                        "generation": lease["generation"],
                        "recovery": True,
                        "reason": "hard_deadline" if hard_expired else "restart",
                    },
                )
                if hard_expired:
                    # 超过最长运行时间的实例不得重新入队刷新预算：按效果分流。
                    self._handle_contraction(
                        conn, lease["instance_id"], at, reason="max_runtime_exceeded"
                    )
                lease_count += 1
            inst_count = 0
            for inst in conn.execute(
                "select instance_id from instances where state in (?, ?)",
                (InstanceState.LEASED.value, InstanceState.RUNNING.value),
            ).fetchall():
                active = conn.execute(
                    "select 1 from execution_leases where instance_id=? and status='active'",
                    (inst["instance_id"],),
                ).fetchone()
                if not active:
                    conn.execute(
                        "update instances set state=?, updated_at=? where instance_id=?",
                        (InstanceState.QUEUED.value, iso(at), inst["instance_id"]),
                    )
                    inst_count += 1
            job_count = 0
            for row in conn.execute(
                "select job_id, version, payload from job_definitions j"
                " where enabled=1 and version=("
                " select max(version) from job_definitions where job_id=j.job_id)"
            ).fetchall():
                spec = self._spec_from_payload(row["job_id"], row["payload"], row["version"])
                state = conn.execute(
                    "select next_window_utc from schedule_state where job_id=?", (spec.job_id,)
                ).fetchone()
                if state is None:
                    trigger = self._compute_next(spec, at)
                    conn.execute(
                        "insert into schedule_state(job_id, last_window_utc, next_window_utc, updated_at)"
                        " values(?, NULL, ?, ?)",
                        (spec.job_id, iso(trigger.scheduled_utc), iso(at)),
                    )
                else:
                    # 下一触发点已过期的不重算：tick 会按补跑策略处理错过窗口。
                    if parse_iso(state["next_window_utc"]) <= at:
                        continue
                    trigger = self._compute_next(spec, at)
                    if trigger.scheduled_utc < parse_iso(state["next_window_utc"]):
                        conn.execute(
                            "update schedule_state set next_window_utc=?, updated_at=? where job_id=?",
                            (iso(trigger.scheduled_utc), iso(at), spec.job_id),
                        )
                job_count += 1
        return {"leases_expired": lease_count, "instances_requed": inst_count, "jobs_recalibrated": job_count}

    def list_dead_letters(self) -> list[str]:
        return [
            r["instance_id"]
            for r in self.store.connection.execute(
                "select instance_id from instances where state=? order by updated_at",
                (InstanceState.DEAD_LETTERED.value,),
            )
        ]

    def next_trigger(self, job_id: str) -> datetime | None:
        row = self.store.connection.execute(
            "select next_window_utc from schedule_state where job_id=?", (job_id,)
        ).fetchone()
        return parse_iso(row["next_window_utc"]) if row else None

    def cancel_requested(self, token: str) -> bool:
        """执行器轮询：当前租约对应的实例是否被要求优雅停止。"""

        parsed = LeaseToken.parse(token)
        with self.store.transaction() as conn:
            lease = conn.execute(
                "select * from execution_leases where lease_id=?", (parsed.lease_id,)
            ).fetchone()
            if lease is None or lease["generation"] != parsed.generation:
                raise LeaseLost("租约已失效")
            state = conn.execute(
                "select state from instances where instance_id=?", (lease["instance_id"],)
            ).fetchone()["state"]
            return state == InstanceState.CANCELLING.value

    def release(self, token: str, now: datetime) -> None:
        """执行器在优雅取消中自行停止后释放租约；效果有无决定终结或补偿。"""

        at = _utc(now)
        with self.store.transaction() as conn:
            lease = self._authenticate(conn, token, at, allow_cancelling=True)
            instance_id = lease["instance_id"]
            conn.execute(
                "update execution_leases set status='released', released_at=? where lease_id=?",
                (iso(at), lease["lease_id"]),
            )
            effects = conn.execute(
                "select count(*) as c from effect_receipts where instance_id=?", (instance_id,)
            ).fetchone()["c"]
            if effects == 0:
                self._cancel_without_effects(conn, instance_id, at, "graceful_release")
            else:
                self._begin_compensation(conn, instance_id, at, "graceful_release_with_effects")

    # -------------------------------------------------------------- 内部工具

    def _authenticate(self, conn, token: str, at: datetime, allow_cancelling: bool = False):
        try:
            parsed = LeaseToken.parse(token)
        except (ValueError, AttributeError):
            raise LeaseLost("非法租约令牌")
        lease = conn.execute(
            "select * from execution_leases where lease_id=?", (parsed.lease_id,)
        ).fetchone()
        if lease is None:
            raise LeaseLost("租约不存在")
        if lease["generation"] != parsed.generation:
            raise LeaseLost("租约世代号已失效（发生过重新领取）")
        if lease["status"] != "active":
            raise LeaseLost(f"租约状态为 {lease['status']}")
        if lease["secret_hash"] != _secret_hash(parsed.secret):
            raise LeaseLost("租约令牌不匹配")
        if parse_iso(lease["deadline"]) <= at:
            raise LeaseLost("租约已超时，请放弃该实例并等待重新领取")
        allowed = {InstanceState.LEASED.value, InstanceState.RUNNING.value}
        if allow_cancelling:
            allowed.add(InstanceState.CANCELLING.value)
        inst = conn.execute("select state from instances where instance_id=?", (lease["instance_id"],)).fetchone()
        if inst is None or inst["state"] not in allowed:
            raise LeaseLost(f"实例状态为 {inst['state'] if inst else 'missing'}，执行器必须停止")
        return lease

    def _compute_next(self, spec: JobSpec, after_utc: datetime) -> ResolvedTrigger:
        return next_window(
            CronSpec.parse(spec.cron),
            ZoneInfo(spec.timezone),
            after_utc.astimezone(UTC),
            spec.gap_policy.value,
            spec.overlap_policy.value,
        )

    def _latest_auth(self, conn, tenant_id: str, subject_id: str) -> Authorization | None:
        row = conn.execute(
            "select payload from authorizations where tenant_id=? and subject_id=?"
            " order by version desc limit 1",
            (tenant_id, subject_id),
        ).fetchone()
        if row is None:
            return None
        data = json.loads(row["payload"])
        return Authorization(
            tenant_id=data["tenant_id"],
            subject_id=data["subject_id"],
            version=data["version"],
            capabilities=frozenset(Capability(**c) for c in data["capabilities"]),
            active=data["active"],
        )

    def _tenant_status(self, conn, tenant_id: str) -> str:
        row = conn.execute("select status from tenants where tenant_id=?", (tenant_id,)).fetchone()
        return row["status"] if row else "active"

    def _latest_enabled(self, conn, job_id: str) -> bool:
        row = conn.execute(
            "select enabled from job_definitions where job_id=? order by version desc limit 1",
            (job_id,),
        ).fetchone()
        return bool(row["enabled"]) if row else False

    def _load_spec(self, conn, job_id: str, version: int) -> JobSpec:
        row = conn.execute(
            "select payload from job_definitions where job_id=? and version=?", (job_id, version)
        ).fetchone()
        if row is None:
            raise KeyError(f"任务定义 {job_id} 版本 {version} 不存在（历史版本不覆盖）")
        return self._spec_from_payload(job_id, row["payload"], version)

    def _job_payload(self, spec: JobSpec) -> dict:
        return {
            "job_id": spec.job_id,
            "tenant_id": spec.tenant_id,
            "subject_id": spec.subject_id,
            "version": spec.version,
            "purpose": spec.purpose,
            "cron": spec.cron,
            "timezone": spec.timezone,
            "required_capabilities": [
                c.__dict__
                for c in sorted(spec.required_capabilities, key=lambda c: c.fingerprint_parts())
            ],
            "max_runtime_seconds": spec.max_runtime_seconds,
            "misfire_policy": spec.misfire_policy.value,
            "max_backfill": spec.max_backfill,
            "gap_policy": spec.gap_policy.value,
            "overlap_policy": spec.overlap_policy.value,
        }

    def _spec_from_payload(self, job_id: str, payload_raw: str, version: int) -> JobSpec:
        p = json.loads(payload_raw)
        return JobSpec(
            job_id=job_id,
            tenant_id=p["tenant_id"],
            subject_id=p["subject_id"],
            version=version,
            purpose=p["purpose"],
            cron=p["cron"],
            timezone=p["timezone"],
            required_capabilities=frozenset(Capability(**c) for c in p["required_capabilities"]),
            max_runtime_seconds=p["max_runtime_seconds"],
            misfire_policy=MisfirePolicy(p.get("misfire_policy", "scheduled")),
            max_backfill=p.get("max_backfill", 3),
            gap_policy=GapPolicy(p.get("gap_policy", "forward")),
            overlap_policy=OverlapPolicy(p.get("overlap_policy", "early")),
        )

    @staticmethod
    def _scope_json(capabilities) -> str:
        return json.dumps(
            [c.__dict__ for c in sorted(capabilities, key=lambda c: c.fingerprint_parts())],
            ensure_ascii=False,
            sort_keys=True,
        )
