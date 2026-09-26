"""能力票据调度服务的端到端测试。"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile

from scheduler.authz import (
    AuthorizationService,
    AuthorizationShrunk,
    CapabilityDenied,
    CapabilityScope,
    JobDefinition,
    TenantStatus,
    TicketError,
    TicketExpired,
    TicketInstanceMismatch,
)
from scheduler.calendar import MissPolicy, Schedule
from scheduler.clock import MockClock
from scheduler.eventlog import EventStore
from scheduler.service import (
    InstanceStatus,
    InvalidState,
    LeaseLost,
    LeaseUnavailable,
    SchedulerService,
)

UTC = timezone.utc


def dt(text: str) -> datetime:
    return datetime.fromisoformat(text)


def make_job(
    clock: MockClock,
    *,
    job_id: str = "job-daily",
    cron: str = "0 * * * *",
    tzname: str = "UTC",
    miss_policy: MissPolicy = MissPolicy.CATCH_UP_LATEST,
    max_run_seconds: int = 3600,
    tenant_id: str = "tenant-a",
    owner: str = "user-1",
) -> JobDefinition:
    return JobDefinition(
        job_id=job_id,
        tenant_id=tenant_id,
        owner_subject_id=owner,
        purpose="每小时同步连接器清单到工作区",
        required=(
            CapabilityScope("connector:read", frozenset({"connector:w1/*"})),
            CapabilityScope("workspace:file:write", frozenset({"workspace:w1/reports/*"})),
        ),
        schedule=Schedule(cron=cron, timezone=tzname, miss_policy=miss_policy),
        max_run_seconds=max_run_seconds,
        created_at=clock.now(),
    )


def make_service(
    clock: MockClock,
    *,
    grants: dict | None = None,
    lease_timeout: timedelta = timedelta(minutes=5),
    store: EventStore | None = None,
) -> SchedulerService:
    authz = AuthorizationService(clock)
    authz.register_tenant("tenant-a")
    authz.register_tenant("tenant-b")
    authz.update_authorization(
        "user-1",
        "tenant-a",
        grants
        or {
            "connector:read": ["connector:w1/*"],
            "workspace:file:write": ["workspace:w1/reports/*"],
        },
    )
    return SchedulerService(clock, authz, store=store, lease_timeout=lease_timeout)


class CalendarTest(unittest.TestCase):
    def test_basic_cron_and_window_key(self) -> None:
        sched = Schedule("30 9 * * *", "Asia/Shanghai")
        nxt = sched.next_after(dt("2026-09-25T00:00:00Z"))
        self.assertEqual(nxt, dt("2026-09-25T01:30:00Z"))  # 北京 09:30 = UTC 01:30
        self.assertEqual(sched.window_key(nxt), "2026-09-25T01:30:00Z")

    def test_spring_forward_gap_after_and_before(self) -> None:
        # 纽约 2026-03-08 02:00 时钟跳到 03:00，02:30 名义时间不存在。
        after = Schedule("30 2 8 3 *", "America/New_York", gap_skip="after")
        before = Schedule("30 2 8 3 *", "America/New_York", gap_skip="before")
        start = dt("2026-03-08T00:00:00Z")
        self.assertEqual(after.next_after(start), dt("2026-03-08T07:30:00Z"))  # 03:30 EDT
        self.assertEqual(before.next_after(start), dt("2026-03-08T06:30:00Z"))  # 01:30 EST

    def test_fall_back_ambiguous_takes_first_occurrence(self) -> None:
        # 纽约 2026-11-01 02:00 EDT 回退到 01:00 EST；01:30 出现两次，固定取第一次，
        # 区间枚举不会为第二次出现另开窗口。
        sched = Schedule("30 1 * * *", "America/New_York")
        windows = sched.missed_windows(dt("2026-11-01T05:00:00Z"), dt("2026-11-01T07:00:00Z"))
        self.assertEqual([w[1] for w in windows], ["2026-11-01T05:30:00Z"])  # 第一次 01:30 EDT
        self.assertEqual(
            sched.next_after(dt("2026-11-01T05:30:00Z")),
            dt("2026-11-02T06:30:00Z"),  # 次日已切回 EST（-5）
        )

    def test_miss_policies(self) -> None:
        sched = Schedule("0 * * * *", "UTC", MissPolicy.CATCH_UP)
        start, now = dt("2026-09-25T00:00:00Z"), dt("2026-09-25T03:00:00Z")
        windows = sched.missed_windows(start, now)
        self.assertEqual([w[1] for w in windows], [
            "2026-09-25T01:00:00Z", "2026-09-25T02:00:00Z", "2026-09-25T03:00:00Z",
        ])
        skip = Schedule("0 * * * *", "UTC", MissPolicy.SKIP)
        self.assertEqual(skip.missed_windows(start, now), [])
        latest = Schedule("0 * * * *", "UTC", MissPolicy.CATCH_UP_LATEST)
        self.assertEqual(
            [w[1] for w in latest.missed_windows(start, now)],
            ["2026-09-25T03:00:00Z"],
        )


class SchedulingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = MockClock(dt("2026-09-25T00:00:00Z"))
        self.svc = make_service(self.clock)
        self.svc.register_job(make_job(self.clock))

    def test_window_opens_once_even_with_repeated_ticks(self) -> None:
        self.svc.register_job(
            make_job(self.clock, job_id="job-catchup", miss_policy=MissPolicy.CATCH_UP)
        )
        self.clock.set(dt("2026-09-25T03:00:00Z"))
        first = self.svc.tick()
        second = self.svc.tick()
        catchup = [i for i in first if i.startswith("inst-job-catchup")]
        self.assertEqual(len(catchup), 3)  # 01:00、02:00、03:00 三个窗口全补
        self.assertEqual(second, [])
        windows = {i.window_key for i in self.svc.list_instances("job-catchup")}
        self.assertEqual(len(windows), 3)  # 同一窗口不重复生成实例

    def test_miss_policy_skip_schedules_nothing_after_downtime(self) -> None:
        self.svc.register_job(
            make_job(self.clock, job_id="job-skip", miss_policy=MissPolicy.SKIP)
        )
        self.clock.set(dt("2026-09-25T03:00:00Z"))
        self.svc.tick()
        self.assertEqual(self.svc.list_instances("job-skip"), [])

    def test_enqueue_issues_version_pinned_nontransferable_ticket(self) -> None:
        self.clock.set(dt("2026-09-25T01:00:00Z"))
        (instance_id,) = self.svc.tick()
        inst = self.svc.get_instance(instance_id)
        self.assertEqual(inst.status, InstanceStatus.QUEUED)
        self.assertEqual(inst.authz_version_at_enqueue, 1)
        # 票据出示给其他实例必须被拒绝（不可转让）。
        with self.assertRaises(TicketInstanceMismatch):
            self.svc._authz.verify(inst.ticket_token, instance_id="inst-other")

    def test_enqueue_denied_when_authorization_missing(self) -> None:
        authz = AuthorizationService(self.clock)
        authz.register_tenant("tenant-b")
        authz.update_authorization("user-2", "tenant-b", {"connector:read": ["connector:w1/*"]})
        svc = SchedulerService(self.clock, authz)
        svc.register_job(make_job(self.clock, job_id="job-b", tenant_id="tenant-b", owner="user-2"))
        self.clock.set(dt("2026-09-25T01:00:00Z"))
        (instance_id,) = svc.tick()
        self.assertEqual(svc.get_instance(instance_id).status, InstanceStatus.CANCELLED)
        self.assertEqual(svc.get_instance(instance_id).cancel_reason, "capability_denied")

    def test_ticket_expiry_and_tamper(self) -> None:
        self.clock.set(dt("2026-09-25T01:00:00Z"))
        (instance_id,) = self.svc.tick()
        token = self.svc.get_instance(instance_id).ticket_token
        self.clock.advance(minutes=11)
        with self.assertRaises(TicketExpired):
            self.svc._authz.verify(token, instance_id=instance_id)
        payload, sig = token.split(".", 1)
        tampered = payload + "." + ("0" * len(sig))
        with self.assertRaises(TicketError):
            self.svc._authz.verify(tampered, instance_id=instance_id)


class ClaimAndAuthorizationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = MockClock(dt("2026-09-25T00:00:00Z"))
        self.svc = make_service(self.clock)
        self.svc.register_job(make_job(self.clock))
        self.clock.set(dt("2026-09-25T01:00:00Z"))
        (self.instance_id,) = self.svc.tick()

    def test_claim_success_and_lease_conflict(self) -> None:
        lease = self.svc.claim(self.instance_id, "exec-A")
        self.assertEqual(lease.executor_id, "exec-A")
        with self.assertRaises(LeaseUnavailable):
            self.svc.claim(self.instance_id, "exec-B")
        # 同一执行器重复领取是幂等的。
        self.assertEqual(self.svc.claim(self.instance_id, "exec-A").lease_id, lease.lease_id)

    def test_authorization_expansion_keeps_queued_instance_runnable(self) -> None:
        self.svc._authz.update_authorization(
            "user-1", "tenant-a",
            {
                "connector:read": ["connector:w1/*"],
                "workspace:file:write": ["workspace:w1/reports/*"],
                "workspace:file:read": ["workspace:w1/*"],  # 扩张
            },
        )
        lease = self.svc.claim(self.instance_id, "exec-A")
        self.assertTrue(lease.lease_id.endswith("-1"))

    def test_authorization_shrink_before_claim_cancels_without_effects(self) -> None:
        self.svc._authz.update_authorization(
            "user-1", "tenant-a",
            {"connector:read": ["connector:w9/*"]},  # 收缩，丢掉 w1
        )
        with self.assertRaises(AuthorizationShrunk):
            self.svc.claim(self.instance_id, "exec-A")
        inst = self.svc.get_instance(self.instance_id)
        self.assertEqual(inst.status, InstanceStatus.CANCELLED)
        self.assertEqual(inst.cancel_reason, "authz_shrunk")

    def test_tenant_disabled_and_quarantined_cancel_queued_instance(self) -> None:
        for status in (TenantStatus.DISABLED, TenantStatus.QUARANTINED):
            clock = MockClock(dt("2026-09-25T00:00:00Z"))
            svc = make_service(clock)
            svc.register_job(make_job(clock, job_id=f"job-{status.value}"))
            clock.set(dt("2026-09-25T01:00:00Z"))
            (instance_id,) = svc.tick()
            svc._authz.set_tenant_status("tenant-a", status)
            with self.assertRaises(TicketError):
                svc.claim(instance_id, "exec-A")
            self.assertEqual(svc.get_instance(instance_id).status, InstanceStatus.CANCELLED)

    def test_tick_proactively_cancels_shrunk_queued_instance(self) -> None:
        self.svc._authz.update_authorization(
            "user-1", "tenant-a", {"connector:read": ["connector:w9/*"]}
        )
        self.svc.tick()
        self.assertEqual(
            self.svc.get_instance(self.instance_id).status, InstanceStatus.CANCELLED
        )

    def test_tick_revokes_leased_instance_immediately_on_quarantine(self) -> None:
        # 已领取、无外部效果：隔离后的下一次 tick 立即取消，不必等心跳或租约超时。
        self.svc.claim(self.instance_id, "exec-A")
        self.svc._authz.set_tenant_status("tenant-a", TenantStatus.QUARANTINED)
        self.svc.tick()
        inst = self.svc.get_instance(self.instance_id)
        self.assertEqual(inst.status, InstanceStatus.CANCELLED)
        self.assertTrue(inst.lease.revoked)
        self.assertEqual(inst.cancel_reason, "tenant_quarantined")


class LeaseAndEffectTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = MockClock(dt("2026-09-25T00:00:00Z"))
        self.svc = make_service(self.clock)
        self.svc.register_job(make_job(self.clock, max_run_seconds=3600))
        self.clock.set(dt("2026-09-25T01:00:00Z"))
        (self.instance_id,) = self.svc.tick()
        self.lease = self.svc.claim(self.instance_id, "exec-A")

    def test_effect_respects_ticket_scope(self) -> None:
        with self.assertRaises(InvalidState):
            self.svc.record_effect(
                self.instance_id, self.lease.lease_id, "k1",
                "workspace:w9/secret", "越权写入",
            )

    def test_lease_timeout_reclaim_and_idempotent_effects(self) -> None:
        self.svc.record_effect(
            self.instance_id, self.lease.lease_id, "key-1",
            "workspace:w1/reports/a.md", "第一次写入",
        )
        # 超过租约超时：旧执行器的心跳与提交都必须被拒绝。
        self.clock.advance(minutes=6)
        with self.assertRaises(LeaseLost):
            self.svc.heartbeat(self.instance_id, self.lease.lease_id)
        with self.assertRaises(LeaseLost):
            self.svc.record_effect(
                self.instance_id, self.lease.lease_id, "key-2",
                "workspace:w1/reports/b.md", "旧执行器迟到的写入",
            )
        # 新执行器重新领取（attempt=2），复用幂等键不会重复副作用。
        new_lease = self.svc.claim(self.instance_id, "exec-B")
        self.assertEqual(new_lease.attempt, 2)
        same = self.svc.record_effect(
            self.instance_id, new_lease.lease_id, "key-1",
            "workspace:w1/reports/a.md", "重试同一写入",
        )
        again = self.svc.record_effect(
            self.instance_id, new_lease.lease_id, "key-1",
            "workspace:w1/reports/a.md", "再试一次",
        )
        self.svc.record_effect(
            self.instance_id, new_lease.lease_id, "key-2",
            "workspace:w1/reports/b.md", "新执行器的写入",
        )
        self.assertEqual(same.effect_id, again.effect_id)
        effects = list(self.svc.get_instance(self.instance_id).effects.values())
        self.assertEqual(len(effects), 2)
        # 重领是新执行器的新一轮运行：最长运行时间预算按重领时刻重新起算。
        self.assertEqual(self.svc.get_instance(self.instance_id).run_started_at,
                         new_lease.claimed_at)
        self.svc.complete(self.instance_id, new_lease.lease_id)
        self.assertEqual(
            self.svc.get_instance(self.instance_id).status, InstanceStatus.COMPLETED
        )

    def test_shrink_after_partial_effect_enters_compensation_on_reclaim(self) -> None:
        self.svc.record_effect(
            self.instance_id, self.lease.lease_id, "key-1",
            "workspace:w1/reports/a.md", "已写出的报表",
        )
        self.svc._authz.update_authorization(
            "user-1", "tenant-a", {"connector:read": ["connector:w9/*"]}
        )
        self.clock.advance(minutes=6)
        with self.assertRaises(CapabilityDenied):
            self.svc.claim(self.instance_id, "exec-B")
        inst = self.svc.get_instance(self.instance_id)
        self.assertEqual(inst.status, InstanceStatus.COMPENSATING)
        self.assertIsNotNone(inst.compensation)

    def test_heartbeat_shrink_with_effects_enters_compensation(self) -> None:
        self.svc.record_effect(
            self.instance_id, self.lease.lease_id, "key-1",
            "workspace:w1/reports/a.md", "已写出的报表",
        )
        self.svc._authz.update_authorization(
            "user-1", "tenant-a", {"connector:read": ["connector:w9/*"]}
        )
        with self.assertRaises(LeaseLost):
            self.svc.heartbeat(self.instance_id, self.lease.lease_id)
        self.assertEqual(
            self.svc.get_instance(self.instance_id).status, InstanceStatus.COMPENSATING
        )

    def test_max_runtime_exceeded_kills_lease(self) -> None:
        # 租约超时设得比最长运行时间长，确保触发的是 max_run_seconds 而非租约超时。
        clock = MockClock(dt("2026-09-25T00:00:00Z"))
        svc = make_service(clock, lease_timeout=timedelta(hours=2))
        svc.register_job(make_job(clock, max_run_seconds=3600))
        clock.set(dt("2026-09-25T01:00:00Z"))
        (instance_id,) = svc.tick()
        lease = svc.claim(instance_id, "exec-A")
        clock.advance(seconds=3601)
        with self.assertRaises(LeaseLost):
            svc.heartbeat(instance_id, lease.lease_id)
        self.assertEqual(svc.get_instance(instance_id).status, InstanceStatus.CANCELLED)


class CompensationAndDeadLetterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = MockClock(dt("2026-09-25T00:00:00Z"))
        self.svc = make_service(self.clock)
        self.svc.register_job(make_job(self.clock))
        self.clock.set(dt("2026-09-25T01:00:00Z"))
        (self.instance_id,) = self.svc.tick()
        self.lease = self.svc.claim(self.instance_id, "exec-A")
        for key, name in (("k1", "a.md"), ("k2", "b.md")):
            self.svc.record_effect(
                self.instance_id, self.lease.lease_id, key,
                f"workspace:w1/reports/{name}", f"写入 {name}",
            )

    def test_manual_cancel_with_effects_completes_compensation_chain(self) -> None:
        self.svc.request_cancel(self.instance_id, reason="risk_quarantine", detail="风险隔离")
        inst = self.svc.get_instance(self.instance_id)
        self.assertEqual(inst.status, InstanceStatus.COMPENSATING)
        effects = list(inst.effects.values())
        self.svc.compensate_effect(self.instance_id, effects[0].effect_id, "file.delete", "删除报表")
        self.assertEqual(inst.status, InstanceStatus.COMPENSATING)  # 还有一个未补偿
        self.svc.compensate_effect(self.instance_id, effects[1].effect_id, "file.delete")
        self.assertEqual(inst.status, InstanceStatus.COMPENSATED)
        self.assertIsNotNone(inst.compensation.completed_at)
        audit = self.svc.explain_instance(self.instance_id)
        self.assertTrue(all(s for s in [e["compensated"] for e in audit["effects"]]))
        self.assertEqual(len(audit["compensation"]["steps"]), 2)

    def test_dead_letter_then_requeue_and_finish(self) -> None:
        self.svc.request_cancel(self.instance_id, reason="downside_unavailable")
        self.svc.dead_letter(self.instance_id, "补偿下游暂时不可用，等待人工")
        self.assertEqual(
            self.svc.get_instance(self.instance_id).status, InstanceStatus.DEAD_LETTERED
        )
        self.assertEqual([d.instance_id for d in self.svc.list_dead_letters()], [self.instance_id])
        self.svc.requeue_dead_letter(self.instance_id)
        self.assertEqual(
            self.svc.get_instance(self.instance_id).status, InstanceStatus.COMPENSATING
        )
        for effect in self.svc.get_instance(self.instance_id).effects.values():
            self.svc.compensate_effect(self.instance_id, effect.effect_id, "file.delete")
        self.assertEqual(
            self.svc.get_instance(self.instance_id).status, InstanceStatus.COMPENSATED
        )


class RecoveryTest(unittest.TestCase):
    def test_restart_restores_windows_leases_dead_letters_and_next_fire(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            clock = MockClock(dt("2026-09-25T00:00:00Z"))
            store = EventStore(path)
            svc = make_service(clock, store=store)
            svc.register_job(make_job(clock))
            clock.set(dt("2026-09-25T01:00:00Z"))
            (instance_id,) = svc.tick()
            lease = svc.claim(instance_id, "exec-A")
            svc.record_effect(
                instance_id, lease.lease_id, "k1",
                "workspace:w1/reports/a.md", "报表",
            )
            svc.request_cancel(instance_id, reason="risk_quarantine")
            svc.dead_letter(instance_id, "等待人工")
            # 再造一个超时未决租约
            svc.register_job(make_job(clock, job_id="job-other"))
            clock.set(dt("2026-09-25T02:00:00Z"))
            other_ids = svc.tick()
            other_id = next(i for i in other_ids if i.startswith("inst-job-other"))
            svc.claim(other_id, "exec-Z")
            clock.advance(minutes=6)
            svc.close()

            # 全新进程：重放日志恢复全部状态。
            clock2 = MockClock(clock.now())
            store2 = EventStore(path)
            svc2 = make_service(clock2, store=store2)
            summary = svc2.recovery_summary()
            self.assertEqual(summary["jobs"][0]["job_id"], "job-daily")
            self.assertEqual(summary["jobs"][0]["next_fire_at"], "2026-09-25T03:00:00+00:00")
            self.assertEqual(summary["dead_letters"], [
                {"instance_id": instance_id, "reason": "等待人工"}
            ])
            pending = [p["instance_id"] for p in summary["pending_leases"]]
            self.assertEqual(pending, [other_id])
            # 已开窗的窗口不会在重启后的 tick 中重复生成实例。
            self.assertEqual(svc2.tick(), [])
            # 未决租约可被新执行器重新领取。
            new_lease = svc2.claim(other_id, "exec-Y")
            self.assertEqual(new_lease.attempt, 2)
            # 审计轨迹完整。
            audit = svc2.explain_instance(instance_id)
            events = [t["event"] for t in audit["trail"]]
            self.assertIn("compensation.started", events)
            self.assertIn("instance.dead_lettered", events)
            self.assertEqual(audit["why"]["purpose"], "每小时同步连接器清单到工作区")
            self.assertEqual(
                {c["capability"] for c in audit["capabilities_used"]},
                {"connector:read", "workspace:file:write"},
            )
            store2.close()

    def test_trailing_half_written_line_is_quarantined(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            clock = MockClock(dt("2026-09-25T00:00:00Z"))
            store = EventStore(path)
            svc = make_service(clock, store=store)
            svc.register_job(make_job(clock))
            store.close()
            # 模拟进程在写日志中途崩溃：末尾追加半截 JSON（无换行）。
            with path.open("a", encoding="utf-8") as fh:
                fh.write('{"seq": 99, "event_type": "window.op')
            reopened = EventStore(path)
            svc2 = make_service(clock, store=reopened)
            self.assertIn("job-daily", svc2.next_fire_times())
            quarantine = path.with_suffix(path.suffix + ".quarantine")
            self.assertTrue(quarantine.exists())
            self.assertIn("window.op", quarantine.read_text(encoding="utf-8"))
            # 隔离后日志恢复正常追加能力。
            clock.set(dt("2026-09-25T01:00:00Z"))
            self.assertEqual(len(svc2.tick()), 1)
            reopened.close()


if __name__ == "__main__":
    unittest.main()
