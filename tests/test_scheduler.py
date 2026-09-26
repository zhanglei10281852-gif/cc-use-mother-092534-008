"""调度服务端到端行为测试。时间全部显式注入，确保确定性。"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from scheduler import (
    Authorization,
    Capability,
    GapPolicy,
    JobSpec,
    LeaseLost,
    MisfirePolicy,
    OverlapPolicy,
    SchedulerService,
    TicketInvalid,
)
from scheduler.models import InstanceState
from scheduler.store import Store
from scheduler.timeeval import CronSpec, resolve_local, window_key

UTC = timezone.utc
NY = ZoneInfo("America/New_York")
SH = ZoneInfo("Asia/Shanghai")

READ_FILE = Capability("file.read", "workspace_file", "workspace-7", "report.csv")
READ_CONNECTOR = Capability("connector.read", "connector", "crm", "*")
SEND = Capability("message.send", "channel", "ops", "*")
FULL = frozenset({READ_FILE, READ_CONNECTOR})


def make_service(lease_seconds: int = 60) -> SchedulerService:
    return SchedulerService(Store.open(":memory:"), lease_seconds=lease_seconds)


def daily_job(
    misfire=MisfirePolicy.SCHEDULED,
    tz="Asia/Shanghai",
    cron="30 9 * * *",
    max_runtime=3600,
    gap=GapPolicy.FORWARD,
    overlap=OverlapPolicy.EARLY,
    max_backfill=3,
) -> JobSpec:
    return JobSpec(
        job_id="job-daily",
        tenant_id="tenant-1",
        subject_id="agent-creator-1",
        version=1,
        purpose="每日同步运营报表到工作区文件",
        cron=cron,
        timezone=tz,
        required_capabilities=FULL,
        max_runtime_seconds=max_runtime,
        misfire_policy=misfire,
        max_backfill=max_backfill,
        gap_policy=gap,
        overlap_policy=overlap,
    )


def bootstrap(service: SchedulerService, spec: JobSpec | None = None, at=None):
    spec = spec or daily_job()
    at = at or datetime(2026, 9, 25, 1, 0, tzinfo=UTC)
    service.set_tenant_status("tenant-1", "active", at)
    service.put_authorization(
        Authorization("tenant-1", "agent-creator-1", 1, FULL), at
    )
    service.register_job(spec, at)
    return spec, at


class TimeEvaluationTest(unittest.TestCase):
    def test_spring_gap_forward_moves_to_post_gap_instant(self):
        local = datetime(2026, 3, 8, 2, 30)
        moved = resolve_local(local, NY, gap_policy="forward")
        self.assertEqual(moved.kind, "gap_forward")
        # 缺口前移到 03:30 EDT（07:30Z）。
        self.assertEqual(moved.scheduled_utc, datetime(2026, 3, 8, 7, 30, tzinfo=UTC))

    def test_spring_gap_skip_marks_skip(self):
        local = datetime(2026, 3, 8, 2, 30)
        skipped = resolve_local(local, NY, gap_policy="skip")
        self.assertEqual(skipped.kind, "gap_skip")

    def test_fall_overlap_early_vs_late(self):
        local = datetime(2026, 11, 1, 1, 30)
        early = resolve_local(local, NY, overlap_policy="early")
        late = resolve_local(local, NY, overlap_policy="late")
        self.assertEqual(early.scheduled_utc, datetime(2026, 11, 1, 5, 30, tzinfo=UTC))
        self.assertEqual(late.scheduled_utc, datetime(2026, 11, 1, 6, 30, tzinfo=UTC))

    def test_window_key_normalizes_timezone(self):
        a = window_key("j", datetime(2026, 9, 25, 9, 30, tzinfo=SH))
        b = window_key("j", datetime(2026, 9, 25, 1, 30, tzinfo=UTC))
        self.assertEqual(a, b)


class ScheduleAndTicketTest(unittest.TestCase):
    def test_window_opens_enqueues_and_issues_short_lived_ticket(self):
        service = make_service()
        bootstrap(service)
        result = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))
        self.assertEqual(len(result["enqueued"]), 1)
        audit = service.audit(result["enqueued"][0])
        self.assertEqual(audit.state, InstanceState.QUEUED)
        self.assertEqual(audit.purpose, "每日同步运营报表到工作区文件")
        self.assertEqual(audit.ticket_capabilities, FULL)
        self.assertEqual(audit.auth_version_at_issue, 1)
        types = [e.event_type for e in audit.timeline]
        self.assertIn("window.opened", types)
        self.assertIn("instance.enqueued", types)
        self.assertIn("ticket.issued", types)
        # 下一触发点推进到次日。
        self.assertEqual(service.next_trigger("job-daily"), datetime(2026, 9, 26, 1, 30, tzinfo=UTC))

    def test_same_window_never_creates_second_instance(self):
        service = make_service()
        bootstrap(service)
        at = datetime(2026, 9, 25, 1, 30, tzinfo=UTC)
        first = service.tick(at)["enqueued"]
        second = service.tick(at + timedelta(minutes=30))
        self.assertEqual(len(first), 1)
        self.assertEqual(second["enqueued"], [])

    def test_enqueue_without_authorization_cancels_before_effects(self):
        service = make_service()
        spec = daily_job()
        at = datetime(2026, 9, 25, 1, 0, tzinfo=UTC)
        service.set_tenant_status("tenant-1", "active", at)
        service.register_job(spec, at)
        result = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))
        self.assertEqual(len(result["enqueued"]), 1)
        audit = service.audit(result["enqueued"][0])
        self.assertEqual(audit.state, InstanceState.CANCELLED)
        self.assertEqual(audit.auth_fingerprint, "none")

    def test_missing_auth_none_policy_drops_missed_but_runs_ontime(self):
        service = make_service()
        spec = daily_job(misfire=MisfirePolicy.NONE)
        bootstrap(service, spec)
        # 停机 3 天后恢复：错过窗口全部放弃，不补跑。
        result = service.tick(datetime(2026, 9, 28, 2, 0, tzinfo=UTC))
        self.assertEqual(result["enqueued"], [])
        # 准点 tick 仍然开窗执行。
        ontime = service.tick(datetime(2026, 9, 29, 1, 30, tzinfo=UTC))
        self.assertEqual(len(ontime["enqueued"]), 1)

    def test_scheduled_misfire_runs_only_latest_window(self):
        service = make_service()
        spec = daily_job(misfire=MisfirePolicy.SCHEDULED)
        bootstrap(service, spec)
        result = service.tick(datetime(2026, 9, 28, 2, 0, tzinfo=UTC))
        self.assertEqual(len(result["enqueued"]), 1)
        audit = service.audit(result["enqueued"][0])
        # 补的是最近一个窗口（09-28 01:30Z）。
        self.assertEqual(audit.scheduled_for_utc, datetime(2026, 9, 28, 1, 30, tzinfo=UTC))

    def test_backfill_policy_creates_each_window_up_to_limit(self):
        service = make_service()
        spec = daily_job(misfire=MisfirePolicy.BACKFILL, max_backfill=2)
        bootstrap(service, spec)
        result = service.tick(datetime(2026, 9, 28, 2, 0, tzinfo=UTC))
        # 三个错过窗口，只补最近两个。
        self.assertEqual(len(result["enqueued"]), 2)
        scheduled = sorted(service.audit(i).scheduled_for_utc for i in result["enqueued"])
        self.assertEqual(
            scheduled,
            [datetime(2026, 9, 27, 1, 30, tzinfo=UTC), datetime(2026, 9, 28, 1, 30, tzinfo=UTC)],
        )


class ClaimAndLeaseTest(unittest.TestCase):
    def _enqueued(self, service):
        bootstrap(service)
        return service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]

    def test_claim_rechecks_versions_and_grants_generation_one(self):
        service = make_service(lease_seconds=60)
        instance_id = self._enqueued(service)
        grant = service.claim(instance_id, "executor-A", datetime(2026, 9, 25, 1, 31, tzinfo=UTC))
        self.assertEqual(grant.generation, 1)
        self.assertEqual(grant.capabilities, FULL)
        self.assertEqual(grant.auth_version, 1)
        self.assertTrue(grant.deadline > datetime(2026, 9, 25, 1, 31, tzinfo=UTC))

    def test_authorization_expansion_does_not_block_queued_instance(self):
        service = make_service()
        instance_id = self._enqueued(service)
        expanded = FULL | frozenset({SEND})
        service.put_authorization(
            Authorization("tenant-1", "agent-creator-1", 2, expanded),
            datetime(2026, 9, 25, 1, 31, tzinfo=UTC),
        )
        grant = service.claim(instance_id, "executor-A", datetime(2026, 9, 25, 1, 32, tzinfo=UTC))
        # 透明重绑到新版本。
        self.assertEqual(grant.auth_version, 2)
        audit = service.audit(instance_id)
        self.assertIn(SEND, audit.ticket_capabilities)

    def test_authorization_shrink_at_claim_cancels_without_effects(self):
        service = make_service()
        instance_id = self._enqueued(service)
        shrunk = frozenset({READ_FILE})  # 丢掉 connector 读取
        affected = service.put_authorization(
            Authorization("tenant-1", "agent-creator-1", 2, shrunk),
            datetime(2026, 9, 25, 1, 31, tzinfo=UTC),
        )
        self.assertEqual(affected, 1)
        audit = service.audit(instance_id)
        self.assertEqual(audit.state, InstanceState.CANCELLED)
        with self.assertRaises(TicketInvalid):
            service.claim(instance_id, "executor-A", datetime(2026, 9, 25, 1, 32, tzinfo=UTC))

    def test_shrink_after_partial_effect_enters_compensation(self):
        service = make_service()
        instance_id = self._enqueued(service)
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        grant = service.claim(instance_id, "executor-A", t0)
        service.start_running(grant.token, t0)
        service.record_effect(grant.token, "eff-1", "connector.write", "crm:account-9", t0)
        affected = service.put_authorization(
            Authorization("tenant-1", "agent-creator-1", 2, frozenset({READ_FILE})),
            datetime(2026, 9, 25, 1, 32, tzinfo=UTC),
        )
        self.assertEqual(affected, 1)
        audit = service.audit(instance_id)
        self.assertEqual(audit.state, InstanceState.COMPENSATING)
        self.assertEqual(len(audit.compensations), 1)
        # 旧执行器的心跳与提交必须被挡下。
        with self.assertRaises(LeaseLost):
            service.heartbeat(grant.token, datetime(2026, 9, 25, 1, 32, 30, tzinfo=UTC))

    def test_tenant_disable_cancels_queued_instances(self):
        service = make_service()
        instance_id = self._enqueued(service)
        affected = service.set_tenant_status(
            "tenant-1", "disabled", datetime(2026, 9, 25, 1, 31, tzinfo=UTC), reason="offboard"
        )
        self.assertEqual(affected, 1)
        self.assertEqual(service.audit(instance_id).state, InstanceState.CANCELLED)

    def test_quarantine_running_with_effects_compensates(self):
        service = make_service()
        instance_id = self._enqueued(service)
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        grant = service.claim(instance_id, "executor-A", t0)
        service.record_effect(grant.token, "eff-1", "file.write", "workspace-7:report.csv", t0)
        service.set_tenant_status("tenant-1", "quarantined", t0 + timedelta(seconds=10), reason="risk")
        audit = service.audit(instance_id)
        self.assertEqual(audit.state, InstanceState.COMPENSATING)

    def test_lease_timeout_reclaim_uses_new_generation_and_dedupes_effects(self):
        service = make_service(lease_seconds=30)
        instance_id = self._enqueued(service)
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        first = service.claim(instance_id, "executor-A", t0)
        service.start_running(first.token, t0)
        service.record_effect(first.token, "eff-1", "connector.write", "crm:a1", t0)
        # 未心跳，30 秒后租约超时。
        tick = service.tick(t0 + timedelta(seconds=31))
        self.assertIn(first.lease_id, tick["expired_leases"])
        self.assertEqual(service.audit(instance_id).state, InstanceState.QUEUED)
        # 旧世代 token 立即失效。
        with self.assertRaises(LeaseLost):
            service.record_effect(
                first.token, "eff-2", "connector.write", "crm:a2", t0 + timedelta(seconds=32)
            )
        # 新执行者重领（世代号 2），重复提交同一 effect_key 幂等返回既有回执。
        second = service.claim(instance_id, "executor-B", t0 + timedelta(seconds=33))
        self.assertEqual(second.generation, 2)
        replay = service.record_effect(
            second.token, "eff-1", "connector.write", "crm:a1", t0 + timedelta(seconds=34)
        )
        self.assertEqual(replay.generation, 1)
        service.record_effect(second.token, "eff-2", "connector.write", "crm:a2", t0 + timedelta(seconds=35))
        service.complete(second.token, t0 + timedelta(seconds=40))
        audit = service.audit(instance_id)
        self.assertEqual(audit.state, InstanceState.COMPLETED)
        self.assertEqual({e.effect_key for e in audit.effects}, {"eff-1", "eff-2"})

    def test_hard_deadline_fires_even_when_heartbeats_keep_lease_alive(self):
        service = make_service(lease_seconds=120)
        bootstrap(service, daily_job(max_runtime=60))
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        grant = service.claim(instance_id, "executor-A", t0)
        service.start_running(grant.token, t0)
        # 持续心跳，软截止永不超时；但 60 秒硬截止仍须强制收敛。
        service.heartbeat(grant.token, t0 + timedelta(seconds=40))
        service.tick(t0 + timedelta(seconds=61))
        audit = service.audit(instance_id)
        self.assertEqual(audit.state, InstanceState.CANCELLED)
        with self.assertRaises(LeaseLost):
            service.heartbeat(grant.token, t0 + timedelta(seconds=62))

    def test_hard_deadline_does_not_refresh_runtime_budget(self):
        service = make_service(lease_seconds=30)
        bootstrap(service, daily_job(max_runtime=60))
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        grant = service.claim(instance_id, "executor-A", t0)
        service.record_effect(grant.token, "eff-1", "file.write", "ws:x", t0)
        # 超过最长运行时间：即便有心跳空间也终结并补偿，不允许重领刷新预算。
        service.tick(t0 + timedelta(seconds=61))
        audit = service.audit(instance_id)
        self.assertEqual(audit.state, InstanceState.COMPENSATING)

    def test_heartbeat_rejected_after_hard_deadline(self):
        service = make_service(lease_seconds=120)
        bootstrap(service, daily_job(max_runtime=60))
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        grant = service.claim(instance_id, "executor-A", t0)
        with self.assertRaises(LeaseLost):
            service.heartbeat(grant.token, t0 + timedelta(seconds=61))


class CompensationTest(unittest.TestCase):
    def _instance_with_effects(self, count=2):
        service = make_service()
        spec, _ = bootstrap(service)
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        grant = service.claim(instance_id, "executor-A", t0)
        for i in range(1, count + 1):
            service.record_effect(grant.token, f"eff-{i}", "write", f"resource-{i}", t0)
        service.request_cancel(instance_id, t0 + timedelta(seconds=5), reason="shrink")
        return service, instance_id

    def test_successful_chain_cancels_instance(self):
        service, instance_id = self._instance_with_effects()
        undone = []

        def compensator(action, key, resource):
            undone.append((key, resource))

        state = service.run_compensation(instance_id, compensator, datetime(2026, 9, 25, 1, 35, tzinfo=UTC))
        self.assertEqual(state, InstanceState.CANCELLED)
        self.assertEqual({k for k, _ in undone}, {"eff-1", "eff-2"})
        audit = service.audit(instance_id)
        self.assertTrue(audit.compensation_complete)
        self.assertTrue(all(c.state.value == "done" for c in audit.compensations))

    def test_retrying_then_dead_letter_and_reopen(self):
        service, instance_id = self._instance_with_effects()
        attempts = {"n": 0}

        def flaky(action, key, resource):
            attempts["n"] += 1
            if key == "eff-1":
                raise RuntimeError("下游撤销接口失败")

        t = datetime(2026, 9, 25, 1, 35, tzinfo=UTC)
        state = service.run_compensation(instance_id, flaky, t)
        self.assertEqual(state, InstanceState.DEAD_LETTERED)
        self.assertIn(instance_id, service.list_dead_letters())
        audit = service.audit(instance_id)
        failed = [c for c in audit.compensations if c.state.value == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0].attempts, 3)
        # 审计后重新发起：这次补偿成功。
        service.reopen_dead_letter(instance_id, t + timedelta(minutes=5), actor_id="oncall-lee")
        state2 = service.run_compensation(
            instance_id, lambda a, k, r: None, t + timedelta(minutes=6)
        )
        self.assertEqual(state2, InstanceState.CANCELLED)
        self.assertEqual(service.audit(instance_id).compensation_complete, True)
        self.assertEqual(service.list_dead_letters(), [])

    def test_compensation_runs_in_reverse_effect_order_saga(self):
        service, instance_id = self._instance_with_effects(count=3)
        order = []

        def compensator(action, key, resource):
            order.append(key)

        service.run_compensation(instance_id, compensator, datetime(2026, 9, 25, 1, 35, tzinfo=UTC))
        # 后产生的效果先撤销。
        self.assertEqual(order, ["eff-3", "eff-2", "eff-1"])


class RecoveryTest(unittest.TestCase):
    def test_restart_recovers_leases_instances_and_next_trigger(self):
        service = make_service(lease_seconds=30)
        bootstrap(service)
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        grant = service.claim(instance_id, "executor-A", datetime(2026, 9, 25, 1, 31, tzinfo=UTC))
        service.start_running(grant.token, datetime(2026, 9, 25, 1, 31, tzinfo=UTC))

        # 模拟调度器重启：新服务复用同一数据库文件。
        path = f"/tmp/sched-recovery-{id(self)}.db"
        service.store.connection.commit()
        import sqlite3
        # :memory: 库不能跨连接共享，这里直接对同一 Store 调 recover 验证语义。
        report = service.recover(datetime(2026, 9, 25, 1, 32, tzinfo=UTC))
        self.assertEqual(report["leases_expired"], 1)
        self.assertEqual(report["instances_requed"], 1)
        self.assertEqual(service.audit(instance_id).state, InstanceState.QUEUED)
        with self.assertRaises(LeaseLost):
            service.heartbeat(grant.token, datetime(2026, 9, 25, 1, 32, tzinfo=UTC))
        # 下一触发点保持为 09-26，重启不产生重复窗口。
        self.assertEqual(service.next_trigger("job-daily"), datetime(2026, 9, 26, 1, 30, tzinfo=UTC))

    def test_restart_with_file_backed_store_preserves_state(self):
        import os
        import tempfile

        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            store = Store.open(path)
            service = SchedulerService(store, lease_seconds=30)
            bootstrap(service)
            enqueued = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"]
            self.assertEqual(len(enqueued), 1)
            store.close()

            store2 = Store.open(path)
            service2 = SchedulerService(store2, lease_seconds=30)
            report = service2.recover(datetime(2026, 9, 25, 1, 31, tzinfo=UTC))
            self.assertEqual(report["instances_requed"], 0)
            self.assertEqual(service2.next_trigger("job-daily"), datetime(2026, 9, 26, 1, 30, tzinfo=UTC))
            audit = service2.audit(enqueued[0])
            self.assertEqual(audit.purpose, "每日同步运营报表到工作区文件")
            store2.close()
        finally:
            os.unlink(path)


class AuditTimelineTest(unittest.TestCase):
    def test_timeline_explains_why_instance_ran_and_what_it_used(self):
        service = make_service()
        bootstrap(service)
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        grant = service.claim(instance_id, "executor-A", t0)
        service.record_effect(grant.token, "eff-1", "connector.write", "crm:a1", t0)
        service.complete(grant.token, t0 + timedelta(seconds=10))
        audit = service.audit(instance_id)
        ordering = [e.event_type for e in audit.timeline]
        self.assertEqual(
            ordering,
            [
                "window.opened",
                "instance.enqueued",
                "ticket.issued",
                "lease.claimed",
                "effect.recorded",
                "instance.completed",
            ],
        )
        self.assertEqual(audit.lease_generation, 1)
        self.assertEqual(audit.window_key, "job-daily@2026-09-25T01:30:00Z")
        self.assertEqual(audit.effects[0].resource, "crm:a1")


class DstEndToEndTest(unittest.TestCase):
    def test_spring_gap_forward_runs_once_at_post_gap_instant(self):
        service = make_service()
        # 美国东部每天 02:30；2026-03-08 落入缺口，forward 后移到 03:30（07:30Z）。
        spec = daily_job(tz="America/New_York", cron="30 2 * * *")
        bootstrap(
            service,
            spec,
            at=datetime(2026, 3, 7, 8, 0, tzinfo=UTC),
        )
        self.assertEqual(service.next_trigger("job-daily"), datetime(2026, 3, 8, 7, 30, tzinfo=UTC))
        result = service.tick(datetime(2026, 3, 8, 7, 30, tzinfo=UTC))
        self.assertEqual(len(result["enqueued"]), 1)
        # 下一次触发回到常规 02:30 EDT = 06:30Z。
        self.assertEqual(service.next_trigger("job-daily"), datetime(2026, 3, 9, 6, 30, tzinfo=UTC))

    def test_spring_gap_skip_produces_no_instance_but_leaves_audit_window(self):
        service = make_service()
        spec = daily_job(tz="America/New_York", cron="30 2 * * *", gap=GapPolicy.SKIP)
        bootstrap(service, spec, at=datetime(2026, 3, 7, 8, 0, tzinfo=UTC))
        result = service.tick(datetime(2026, 3, 8, 8, 0, tzinfo=UTC))
        self.assertEqual(result["enqueued"], [])
        rows = service.store.connection.execute(
            "select resolution, suppressed from windows"
        ).fetchall()
        self.assertEqual([(r["resolution"], r["suppressed"]) for r in rows], [("gap_skip", 1)])

    def test_fall_overlap_does_not_duplicate_window(self):
        service = make_service()
        spec = daily_job(tz="America/New_York", cron="30 1 * * *", overlap=OverlapPolicy.EARLY)
        bootstrap(service, spec, at=datetime(2026, 10, 31, 5, 0, tzinfo=UTC))
        # 早次 01:30 EDT = 05:30Z；晚次若被重复枚举也必须被窗口键挡下。
        result = service.tick(datetime(2026, 11, 1, 6, 0, tzinfo=UTC))
        self.assertEqual(len(result["enqueued"]), 1)
        audit = service.audit(result["enqueued"][0])
        self.assertEqual(audit.scheduled_for_utc, datetime(2026, 11, 1, 5, 30, tzinfo=UTC))


class GracefulCancelTest(unittest.TestCase):
    def test_graceful_cancel_keeps_lease_until_executor_releases(self):
        service = make_service(lease_seconds=120)
        bootstrap(service)
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        grant = service.claim(instance_id, "executor-A", t0)
        service.start_running(grant.token, t0)
        service.request_cancel(
            instance_id, t0 + timedelta(seconds=5), reason="risk", immediate=False
        )
        # 租约仍可心跳，但执行器能观察到取消信号。
        self.assertTrue(service.cancel_requested(grant.token))
        service.heartbeat(grant.token, t0 + timedelta(seconds=10))
        # 无外部效果，主动释放后实例取消。
        service.release(grant.token, t0 + timedelta(seconds=15))
        self.assertEqual(service.audit(instance_id).state, InstanceState.CANCELLED)

    def test_graceful_cancel_with_effects_goes_to_compensation_on_release(self):
        service = make_service(lease_seconds=120)
        bootstrap(service)
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        grant = service.claim(instance_id, "executor-A", t0)
        service.record_effect(grant.token, "eff-1", "write", "r-1", t0)
        service.request_cancel(instance_id, t0 + timedelta(seconds=5), reason="risk", immediate=False)
        service.release(grant.token, t0 + timedelta(seconds=10))
        audit = service.audit(instance_id)
        self.assertEqual(audit.state, InstanceState.COMPENSATING)


class TicketReissueTest(unittest.TestCase):
    def test_near_expired_ticket_is_reissued_against_current_authorization(self):
        service = make_service(lease_seconds=60)
        bootstrap(service)
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        # 票据 TTL 为 10 分钟；临近到期才领取，应透明重签。
        grant = service.claim(instance_id, "executor-A", datetime(2026, 9, 25, 1, 39, 45, tzinfo=UTC))
        self.assertEqual(grant.generation, 1)
        tickets = service.store.connection.execute(
            "select status from capability_tickets where instance_id=? order by issued_at",
            (instance_id,),
        ).fetchall()
        self.assertEqual([r["status"] for r in tickets], ["expired", "valid"])

    def test_near_expired_ticket_with_shrunk_authorization_is_rejected(self):
        service = make_service(lease_seconds=60)
        bootstrap(service)
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        service.put_authorization(
            Authorization("tenant-1", "agent-creator-1", 2, frozenset({READ_FILE})),
            datetime(2026, 9, 25, 1, 35, tzinfo=UTC),
        )
        with self.assertRaises(TicketInvalid):
            service.claim(instance_id, "executor-A", datetime(2026, 9, 25, 1, 39, 45, tzinfo=UTC))
        self.assertEqual(service.audit(instance_id).state, InstanceState.CANCELLED)


class DefinitionVersionTest(unittest.TestCase):
    def test_enqueued_instance_is_bound_to_enqueue_time_definition(self):
        service = make_service()
        bootstrap(service)
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        # 发布新版本定义（改计划）：排队实例仍按 v1 执行，不追溯取消。
        v2 = JobSpec(
            job_id="job-daily",
            tenant_id="tenant-1",
            subject_id="agent-creator-1",
            version=2,
            purpose="每日同步运营报表到工作区文件（新版）",
            cron="0 10 * * *",
            timezone="Asia/Shanghai",
            required_capabilities=FULL,
            max_runtime_seconds=3600,
        )
        service.register_job(v2, datetime(2026, 9, 25, 1, 31, tzinfo=UTC))
        grant = service.claim(instance_id, "executor-A", datetime(2026, 9, 25, 1, 32, tzinfo=UTC))
        self.assertEqual(grant.job.version, 1)
        self.assertEqual(service.audit(instance_id).definition_version, 1)

    def test_version_must_be_appended_not_overwritten(self):
        service = make_service()
        bootstrap(service)
        bad = JobSpec(
            job_id="job-daily",
            tenant_id="tenant-1",
            subject_id="agent-creator-1",
            version=1,  # 已存在
            purpose="重复版本",
            cron="0 10 * * *",
            timezone="Asia/Shanghai",
            required_capabilities=FULL,
            max_runtime_seconds=3600,
        )
        with self.assertRaises(ValueError):
            service.register_job(bad, datetime(2026, 9, 25, 1, 31, tzinfo=UTC))

    def test_disabling_job_cancels_its_pending_instances_only(self):
        service = make_service()
        bootstrap(service)
        inst1 = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        other = JobSpec(
            job_id="job-other",
            tenant_id="tenant-1",
            subject_id="agent-creator-1",
            version=1,
            purpose="另一个任务",
            cron="0 10 * * *",
            timezone="Asia/Shanghai",
            required_capabilities=FULL,
            max_runtime_seconds=3600,
        )
        service.register_job(other, datetime(2026, 9, 25, 1, 31, tzinfo=UTC))
        # 另一任务 10:00 SH = 02:00Z 才开窗（同 tick 也会开主任务的新窗口）。
        due = service.tick(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))["enqueued"]
        inst2 = [i for i in due if service.audit(i).job_id == "job-other"]
        self.assertEqual(len(inst2), 1)
        disabled = JobSpec(
            job_id="job-daily",
            tenant_id="tenant-1",
            subject_id="agent-creator-1",
            version=2,
            purpose="每日同步运营报表到工作区文件",
            cron="30 9 * * *",
            timezone="Asia/Shanghai",
            required_capabilities=FULL,
            max_runtime_seconds=3600,
            enabled=False,
        )
        service.register_job(disabled, datetime(2026, 9, 26, 2, 1, tzinfo=UTC))
        self.assertEqual(service.audit(inst1).state, InstanceState.CANCELLED)
        # 另一个启用任务的实例不受影响。
        self.assertEqual(service.audit(inst2[0]).state, InstanceState.QUEUED)


class DeadLetterPersistenceTest(unittest.TestCase):
    def test_dead_letters_survive_scheduler_restart(self):
        import os
        import tempfile

        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            store = Store.open(path)
            service = SchedulerService(store, lease_seconds=60)
            bootstrap(service)
            instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
            t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
            grant = service.claim(instance_id, "executor-A", t0)
            service.record_effect(grant.token, "eff-1", "write", "r-1", t0)
            service.request_cancel(instance_id, t0, reason="risk")

            def always_fail(action, key, resource):
                raise RuntimeError("撤销不可用")

            state = service.run_compensation(instance_id, always_fail, t0)
            self.assertEqual(state, InstanceState.DEAD_LETTERED)
            store.close()

            store2 = Store.open(path)
            service2 = SchedulerService(store2, lease_seconds=60)
            self.assertEqual(service2.list_dead_letters(), [instance_id])
            audit = service2.audit(instance_id)
            self.assertTrue(any(c.state.value == "failed" for c in audit.compensations))
            store2.close()
        finally:
            os.unlink(path)


class ClaimTakeoverTest(unittest.TestCase):
    def test_claim_takes_over_expired_lease_without_waiting_for_tick(self):
        service = make_service(lease_seconds=30)
        bootstrap(service)
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        first = service.claim(instance_id, "executor-A", t0)
        service.record_effect(first.token, "eff-1", "write", "r-1", t0)
        # 不调用 tick，直接在截止后领取：自动接管，世代号 +1。
        second = service.claim(instance_id, "executor-B", t0 + timedelta(seconds=31))
        self.assertEqual(second.generation, 2)
        replay = service.record_effect(second.token, "eff-1", "write", "r-1", t0 + timedelta(seconds=32))
        self.assertEqual(replay.generation, 1)
        with self.assertRaises(LeaseLost):
            service.heartbeat(first.token, t0 + timedelta(seconds=33))

    def test_recover_hard_deadline_routes_to_compensation_not_requeue(self):
        service = make_service(lease_seconds=300)
        bootstrap(service, daily_job(max_runtime=60))
        instance_id = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=UTC))["enqueued"][0]
        t0 = datetime(2026, 9, 25, 1, 31, tzinfo=UTC)
        grant = service.claim(instance_id, "executor-A", t0)
        service.record_effect(grant.token, "eff-1", "write", "r-1", t0)
        report = service.recover(t0 + timedelta(seconds=90))
        self.assertEqual(report["leases_expired"], 1)
        audit = service.audit(instance_id)
        self.assertEqual(audit.state, InstanceState.COMPENSATING)
        # 已在补偿链中的实例不能被再次领取。
        with self.assertRaises(TicketInvalid):
            service.claim(instance_id, "executor-B", t0 + timedelta(seconds=91))


if __name__ == "__main__":
    unittest.main()
