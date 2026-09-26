"""端到端演示：能力票据调度服务的完整生命周期。

运行：

    python3 examples/demo.py

故事线：注册任务 → 错过触发按策略补跑 → 入队签票 → 领取执行 → 产生外部效果 →
租户风险隔离 → 部分效果进入补偿链 → 死信 → 人工重投 → 补偿完成 → 重启恢复。
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scheduler.authz import (
    AuthorizationService,
    CapabilityScope,
    JobDefinition,
    TenantStatus,
)
from scheduler.calendar import MissPolicy, Schedule
from scheduler.clock import MockClock
from scheduler.eventlog import EventStore
from scheduler.service import SchedulerService


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="ticket-scheduler-")
    log_path = Path(tmp) / "events.jsonl"
    clock = MockClock(datetime(2026, 9, 25, 0, 0, tzinfo=timezone.utc))

    authz = AuthorizationService(clock)
    authz.register_tenant("tenant-a")
    authz.update_authorization(
        "user-1",
        "tenant-a",
        {
            "connector:read": ["connector:w1/*"],
            "workspace:file:write": ["workspace:w1/reports/*"],
        },
    )
    svc = SchedulerService(
        clock, authz, store=EventStore(log_path),
        lease_timeout=timedelta(minutes=5),
    )

    # 1) 任务定义：目的、资源范围、计划时区、最长运行时间。
    job = JobDefinition(
        job_id="job-report",
        tenant_id="tenant-a",
        owner_subject_id="user-1",
        purpose="每小时把连接器清单同步到工作区报表",
        required=(
            CapabilityScope("connector:read", frozenset({"connector:w1/*"})),
            CapabilityScope("workspace:file:write", frozenset({"workspace:w1/reports/*"})),
        ),
        schedule=Schedule(
            cron="0 * * * *", timezone="Asia/Shanghai",
            miss_policy=MissPolicy.CATCH_UP,
        ),
        max_run_seconds=900,
        created_at=clock.now(),
    )
    svc.register_job(job)

    # 2) 调度器停机三小时后恢复：三个错过窗口各补一个实例（窗口唯一）。
    clock.set(datetime(2026, 9, 25, 3, 0, tzinfo=timezone.utc))
    enqueued = svc.tick()
    print(f"[补跑] 新实例：{enqueued}")

    instance_id = enqueued[-1]
    # 3) 执行器领取（领取时复核授权版本）。
    lease = svc.claim(instance_id, "executor-01")
    print(f"[领取] {lease.lease_id}，租约到期 {lease.expires_at.isoformat()}")

    # 4) 产生一个已确认外部效果（幂等键）。
    effect = svc.record_effect(
        instance_id, lease.lease_id,
        idempotency_key="report-2026-09-25T03:00",
        resource="workspace:w1/reports/connectors.md",
        summary="写入连接器清单报表",
    )
    print(f"[效果] {effect.effect_id} -> {effect.resource}")

    # 5) 运维执行风险隔离：下一个 tick 立即撤销租约；已有部分效果 → 补偿链。
    authz.set_tenant_status("tenant-a", TenantStatus.QUARANTINED)
    svc.tick()
    inst = svc.get_instance(instance_id)
    print(f"[隔离] 实例状态：{inst.status.value}，原因：{inst.cancel_reason}")

    # 6) 补偿下游暂时不可用 → 死信，保留审计。
    svc.dead_letter(instance_id, "删除接口超时，等待人工")
    print(f"[死信] {[d.instance_id for d in svc.list_dead_letters()]}")

    # 7) 人工处理后重投，完成补偿。
    svc.requeue_dead_letter(instance_id)
    svc.compensate_effect(instance_id, effect.effect_id, "file.delete", "隔离后删除报表", by="operator-01")
    print(f"[补偿完成] {svc.get_instance(instance_id).status.value}")

    # 8) 值班员审计：为何运行、用了哪些能力、补偿是否完成。
    audit = svc.explain_instance(instance_id)
    print("[审计] 目的：", audit["why"]["purpose"])
    print("[审计] 能力：", [c["capability"] for c in audit["capabilities_used"]])
    print("[审计] 轨迹：", [t["event"] for t in audit["trail"]])

    # 9) 模拟调度器重启：新进程重放事件日志恢复全部状态。
    svc.close()
    svc2 = SchedulerService(
        MockClock(clock.now()), authz, store=EventStore(log_path),
    )
    print("[重启恢复]")
    print(json.dumps(svc2.recovery_summary(), ensure_ascii=False, indent=2))
    print(f"事件日志：{log_path}")


if __name__ == "__main__":
    main()
