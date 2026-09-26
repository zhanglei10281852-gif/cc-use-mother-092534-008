# 后台任务能力票据调度

面向智能体后台定时任务的完整调度服务：任务定义保存目的、资源范围、计划时区与最长运行时间；
每次入队按**当前授权**签发不可转让的短期能力票据；执行器领取时再次复核版本；
授权收缩、租户停用或风险隔离后，未生效实例立即取消，已有部分效果的实例进入可审计补偿链。

## 领域规则速览

| 关注点 | 规则 |
| --- | --- |
| 窗口唯一 | 窗口键 = `任务ID@UTC触发瞬间`（计划时区解析后归一化到秒），同一窗口只有一个实例 |
| 夏令时缺口 | 本地时间落入春令时缺口：`gap_policy=forward` 前移到缺口后沿，`skip` 跳过并留抑制事件 |
| 夏令时重叠 | 秋令时重叠：`overlap_policy=early`（fold=0 早次）或 `late`（fold=1 晚次），UTC 瞬间去重 |
| 错过触发 | `misfire_policy`：`none` 不补跑（仅 60s 宽限内准点执行）、`scheduled` 只补最近窗口、`backfill` 逐窗口补跑且受 `max_backfill` 限制 |
| 票据 | 入队时按当前授权签发 10 分钟短期票据，固化授权版本与能力指纹；不可跨实例转让 |
| 领取复核 | 校验票据状态、租户状态、任务启用状态、当前授权是否仍覆盖所需能力；票据临近到期或授权换版（仍覆盖）时透明重签，收缩/停用则拒绝 |
| 租约世代 | 每次领取 `generation+1`；旧世代的心跳、效果提交一律拒绝。软截止超时可重领，硬截止（最长运行时间）强制收敛不刷新预算 |
| 副作用幂等 | `effect_key` 在实例内唯一；重领后重复提交返回既有回执，不重复外部效果 |
| 取消分流 | 无外部效果 → `cancelled`；有部分效果 → `compensating`，saga 逆序补偿，全部成功才终结 |
| 死信 | 补偿重试耗尽（默认 3 次）→ `dead_lettered`；值班员审计后可 `reopen_dead_letter` 重置预算再补偿 |
| 恢复 | 重启后 `recover()`：active 租约作废（硬截止已过的转补偿）、leased/running 实例重新排队、校准下一触发点；错过窗口由后续 tick 按补跑策略处理 |
| 审计 | `audit(instance_id)` 给出目的、窗口、定义版本、票据能力与授权版本、效果、补偿结果和完整事件时间线 |

## 实例状态机

```
                授权缺失/收缩（无效果）
 scheduled ──开窗──> queued ──领取──> leased ──start──> running
                       │                │                   │
                       │ 收缩/停用/隔离  │  软超时(世代+1)    │ 硬截止
                       ▼                ▼                   ▼
                   cancelled        queued（重领）      取消或补偿
                       ▲                │                   │
                       │                └──complete──> completed
                       │
                  compensating ──全部补偿成功──> cancelled
                       │ 重试耗尽
                       ▼
                  dead_lettered ──reopen──> compensating

 leased/running ──优雅取消──> cancelling ──租约失效/执行器 release──> cancelled | compensating
```

## 目录

- `domain/contract.json`：实体、状态、事件类型和关键业务规则。
- `domain/policies.json`：可被程序读取的策略样例（P-08-01 ~ P-08-06）。
- `examples/events.json`：完整生命周期事件样例（含「有效果 → 补偿链 → 取消」）。
- `scheduler/`：调度服务实现。
  - `timeeval.py`：cron、夏令时缺口/重叠解析、窗口键、窗口枚举。
  - `models.py`：任务定义、授权（版本追加）、能力、审计视图。
  - `store.py`：SQLite 建表、事务（`begin immediate`）、事件追加。
  - `service.py`：开窗/补跑、票据签发与复核、租约世代、效果幂等、取消补偿、恢复、审计。
  - `errors.py`：`LeaseLost` / `TicketInvalid` 等领域异常。
- `tools/validate_contract.py`：用 SQLite 内存表校验资料一致性与生命周期不变量。
- `tests/test_scheduler.py`：40 项端到端行为测试（时间全部显式注入）。

## 使用示例

```python
from datetime import datetime, timezone
from scheduler import (
    SchedulerService, Authorization, Capability, JobSpec,
    GapPolicy, OverlapPolicy, MisfirePolicy,
)
from scheduler.store import Store

service = SchedulerService(Store.open("scheduler.db"), lease_seconds=60)
now = datetime(2026, 9, 25, 1, 0, tzinfo=timezone.utc)

caps = frozenset({
    Capability("file.read", "workspace_file", "workspace-7", "report.csv"),
    Capability("connector.read", "connector", "crm", "*"),
})
service.set_tenant_status("tenant-1", "active", now)
service.put_authorization(Authorization("tenant-1", "agent-creator-1", 1, caps), now)
service.register_job(JobSpec(
    job_id="job-daily", tenant_id="tenant-1", subject_id="agent-creator-1",
    version=1, purpose="每日同步运营报表到工作区文件",
    cron="30 9 * * *", timezone="Asia/Shanghai",
    required_capabilities=caps, max_runtime_seconds=3600,
    misfire_policy=MisfirePolicy.SCHEDULED, max_backfill=3,
    gap_policy=GapPolicy.FORWARD, overlap_policy=OverlapPolicy.EARLY,
), now)

# 调度心跳：开窗、补跑、超时收敛
result = service.tick(datetime(2026, 9, 25, 1, 30, tzinfo=timezone.utc))
instance_id = result["enqueued"][0]

# 执行器领取（内部完成版本/授权/租户/票据复核）
grant = service.claim(instance_id, "executor-A", datetime(2026, 9, 25, 1, 31, tzinfo=timezone.utc))
service.start_running(grant.token, datetime(2026, 9, 25, 1, 31, tzinfo=timezone.utc))
service.heartbeat(grant.token, datetime(2026, 9, 25, 1, 31, 40, tzinfo=timezone.utc))
# 外部副作用成功后登记；同 effect_key 重领重放幂等
service.record_effect(grant.token, "eff-1", "connector.write", "crm:account-9",
                      datetime(2026, 9, 25, 1, 31, 45, tzinfo=timezone.utc))

# 授权收缩（同事务立即处理受影响实例）：本实例已有部分效果 → 进入补偿链
service.put_authorization(
    Authorization("tenant-1", "agent-creator-1", 2, frozenset()),
    datetime(2026, 9, 25, 1, 32, tzinfo=timezone.utc),
)

# 驱动补偿（撤销动作在事务外执行，抛异常计入重试，saga 逆序撤销）
state = service.run_compensation(
    instance_id,
    compensator=lambda action, key, resource: None,
    now=datetime(2026, 9, 25, 1, 33, tzinfo=timezone.utc),
)
assert state.value == "cancelled"

# 值班员审计：为何运行、用了哪些能力、补偿是否完成
audit = service.audit(instance_id)
assert audit.compensation_complete
# 正常完成路径则在最后调用 service.complete(grant.token, now)
```

## 构建与测试

```bash
python3 -m compileall -q .
python3 -m unittest discover -s tests -v
python3 tools/validate_contract.py
```

所有命令都在项目根目录执行，仅依赖 Python 3.11+ 标准库（`sqlite3`、`zoneinfo`），
不需要另行启动数据库或缓存；生产部署时把 `:memory:` 换成文件路径即可获得崩溃恢复能力。
