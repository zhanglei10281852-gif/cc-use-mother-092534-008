# 后台任务能力票据调度

完整的领域服务实现：后台任务定义、时区计划与补跑、能力票据签发与复核、租约执行、
幂等外部效果、补偿链、死信与重启恢复。仓库同时保留领域合同、策略样例和事件资料。

## 目录

- `scheduler/`：可直接使用的服务实现（仅依赖 Python 3.11+ 标准库）。
  - `clock.py`：时钟抽象（`SystemClock` / 可推进的 `MockClock`）。
  - `calendar.py`：IANA 时区 cron 计划、夏令时 gap/重叠处理、错过触发补跑策略。
  - `authz.py`：版本化授权、租户状态（停用/风险隔离）、HMAC 签名的不可转让短期票据。
  - `eventlog.py`：只追加 JSONL 事件日志，末尾半截写损坏行自动隔离。
  - `service.py`：门面服务 `SchedulerService`（调度、签票、租约、效果、补偿、审计、恢复）。
- `domain/contract.json`：实体、状态、事件类型和关键业务规则。
- `domain/policies.json`：可被程序读取的策略样例。
- `examples/events.json`：按业务发生时间排列的事件样例（含完整补偿链）。
- `examples/demo.py`：端到端演示脚本。
- `tools/validate_contract.py`：使用 Python 标准库和 SQLite 内存表验证资料一致性。
- `tests/`：领域资料校验 + 服务的端到端测试（25 个用例）。

## 核心语义

| 需求 | 实现 |
| --- | --- |
| 任务定义保存目的、资源范围、计划时区、最长运行时间 | `authz.JobDefinition` + `calendar.Schedule`；定义按 `revision` 版本追加 |
| 每次入队按当前授权签发不可转让短期票据 | `AuthorizationService.issue_ticket`：HMAC 签名，钉住实例与授权版本，有 TTL |
| 领取时再次校验版本 | `claim`：复核签名/实例绑定/有效期/授权版本/租户状态；重领强制按当前授权重签 |
| 授权收缩、租户停用、风险隔离 | 领取、心跳、每次 `tick` 主动巡检三处生效；收缩即撤销，扩张不影响已签票据 |
| 未产生外部效果的实例取消 | `cancelled`（直接终态） |
| 已有部分效果的实例取消 | `compensating` → 全部效果补偿完成 → `compensated`，全过程可审计 |
| 跨夏令时 | 春跳 gap 可选跳变前/后；秋叠固定取第一次出现，窗口键不重复 |
| 错过触发补跑 | `skip` / `catch_up`（逐窗口补） / `catch_up_latest`（只补最近一个） |
| 同一计划窗口只有一个实例 | 窗口键 `(job_id, window_key)` 唯一，重复 tick/重启都不重复生成 |
| 租约超时重领不重复提交副作用 | 原子作废旧租约（attempt 递增）；外部效果以幂等键登记，重复提交返回同一回执 |
| 值班员查询"为何运行/用了哪些能力/补偿是否完成" | `explain_instance`：目的、窗口、授权版本、能力、效果、补偿步骤、完整事件轨迹 |
| 重启恢复未决租约、死信、下一触发点 | 事件溯源：重放 JSONL 日志；`recovery_summary` 一页汇总 |

## 构建

```bash
python3 -m compileall -q .
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 资料校验

```bash
python3 tools/validate_contract.py
```

## 端到端演示

```bash
python3 examples/demo.py
```

所有命令都在项目根目录执行，不需要另行启动数据库、缓存或其他服务；持久化只使用
一个 JSONL 事件日志文件。
