"""校验领域合同与样例事件，不实现完整业务服务。

除字段、时区、排序外，还校验关键生命周期不变量：
- 已记录外部效果（effect.recorded）的实例若被取消，其间必须出现
  compensation.scheduled 与 compensation.completed（有效果必须走补偿链）；
- compensation.completed 之后才能出现终态 instance.cancelled；
- 事件类型必须在合同登记范围内。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_json(relative_path: str):
    return json.loads((ROOT / relative_path).read_text(encoding="utf-8"))


def validate() -> tuple[int, int, int]:
    contract = load_json("domain/contract.json")
    events = load_json("examples/events.json")
    policies = load_json("domain/policies.json")
    required = {"project", "entities", "states", "event_types", "time_policy", "rules"}
    missing = sorted(required - set(contract))
    if missing:
        raise ValueError("领域合同缺少字段：" + "、".join(missing))
    if contract["time_policy"] != "ISO 8601 with timezone":
        raise ValueError("time_policy 必须明确包含时区")
    for must_have in ("cancelled", "dead_lettered"):
        if must_have not in contract["states"]:
            raise ValueError(f"合同状态必须包含终态：{must_have}")
    allowed = set(contract["event_types"])
    if not isinstance(policies, list) or len(policies) < 3:
        raise ValueError("策略资料至少需要三项")
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "create table event_log(event_id text primary key, event_type text not null, "
        "aggregate_id text not null, occurred_at text not null, actor_id text not null, "
        "seq integer not null)"
    )
    previous = None
    types_in_order: list[str] = []
    for seq, event in enumerate(events):
        if event["event_type"] not in allowed:
            raise ValueError(f"未知事件类型：{event['event_type']}")
        occurred_at = datetime.fromisoformat(event["occurred_at"])
        if occurred_at.tzinfo is None:
            raise ValueError("样例事件必须包含时区")
        if previous is not None and occurred_at < previous:
            raise ValueError("样例事件必须按发生时间排序")
        previous = occurred_at
        types_in_order.append(event["event_type"])
        connection.execute(
            "insert into event_log values (?, ?, ?, ?, ?, ?)",
            (
                event["event_id"],
                event["event_type"],
                event["aggregate_id"],
                event["occurred_at"],
                event["actor_id"],
                seq,
            ),
        )
    connection.commit()

    def index_of(event_type: str) -> int | None:
        return types_in_order.index(event_type) if event_type in types_in_order else None

    effect_at = index_of("effect.recorded")
    cancel_at = index_of("instance.cancelled")
    comp_scheduled = index_of("compensation.scheduled")
    comp_completed = index_of("compensation.completed")
    if effect_at is not None and cancel_at is not None:
        if not (comp_scheduled is not None and effect_at < comp_scheduled < cancel_at):
            raise ValueError("有外部效果的实例取消前必须先进入补偿链（compensation.scheduled）")
        if not (comp_completed is not None and comp_scheduled < comp_completed <= cancel_at):
            raise ValueError("补偿全部完成（compensation.completed）后才能终结为 cancelled")
    if comp_completed is not None and cancel_at is not None and comp_completed > cancel_at:
        raise ValueError("compensation.completed 不能晚于 instance.cancelled")

    # 同一聚合内事件必须按登记顺序出现（开窗早于入队早于领取…）。
    stage_order = [
        "window.opened",
        "instance.enqueued",
        "ticket.issued",
        "lease.claimed",
        "effect.recorded",
    ]
    last_stage = -1
    for event_type in types_in_order:
        if event_type in stage_order:
            stage = stage_order.index(event_type)
            if stage < last_stage:
                raise ValueError(f"生命周期阶段乱序：{event_type}")
            last_stage = stage

    stored = connection.execute("select count(*) from event_log").fetchone()[0]
    connection.close()
    return len(contract["entities"]), stored, len(policies)


if __name__ == "__main__":
    entity_count, event_count, policy_count = validate()
    print(f"合同校验通过：{entity_count} 类实体，{event_count} 条样例事件，{policy_count} 项策略")
