"""SQLite 持久化层。

仅负责建表、事务与事件追加，业务裁决全部在 :mod:`scheduler.service`。
所有时间以带时区的 UTC ISO 字符串存储；窗口键是去重与幂等的核心约束。
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from .timeeval import UTC

SCHEMA = """
create table if not exists job_definitions(
    job_id          text not null,
    tenant_id       text not null,
    version         integer not null,
    payload         text not null,
    enabled         integer not null,
    created_at      text not null,
    primary key(job_id, version)
);

-- 租户状态：active / disabled（停用）/ quarantined（风险隔离）。
create table if not exists tenants(
    tenant_id       text primary key,
    status          text not null,
    reason          text,
    updated_at      text not null
);

create table if not exists authorizations(
    tenant_id       text not null,
    subject_id      text not null,
    version         integer not null,
    active          integer not null,
    fingerprint     text not null,
    payload         text not null,
    created_at      text not null,
    primary key(tenant_id, subject_id, version)
);

-- 每个任务的调度游标：最近已开窗的 UTC 瞬间与下一触发点，重启后据此恢复。
create table if not exists schedule_state(
    job_id          text primary key,
    last_window_utc text,
    next_window_utc text not null,
    updated_at      text not null
);

create table if not exists windows(
    window_key      text primary key,
    job_id          text not null,
    scheduled_utc   text not null,
    local_time      text not null,
    resolution      text not null,
    instance_id     text,
    suppressed      integer not null default 0,
    created_at      text not null
);
create index if not exists idx_windows_job_time on windows(job_id, scheduled_utc);

create table if not exists instances(
    instance_id         text primary key,
    job_id              text not null,
    tenant_id           text not null,
    window_key          text not null unique,
    state               text not null,
    scheduled_for_utc   text not null,
    purpose             text not null,
    definition_version  integer not null,
    enqueue_attempts    integer not null default 0,
    created_at          text not null,
    updated_at          text not null
);
create index if not exists idx_instances_state on instances(state);

-- 票据与实例一一对应于“当前有效代”；历史代通过 status 保留，故用索引而非唯一约束。
create table if not exists capability_tickets(
    ticket_id           text primary key,
    instance_id         text not null,
    auth_version        integer not null,
    auth_fingerprint    text not null,
    scope_json          text not null,
    issued_at           text not null,
    expires_at          text not null,
    status              text not null,
    revoke_reason       text
);
create index if not exists idx_tickets_instance on capability_tickets(instance_id, issued_at);
create index if not exists idx_tickets_status_expiry
    on capability_tickets(status, expires_at);

-- 租约世代：generation 单调递增，旧世代的心跳与提交一律拒绝。
create table if not exists execution_leases(
    lease_id        text primary key,
    instance_id     text not null,
    generation      integer not null,
    secret_hash     text not null,
    status          text not null,
    executor_id     text not null,
    lease_seconds   integer not null,
    claimed_at      text not null,
    deadline        text not null,
    hard_deadline   text not null,
    heartbeat_at    text not null,
    released_at     text,
    unique(instance_id, generation)
);
create index if not exists idx_leases_status_deadline on execution_leases(status, deadline);

-- 外部效果回执：同一实例内 effect_key 唯一，重领后重复提交只返回既有回执。
create table if not exists effect_receipts(
    instance_id     text not null,
    effect_key      text not null,
    action          text not null,
    resource        text not null,
    recorded_at     text not null,
    generation      integer not null,
    primary key(instance_id, effect_key)
);

create table if not exists compensations(
    instance_id     text not null,
    effect_key      text not null,
    action          text not null,
    state           text not null,
    attempts        integer not null,
    max_attempts    integer not null,
    last_error      text,
    updated_at      text not null,
    primary key(instance_id, effect_key)
);

create table if not exists event_log(
    event_id        text primary key,
    event_type      text not null,
    aggregate_id    text not null,
    instance_id     text,
    occurred_at     text not null,
    actor_id        text not null,
    detail_json     text not null
);
create index if not exists idx_event_instance on event_log(instance_id, occurred_at);
create index if not exists idx_event_aggregate_time on event_log(aggregate_id, occurred_at);

-- 键值元数据（格式版本、调度器代次等）。
create table if not exists meta(key text primary key, value text not null);
"""


def iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("持久化时间必须带时区")
    return dt.astimezone(UTC).isoformat()


def parse_iso(raw: str) -> datetime:
    return datetime.fromisoformat(raw)


class Store:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        connection.row_factory = sqlite3.Row
        connection.execute("pragma foreign_keys=on")
        connection.execute("pragma journal_mode=wal")
        connection.executescript(SCHEMA)

    @classmethod
    def open(cls, path: str | Path = ":memory:") -> "Store":
        connection = sqlite3.connect(str(path), isolation_level=None)
        return cls(connection)

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """立即加写锁的事务，避免升级死锁。"""

        connection = self.connection
        connection.execute("begin immediate")
        try:
            yield connection
            connection.execute("commit")
        except Exception:
            connection.execute("rollback")
            raise

    def append_event(
        self,
        connection: sqlite3.Connection,
        event_type: str,
        aggregate_id: str,
        occurred_at: datetime,
        actor_id: str,
        instance_id: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> str:
        event_id = "evt-" + uuid.uuid4().hex
        connection.execute(
            "insert into event_log values(?, ?, ?, ?, ?, ?, ?)",
            (
                event_id,
                event_type,
                aggregate_id,
                instance_id,
                iso(occurred_at),
                actor_id,
                json.dumps(detail or {}, ensure_ascii=False, sort_keys=True),
            ),
        )
        return event_id
