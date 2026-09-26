"""只追加的事件日志与事件溯源基元。

- 存储格式为 JSON Lines，每行一个事件，``seq`` 全局单调递增。
- 追加后 ``flush + fsync``，保证调度器崩溃后已确认事件不丢。
- 打开时重放全部历史；**末尾半截写损坏行**（进程在 write 中途被杀）会被隔离到
  ``<path>.quarantine`` 侧车文件并截断，其余历史照常重放。日志中间出现损坏则
  直接报错——历史不允许跳过（version_policy：新版本追加，不覆盖历史）。
"""
from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator


class EventLogCorrupted(RuntimeError):
    pass


class EventStore:
    def __init__(self, path: str | os.PathLike[str] | None = None) -> None:
        self._path = Path(path) if path else None
        self._lock = threading.Lock()
        self._seq = 0
        if self._path is not None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._quarantine_trailing_line()
            self._fh = self._path.open("a", encoding="utf-")
        else:
            self._fh = None
            self._memory: list[dict[str, Any]] = []

    # ---- 打开时的损坏尾行隔离 ----
    def _quarantine_trailing_line(self) -> None:
        assert self._path is not None
        if not self._path.exists() or self._path.stat().st_size == 0:
            return
        good_length = 0
        trailing_bad: bytes | None = None
        with self._path.open("rb") as fh:
            for raw in fh:  # 二进制迭代期间 tell() 可用
                end = fh.tell()
                if not raw.strip():
                    good_length = end
                    continue
                try:
                    event = json.loads(raw.decode("utf-8"))
                    int(event["seq"])
                except (json.JSONDecodeError, KeyError, TypeError, ValueError, UnicodeDecodeError):
                    # 文件末尾的半行（没有下一个换行）是崩溃写入的典型痕迹。
                    if not raw.endswith(b"\n"):
                        trailing_bad = raw
                        break
                    raise EventLogCorrupted(
                        f"事件日志在偏移 {end - len(raw)} 处出现无法恢复的损坏行"
                    )
                good_length = end
        if trailing_bad is not None:
            quarantine = self._path.with_suffix(
                self._path.suffix + ".quarantine"
            )
            quarantine.write_bytes(trailing_bad)
            with self._path.open("r+b") as fh:
                fh.truncate(good_length)

    # ---- 写入与读取 ----
    def append(
        self,
        event_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
        *,
        occurred_at: datetime | None = None,
        actor_id: str = "scheduler",
    ) -> dict[str, Any]:
        if occurred_at is not None and occurred_at.tzinfo is None:
            raise ValueError("事件时间必须带时区")
        with self._lock:
            self._seq += 1
            event = {
                "event_id": f"evt-{self._seq:08d}-{uuid.uuid4().hex[:8]}",
                "seq": self._seq,
                "event_type": event_type,
                "aggregate_id": aggregate_id,
                "occurred_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
                "actor_id": actor_id,
                "payload": payload,
            }
            line = json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n"
            if self._fh is not None:
                self._fh.write(line)
                self._fh.flush()
                os.fsync(self._fh.fileno())
            else:
                self._memory.append(event)
            return event

    def read_all(self) -> list[dict[str, Any]]:
        return list(self)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        if self._fh is not None:
            ctx = self._path.open("r", encoding="utf-8")
            fh = ctx
        else:
            ctx = None
            fh = None
        try:
            source: Iterable[str] = self._memory if fh is None else fh
            for line in source:
                if line.strip():
                    yield json.loads(line)
        finally:
            if ctx is not None:
                ctx.close()

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                self._fh.close()
                self._fh = None
