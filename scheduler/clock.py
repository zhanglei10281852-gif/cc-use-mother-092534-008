"""时钟抽象。

领域规则要求所有时间点都带时区（见 ``domain/contract.json`` 的 ``time_policy``），
生产使用 UTC 的 :class:`SystemClock`，测试和演示用可推进的 :class:`MockClock`。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """返回当前带时区的时间。"""


class SystemClock:
    """真实墙上时钟，统一使用 UTC。"""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class MockClock:
    """可显式推进的时钟，初始时间必须带时区，默认取 UTC。"""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        if self._now.tzinfo is None:
            raise ValueError("MockClock 初始时间必须带时区")

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float = 0, **kwargs: float) -> datetime:
        delta = timedelta(seconds=seconds, **kwargs)
        self._now = self._now + delta
        return self._now

    def set(self, value: datetime) -> None:
        if value.tzinfo is None:
            raise ValueError("设置的时间必须带时区")
        self._now = value
