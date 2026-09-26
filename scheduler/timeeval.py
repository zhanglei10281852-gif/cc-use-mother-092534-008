"""计划时间评估：cron 匹配、夏令时解析、窗口键。

所有对外返回的时间都是带时区的 ``datetime``（内部统一换算为 UTC 存储）。
夏令时策略与任务定义中的字段一致：

- ``gap_policy``：本地时间落入春令时缺口时，``forward`` 前移到缺口后沿，
  ``skip`` 跳过本次触发；
- ``overlap_policy``：本地时间落入秋令时重叠区间时，``early`` 取偏移切换前
  的早次瞬间（fold=0），``late`` 取切换后的晚次瞬间（fold=1）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc

_WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


@dataclass(frozen=True)
class CronSpec:
    """最小 cron 表达式：分 时 日 月 周（周可写 ``mon``-``sun`` 或 0-6，0=周一）。

    日与周同时受限时取并集（与 Vixie cron 一致）；写 ``*`` 表示不限制。
    """

    minute: int
    hour: int
    dom: int | None = None
    month: int | None = None
    dow: int | None = None

    @classmethod
    def parse(cls, expression: str) -> "CronSpec":
        parts = expression.split()
        if len(parts) != 5:
            raise ValueError(f"cron 表达式必须是五段：{expression!r}")

        def fix(segment: str) -> int | None:
            return None if segment == "*" else int(segment)

        minute = int(parts[0])
        hour = int(parts[1])
        dom = fix(parts[2])
        month = fix(parts[3])
        dow_raw = parts[4].lower()
        dow: int | None
        if dow_raw == "*":
            dow = None
        elif dow_raw in _WEEKDAYS:
            dow = _WEEKDAYS[dow_raw]
        else:
            dow = int(dow_raw)
        if not 0 <= minute <= 59 or not 0 <= hour <= 23:
            raise ValueError(f"cron 时、分越界：{expression!r}")
        if dom is not None and not 1 <= dom <= 31:
            raise ValueError(f"cron 日越界：{expression!r}")
        if month is not None and not 1 <= month <= 12:
            raise ValueError(f"cron 月越界：{expression!r}")
        if dow is not None and not 0 <= dow <= 6:
            raise ValueError(f"cron 周越界：{expression!r}")
        return cls(minute, hour, dom, month, dow)

    def matches(self, naive_local: datetime) -> bool:
        if naive_local.minute != self.minute or naive_local.hour != self.hour:
            return False
        if self.month is not None and naive_local.month != self.month:
            return False
        # Python weekday()：周一=0，与本类约定一致。
        if self.dom is not None and self.dow is not None:
            return naive_local.day == self.dom or naive_local.weekday() == self.dow
        if self.dom is not None:
            return naive_local.day == self.dom
        if self.dow is not None:
            return naive_local.weekday() == self.dow
        return True


@dataclass(frozen=True)
class ResolvedTrigger:
    """一次计划触发解析后的结果。"""

    scheduled_utc: datetime
    """计划窗口对应的 UTC 瞬间。"""
    kind: str
    """``normal`` / ``gap_forward`` / ``gap_skip``。"""
    local_time: datetime
    """任务定义里的名义本地时间（naive）。"""


def is_gap(naive_local: datetime, tz: ZoneInfo) -> bool:
    """本地时间是否落入春令时缺口。

    zoneinfo 对缺口时间做前移后的偏移，因此 naive→UTC→naive 不自洽。
    """

    aware = naive_local.replace(tzinfo=tz)
    roundtrip = aware.astimezone(UTC).astimezone(tz).replace(tzinfo=None)
    return roundtrip != naive_local


def is_overlap(naive_local: datetime, tz: ZoneInfo) -> bool:
    """本地时间是否落入秋令时重叠区间（fold=0 与 fold=1 偏移不同）。"""

    early = naive_local.replace(tzinfo=tz, fold=0)
    late = naive_local.replace(tzinfo=tz, fold=1)
    return early.utcoffset() != late.utcoffset()


def resolve_local(
    naive_local: datetime,
    tz: ZoneInfo,
    gap_policy: str = "forward",
    overlap_policy: str = "early",
) -> ResolvedTrigger | None:
    """把名义本地时间解析为确定的 UTC 触发瞬间。

    返回 ``None`` 表示按 ``skip`` 策略跳过本次触发。
    """

    if is_gap(naive_local, tz):
        if gap_policy == "skip":
            return ResolvedTrigger(
                scheduled_utc=naive_local.replace(tzinfo=tz).astimezone(UTC),
                kind="gap_skip",
                local_time=naive_local,
            )
        if gap_policy != "forward":
            raise ValueError(f"未知 gap_policy：{gap_policy!r}")
        # fold=0 时 zoneinfo 已把缺口时间映射到缺口后沿（实测见 README 时区说明）。
        moved = naive_local.replace(tzinfo=tz, fold=0).astimezone(UTC)
        return ResolvedTrigger(moved, "gap_forward", naive_local)

    if is_overlap(naive_local, tz):
        if overlap_policy not in ("early", "late"):
            raise ValueError(f"未知 overlap_policy：{overlap_policy!r}")
        fold = 1 if overlap_policy == "late" else 0
        instant = naive_local.replace(tzinfo=tz, fold=fold).astimezone(UTC)
        return ResolvedTrigger(instant, "normal", naive_local)

    return ResolvedTrigger(naive_local.replace(tzinfo=tz).astimezone(UTC), "normal", naive_local)


def enumerate_windows(
    cron: CronSpec,
    tz: ZoneInfo,
    from_utc: datetime,
    to_utc: datetime,
    gap_policy: str = "forward",
    overlap_policy: str = "early",
) -> list[ResolvedTrigger]:
    """枚举半开区间 ``[from_utc, to_utc)`` 内的计划窗口（UTC 瞬间去重）。

    以一分钟为粒度扫描本地时间；若 cron 精度高于一分钟（本实现不支持），
    缺口前移和重叠选择仍由 :func:`resolve_local` 统一裁决。
    """

    if from_utc.tzinfo is None or to_utc.tzinfo is None:
        raise ValueError("枚举边界必须带时区")
    results: list[ResolvedTrigger] = []
    seen: set[tuple[datetime, str]] = set()
    # 多扫一小时保证秋令时重叠与边界窗口被覆盖；最终以 UTC 区间过滤。
    # 游标必须是 naive 本地时间（resolve_local 的输入约定）。
    cursor = from_utc.astimezone(tz).replace(second=0, microsecond=0, tzinfo=None)
    end_local = to_utc.astimezone(tz).replace(tzinfo=None)
    while cursor <= end_local + timedelta(hours=1):
        if cron.matches(cursor):
            resolved = resolve_local(cursor, tz, gap_policy, overlap_policy)
            if resolved is not None:
                if from_utc <= resolved.scheduled_utc < to_utc:
                    dedupe_key = (resolved.scheduled_utc, resolved.kind)
                    if dedupe_key not in seen:
                        seen.add(dedupe_key)
                        results.append(resolved)
        cursor += timedelta(minutes=1)
    results.sort(key=lambda item: item.scheduled_utc)
    return results


def next_window(
    cron: CronSpec,
    tz: ZoneInfo,
    after_utc: datetime,
    gap_policy: str = "forward",
    overlap_policy: str = "early",
) -> ResolvedTrigger:
    """返回严格晚于 ``after_utc`` 的下一个窗口。"""

    if after_utc.tzinfo is None:
        raise ValueError("after_utc 必须带时区")
    cursor = (after_utc.astimezone(tz) + timedelta(minutes=1)).replace(
        second=0, microsecond=0, tzinfo=None
    )
    # 最多向前扫描 367 天，任何合法 cron（年级别）在此范围内必有匹配。
    deadline = cursor + timedelta(days=367)
    while cursor <= deadline:
        if cron.matches(cursor):
            resolved = resolve_local(cursor, tz, gap_policy, overlap_policy)
            # gap_skip 也返回其唤醒锚点（fold=0 的 UTC 瞬间），
            # 供调度器记录抑制事件后推进到下一窗口。
            if resolved is not None and resolved.scheduled_utc > after_utc:
                return resolved
        cursor += timedelta(minutes=1)
    raise RuntimeError("367 天内未找到下一个窗口，cron 可能非法")


def window_key(job_id: str, scheduled_utc: datetime) -> str:
    """同一任务与计划窗口的唯一键：UTC 瞬间归一化到秒。"""

    if scheduled_utc.tzinfo is None:
        raise ValueError("窗口键必须基于带时区时间")
    utc = scheduled_utc.astimezone(UTC).replace(microsecond=0)
    return f"{job_id}@{utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"
