"""时区感知的计划日历。

任务定义保存 cron 表达式、IANA 时区和错过触发策略（需求：跨夏令时与错过触发
需要明确补跑策略）。本模块只负责"下一次触发时刻"的计算与窗口键，不持久化任何状态。

cron 语法为标准五段式：``分 时 日 月 周``，``*`` 表示任意，支持逗号列表、
连字符区间和 ``*/n`` 步长；周字段 0-6，0 表示周一。

DST 语义
--------
- **春跳 gap**：本地名义触发时刻在该时区不存在（时钟前跳），按 ``gap_skip``
  选择：``after`` 取跳变后的同一绝对时刻（wall+offset），``before`` 取跳变前。
- **秋叠 ambiguous**：同一本地时刻出现两次，固定取 fold=0（第一次），保证同一
  计划窗口只产生一个窗口键、一个实例。

错过触发策略 :class:`MissPolicy`
--------------------------------
- ``skip``：错过即放弃，不补跑。
- ``catch_up``：停机期间每个错过的窗口各补一个实例（窗口唯一，不重复）。
- ``catch_up_latest``：只补最近一个错过的窗口。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from zoneinfo import ZoneInfo


class MissPolicy(str, Enum):
    SKIP = "skip"
    CATCH_UP = "catch_up"
    CATCH_UP_LATEST = "catch_up_latest"


def _parse_field(spec: str, low: int, high: int) -> frozenset[int]:
    values: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        step = 1
        if part == "*":
            values.update(range(low, high + 1))
            continue
        if "/" in part:
            base, step_text = part.split("/", 1)
            step = int(step_text)
            if step <= 0:
                raise ValueError(f"步长必须为正整数：{part}")
        else:
            base = part
        if base == "*" or base == "":
            start, end = low, high
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start, end = int(start_text), int(end_text)
        else:
            start = end = int(base)
        if not (low <= start <= high and low <= end <= high and start <= end):
            raise ValueError(f"cron 字段超出范围 [{low},{high}]：{part}")
        values.update(range(start, end + 1, step))
    return frozenset(values)


@dataclass(frozen=True)
class Schedule:
    """一个可复用的 cron 计划。

    :param cron: 五段 ``分 时 日 月 周``；周字段 0=周一。
    :param timezone: IANA 时区名（如 ``Asia/Shanghai``、``America/New_York``）。
    :param miss_policy: 错过触发的补跑策略。
    :param gap_skip: 春跳缺失时刻取跳变后（after）还是跳变前（before）。
    """

    cron: str
    timezone: str
    miss_policy: MissPolicy = MissPolicy.CATCH_UP_LATEST
    gap_skip: str = "after"

    def __post_init__(self) -> None:
        parts = self.cron.split()
        if len(parts) != 5:
            raise ValueError(f"cron 必须是五段式：{self.cron!r}")
        minute, hour, dom, month, dow = parts
        object.__setattr__(self, "_minute", _parse_field(minute, 0, 59))
        object.__setattr__(self, "_hour", _parse_field(hour, 0, 23))
        try:
            object.__setattr__(self, "_tz", ZoneInfo(self.timezone))
        except Exception as exc:  # pragma: no cover - 依赖 tzdata
            raise ValueError(f"未知时区：{self.timezone}") from exc
        object.__setattr__(self, "_dom", _parse_field(dom, 1, 31))
        object.__setattr__(self, "_month", _parse_field(month, 1, 12))
        if dow == "*":
            object.__setattr__(self, "_dow", None)
        else:
            object.__setattr__(self, "_dow", _parse_field(dow, 0, 6))
        if self.gap_skip not in ("after", "before"):
            raise ValueError("gap_skip 只能是 after 或 before")

    # ---- 内部：字段集合（__post_init__ 中通过 object.__setattr__ 注入）----
    @property
    def tz(self) -> ZoneInfo:
        return getattr(self, "_tz")

    def _matches_day(self, day: date) -> bool:
        if day.month not in getattr(self, "_month"):
            return False
        dom_ok = day.day in getattr(self, "_dom")
        dow = getattr(self, "_dow")
        dow_ok = True if dow is None else (day.weekday() in dow)
        # cron 语义：日与周都被限制时取"或"，任一字段为 * 时取"与"。
        dom_restricted = self.cron.split()[2] != "*"
        dow_restricted = self.cron.split()[4] != "*"
        if dom_restricted and dow_restricted:
            return dom_ok or dow_ok
        return dom_ok and dow_ok

    def _resolve_local(self, naive: datetime) -> datetime:
        """把本地朴素时间解释为时区时间，处理 gap 与 ambiguous。

        PEP 495 约定（fold=0 取跳变前偏移，fold=1 取跳变后偏移）：

        - 普通时刻：两个 fold 的 UTC 偏移相同。
        - 秋叠（ambiguous）：fold=0 的偏移更大（纽约 11 月，-4 > -5），
          固定取 fold=0 即第一次出现，保证同一窗口只生成一个实例。
        - 春跳（gap）：fold=0 的偏移更小（纽约 3 月，-5 < -4）。
          ``after`` 用 fold=0 偏移，绝对时刻落在跳变后（02:30 名义 → 03:30）；
          ``before`` 用 fold=1 偏移，绝对时刻提前到跳变前（02:30 名义 → 01:30）。
        """
        offset_before = naive.replace(tzinfo=self.tz, fold=0).utcoffset()
        offset_after = naive.replace(tzinfo=self.tz, fold=1).utcoffset()
        if offset_before == offset_after:
            return naive.replace(tzinfo=self.tz)  # 普通时刻
        if offset_after < offset_before:
            return naive.replace(tzinfo=self.tz, fold=0)  # 秋叠：取第一次
        # 春跳 gap
        fold = 0 if self.gap_skip == "after" else 1
        return naive.replace(tzinfo=self.tz, fold=fold)

    def next_after(self, after_utc: datetime) -> datetime:
        """返回严格晚于 ``after_utc`` 的下一次触发时间（UTC，带时区）。

        在任务时区的**本地墙上时间**上逐分钟扫描（朴素时间），这样春跳缺失的
        名义分钟也能被命中并交给 :meth:`_resolve_local` 按 gap 策略解析；
        每次只推进一个窗口，最多扫描 367 天以防错误表达式死循环。
        """
        if after_utc.tzinfo is None:
            raise ValueError("after_utc 必须带时区")
        local = after_utc.astimezone(self.tz)
        candidate = local.replace(tzinfo=None, second=0, microsecond=0) + timedelta(minutes=1)
        minute_set = getattr(self, "_minute")
        hour_set = getattr(self, "_hour")
        deadline = candidate + timedelta(days=367)
        while candidate < deadline:
            if (
                candidate.minute in minute_set
                and candidate.hour in hour_set
                and self._matches_day(candidate.date())
            ):
                resolved = self._resolve_local(candidate)
                if resolved > local:
                    return resolved.astimezone(ZoneInfo("UTC"))
            candidate += timedelta(minutes=1)
        raise RuntimeError(f"cron 在一年内没有触发：{self.cron}")

    def window_key(self, trigger_utc: datetime) -> str:
        """同一任务与计划窗口的稳定键（需求 P-08-01）。

        用触发时刻的 UTC ISO 串，保证秋叠的两次本地表示不会生成两个窗口。
        """
        return trigger_utc.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")

    def missed_windows(
        self, last_considered_utc: datetime, now_utc: datetime
    ) -> list[tuple[datetime, str]]:
        """枚举区间 ``(last_considered_utc, now_utc]`` 内全部触发窗口。

        按 :attr:`miss_policy` 收敛为应补跑的窗口（``skip`` 返回空，
        ``catch_up_latest`` 只留最后一个）。
        """
        windows: list[tuple[datetime, str]] = []
        cursor = last_considered_utc
        while True:
            nxt = self.next_after(cursor)
            if nxt > now_utc:
                break
            windows.append((nxt, self.window_key(nxt)))
            cursor = nxt
        if self.miss_policy is MissPolicy.SKIP:
            return []
        if self.miss_policy is MissPolicy.CATCH_UP_LATEST and len(windows) > 1:
            return windows[-1:]
        return windows
