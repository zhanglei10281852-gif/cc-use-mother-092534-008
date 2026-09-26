"""能力票据调度服务。

对外入口：

- :class:`scheduler.service.SchedulerService`：调度、票据、租约、补偿与恢复的门面 API。
- :class:`scheduler.authz.AuthorizationService`：授权版本、租户状态与收缩判定。
- :class:`scheduler.calendar.Schedule`：时区感知的 cron 计划，含夏令时与错过触发策略。
"""
from __future__ import annotations

from scheduler.authz import AuthorizationService, CapabilityTicket
from scheduler.calendar import Schedule
from scheduler.clock import Clock, MockClock, SystemClock
from scheduler.service import SchedulerService

__all__ = [
    "AuthorizationService",
    "CapabilityTicket",
    "Clock",
    "MockClock",
    "Schedule",
    "SchedulerService",
    "SystemClock",
]
