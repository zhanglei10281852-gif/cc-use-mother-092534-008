"""调度服务的领域异常。"""
from __future__ import annotations


class SchedulerError(Exception):
    """所有调度服务错误的基类。"""


class LeaseLost(SchedulerError):
    """租约世代号已失效：执行者已失去该实例，必须停止一切外部提交。"""


class TicketInvalid(SchedulerError):
    """票据在领取复核时未通过，实例已被拒绝执行。"""


class CompensationExhausted(SchedulerError):
    """补偿重试次数耗尽，实例进入死信。"""
