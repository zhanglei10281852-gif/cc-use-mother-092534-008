"""后台任务能力票据调度服务。"""
from __future__ import annotations

from .errors import (
    CompensationExhausted,
    LeaseLost,
    SchedulerError,
    TicketInvalid,
)
from .service import SchedulerService
from .timeeval import CronSpec, resolve_local
from .models import (
    Authorization,
    Capability,
    GapPolicy,
    JobSpec,
    LeaseGrant,
    LeaseToken,
    MisfirePolicy,
    OverlapPolicy,
)

__all__ = [
    "SchedulerService",
    "Authorization",
    "Capability",
    "JobSpec",
    "LeaseGrant",
    "LeaseToken",
    "CronSpec",
    "resolve_local",
    "GapPolicy",
    "MisfirePolicy",
    "OverlapPolicy",
    "SchedulerError",
    "LeaseLost",
    "TicketInvalid",
    "CompensationExhausted",
]
