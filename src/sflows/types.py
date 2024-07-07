from saq.types import Context, CountKind, QueueInfo, QueueStats
from typing_extensions import TypedDict


# for pydantic models compability
# >> Please use `typing_extensions.TypedDict` instead of `typing.TypedDict` on Python < 3.12
class TimersDict(TypedDict):
    """Timers Dictionary"""

    schedule: int
    "How often we poll to schedule jobs in seconds (default 1)"
    stats: int
    "How often to update stats in seconds (default 10)"
    sweep: int
    "How often to clean up stuck jobs in seconds (default 60)"
    abort: int
    "How often to check if a job is aborted in seconds (default 1)"


class PartialTimersDict(TimersDict, total=False):
    """For argument to `Worker`, all keys are not required"""


__all__ = ["Context", "PartialTimersDict", "QueueInfo", "QueueStats", "CountKind"]
