from __future__ import annotations

import base64
import dataclasses
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

import dill  # type: ignore
from pydantic import ConfigDict
from saq import Job, Status
from saq.job import get_default_job_key
from typing_extensions import Self, TypedDict

if TYPE_CHECKING:
    from .queue import TaskQueue


class Heartbeat(TypedDict):
    attempt: Union[int, Literal["compensate_attempt"]]
    timestamp: int
    message: str


class Stage(TypedDict):
    name: str
    attempt: Union[int, Literal["compensate_attempt"]]
    status: Literal["started", "resumed", "failed", "completed", "aborted", "paused"]
    result: Any
    error: Union[str, None]
    skipped_due_previous_attempt_success: bool
    task: Optional[AsyncTask]
    previous_states: list[dict[str, Any]]


def stage_to_dict(s: Stage) -> dict[str, Any]:
    s_copy = dict(**s)
    if s_copy["task"] and not isinstance(s_copy["task"], dict):
        s_copy["task"] = s_copy["task"].to_dict()
    previous_states = []
    for previous_state in s["previous_states"]:
        previous_states.append(stage_to_dict(previous_state))
    s_copy["previous_states"] = previous_states
    return s_copy


def stringify_result_cls(t: type) -> str:
    return base64.b64encode(dill.dumps(t)).decode("ascii")  # type: ignore


def result_cls_from_encoded_string(s: str) -> Any:  # noqa: ANN401
    return dill.loads(base64.b64decode(s))  # type: ignore # noqa: S301


@dataclasses.dataclass
class AsyncTask(Job):
    __pydantic_config__ = ConfigDict(arbitrary_types_allowed=True)

    function: str
    kwargs: Optional[dict[str, Any]] = None
    queue: Optional[TaskQueue] = None  # type: ignore
    key: str = dataclasses.field(default_factory=get_default_job_key)
    timeout: int = 10
    heartbeat: int = 0
    retries: int = 1
    ttl: int = 600
    retry_delay: float = 0.0
    retry_backoff: Union[bool, float] = False
    scheduled: int = 0
    progress: float = 0.0
    attempts: int = 0
    completed: int = 0
    queued: int = 0
    started: int = 0
    touched: int = 0
    result: Any = None
    error: Union[str, None] = None
    status: Status = Status.NEW
    meta: dict[Any, Any] = dataclasses.field(default_factory=dict)

    stage: Optional[str] = None
    parent_attempt: Optional[Union[int, Literal["compensate_attempt"]]] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    stages: list[Stage] = dataclasses.field(default_factory=list)
    paused: bool = False
    pause_event_set: bool = False

    need_compensate: bool = False
    compensation_status: Optional[Literal["started", "paused", "complete", "failed"]] = None
    compensation_error: Optional[str] = None

    heartbeats_history: list[Heartbeat] = dataclasses.field(default_factory=list)
    result_cls_encoded: str = dataclasses.field(default_factory=lambda: stringify_result_cls(type(None)))

    _EXCLUDE_NON_FULL: ClassVar[set[str]] = {
        "kwargs",
        "scheduled",
        "progress",
        "total_ms",
        "result",
        "error",
        "status",
        "meta",
        "heartbeats_history",
    }

    def info(self: Self, full: bool = False) -> str:  # noqa: FBT001, FBT002
        """String with AsyncTask info

        Args:
            full: If true, will list the full kwargs for the AsyncTask, else an abridged version.
        """
        # Using an exclusion list preserves order for kwargs below
        excluded = set[str]() if full else self._EXCLUDE_NON_FULL
        kwargs = ", ".join(
            f"{k}={v}"
            for k, v in {
                "function": self.function,
                "kwargs": self.kwargs,
                "queue": self.get_queue().name,
                "id": self.id,
                "scheduled": self.scheduled,
                "progress": self.progress,
                "process_ms": self.duration("process"),
                "start_ms": self.duration("start"),
                "total_ms": self.duration("total"),
                "attempts": self.attempts,
                "result": self.result,
                "error": self.error,
                "status": self.status,
                "meta": self.meta,
                "correlation_id": self.correlation_id,
                "causation_id": self.causation_id,
                "heartbeats_history": self.heartbeats_history,
            }.items()
            if v is not None and k not in excluded
        )
        return f"AsyncTask<{kwargs}>"

    def __getstate__(self: Self) -> dict[str, Any]:
        return self.to_dict()

    def to_dict(self: Self) -> dict[str, Any]:
        """Serialises the AsyncTask to dict"""
        result: dict[str, Any] = {}
        for field in dataclasses.fields(self):
            key = field.name
            value = getattr(self, key)
            if key == "meta" and not value:
                continue
            if key == "queue" and value:
                value = value.name
            if key == "stages" and value:
                stages: list[dict[str, Any]] = [stage_to_dict(s) for s in value]
                value = stages
            result[key] = value
        return result

    def __hash__(self: Self) -> int:
        return hash(self.key)
