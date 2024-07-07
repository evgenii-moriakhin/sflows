from __future__ import annotations

import inspect
import time
import uuid
from typing import TYPE_CHECKING, Any, Generic, Literal, Optional, TypeVar, Union

from pydantic import BaseModel, Field
from typing_extensions import Self, get_original_bases

if TYPE_CHECKING:
    from typing_extensions import Self


_T_request_payload = TypeVar("_T_request_payload")
_T_request_result = TypeVar("_T_request_result")


class AsyncRequest(BaseModel, Generic[_T_request_payload, _T_request_result]):
    """AsyncRequest."""

    correlation_id: str
    causation_id: str
    payload: _T_request_payload
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = Field(default_factory=time.time)
    headers: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def name(cls: type[Self]) -> str:
        return cls.__name__

    @classmethod
    def result_cls(cls: type[Self]) -> type[_T_request_result]:
        generic_args: list[type] = []

        def infer_generic_args(kls: type) -> None:
            if not inspect.isclass(kls) or not issubclass(kls, AsyncRequest):
                return
            generic_args.extend([a for a in kls.__pydantic_generic_metadata__["args"] if not isinstance(a, TypeVar)])
            for base in get_original_bases(kls):  # type: ignore
                if base is AsyncRequest:
                    return
                infer_generic_args(base)

        infer_generic_args(cls)
        return generic_args[1]


class RequestToPersist(BaseModel):
    name: str = Field(alias="function")
    request_id: str = Field(alias="key")
    correlation_id: str
    causation_id: str
    queue: str
    request: dict[str, Any]
    timeout: int
    heartbeat_timeout: int = Field(alias="heartbeat")
    retries: int
    status: str
    scheduled: int
    progress: float
    attempts: int
    completed: int
    queued: int
    started: int
    touched: int
    result: Any
    error: Union[str, None]
    stage: Optional[str]
    paused: bool
    need_compensate: bool = False
    compensation_status: Optional[Literal["started", "paused", "complete", "failed"]] = None
    compensation_error: Optional[str] = None
    stages: list[dict[str, Any]]
    heartbeats_history: list[dict[str, Any]]
