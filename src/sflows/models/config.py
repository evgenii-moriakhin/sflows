from __future__ import annotations

from collections.abc import Awaitable, Callable, Collection  # noqa: TCH003
from logging import Logger  # noqa: TCH003
from typing import TYPE_CHECKING, Any, Optional, TypeVar, Union

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Self

from ..types import PartialTimersDict  # noqa: TCH001, TID252
from .request import RequestToPersist  # noqa: TCH001

if TYPE_CHECKING:
    from typing_extensions import Self

    from ..request_handler import AsyncRequestHandler  # noqa: TID252
    from .request import AsyncRequest

_T_request_payload = TypeVar("_T_request_payload")
_T_request_result = TypeVar("_T_request_result")


class RequestHandlerConfig:
    def __init__(
        self: Self,
        request_cls: type[AsyncRequest[_T_request_payload, _T_request_result]],
        request_handler_cls: type[AsyncRequestHandler[_T_request_payload, _T_request_result]],
        dependencies: dict[str, Any] | None = None,
    ) -> None:
        self.request_cls = request_cls
        self.request_handler_cls = request_handler_cls
        self.dependencies = dependencies or {}


class AsyncTaskManagerConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    key: str
    redis_url: str
    queue_name: str
    queue_max_concurrent_opts: int = 20
    concurrency: int = 10
    timers: Optional[PartialTimersDict] = None
    dequeue_timeout: float = 0

    external_queues: Collection[str] = Field(default_factory=list)

    request_handlers_cfgs: Collection[RequestHandlerConfig] = Field(default_factory=list)
    dependencies: dict[str, Any] = Field(default_factory=dict)

    logger: Union[str, Logger] = "root"

    persist_request_callback: Optional[Callable[[dict[str, Any], RequestToPersist], Awaitable[None]]] = None
    persist_task_config: dict[str, Any] = Field(default_factory=dict)

    def model_post_init(self: Self, __context: Any) -> None:  # noqa: ANN401
        if self.queue_name in self.external_queues:
            msg = f"Queue name for worker {self.queue_name!r} must not be present in external queues"
            raise ValueError(msg)
        if len(self.external_queues) != len(set(self.external_queues)):
            msg = "There are duplicates in the names of external queues"
            raise ValueError(msg)
