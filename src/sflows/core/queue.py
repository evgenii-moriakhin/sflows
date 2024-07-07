# pyright: reportIncompatibleMethodOverride=false
# ruff: noqa: PLW2901
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

import saq
from typing_extensions import Self

from .task import AsyncTask

if TYPE_CHECKING:
    from collections.abc import Mapping

    from anyio.streams.memory import MemoryObjectSendStream
    from redis.asyncio import Redis


class TaskQueue(saq.Queue):
    def __init__(  # noqa: PLR0913
        self: Self,
        redis: Redis[bytes],
        name: str = "default",
        dump: Callable[[Mapping[Any, Any]], str] | None = None,
        load: Callable[[bytes | str], Any] | None = None,
        max_concurrent_ops: int = 20,
        persist_task_send_stream: MemoryObjectSendStream[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(redis, name, dump, load, max_concurrent_ops)
        self.persist_task_send_stream = persist_task_send_stream

    def serialize(self: Self, job: AsyncTask) -> str:
        job_dict = job.to_dict()
        if self.persist_task_send_stream:
            self.persist_task_send_stream.send_nowait(job_dict)
        return self._dump(job_dict)

    def deserialize(self: Self, job_bytes: bytes | None) -> AsyncTask | None:
        if not job_bytes:
            return None

        job_dict = self._load(job_bytes)
        if job_dict.pop("queue") != self.name:
            msg = f"Job {job_dict} fetched by wrong queue: {self.name}"
            raise ValueError(msg)
        return AsyncTask(**job_dict, queue=self)
