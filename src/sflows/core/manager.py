from __future__ import annotations

import inspect
import logging
import math
import time
from asyncio import exceptions as asyncio_exceptions
from collections.abc import AsyncGenerator, Awaitable, Callable  # noqa: TCH003
from contextlib import asynccontextmanager
from functools import partial
from typing import TYPE_CHECKING, Any, TypeVar, cast

import anyio
import saq.queue
import saq.worker
from pydantic import TypeAdapter
from saq import Status
from saq.job import TERMINAL_STATUSES, UNSUCCESSFUL_TERMINAL_STATUSES
from saq.queue import PubSubMultiplexer
from typing_extensions import Self

from ..core.worker import Worker  # noqa: TID252
from ..exceptions import (  # noqa: TID252
    AbortedError,
    CompensatedError,
    CompensatedSuccess,
    PausedError,
    UnprocessedError,
)
from ..helpers import filter_dict_by_keys  # noqa: TID252
from ..models.request import AsyncRequest, RequestToPersist  # noqa: TID252
from ..request_handler import AttemptInfo, TaskExecutionContext, stages_stack_ctx  # noqa: TID252
from .queue import TaskQueue
from .task import AsyncTask, Heartbeat, Stage, result_cls_from_encoded_string, stringify_result_cls

if TYPE_CHECKING:
    from typing_extensions import Self

    from ..models.config import AsyncTaskManagerConfig  # noqa: TID252
    from ..request_handler import AsyncRequestHandler  # noqa: TID252
    from ..types import Context as JobContext  # noqa: TID252
    from ..types import CountKind, QueueInfo, QueueStats  # noqa: TID252

_T_request_payload = TypeVar("_T_request_payload")
_T_request_result = TypeVar("_T_request_result")


class AsyncTaskManager:
    def __init__(self: Self, config: AsyncTaskManagerConfig) -> None:
        """Init."""
        self._config = config
        if isinstance(self._config.logger, str):
            self.logger = logging.getLogger(self._config.logger)
        else:
            self.logger = self._config.logger
        saq.worker.logger = self.logger
        saq.queue.logger = self.logger
        self._persist_task_callback = self._config.persist_request_callback
        self.persist_task_send_stream, self.persist_task_receive_stream = anyio.create_memory_object_stream[
            dict[str, Any]
        ](math.inf)
        self._queue = self._setup_queue()
        self.pubsub = PubSubMultiplexer(self._queue.redis.pubsub(), "")  # type: ignore
        self._external_queues = {
            ext_queue_name: TaskQueue.from_url(self._config.redis_url, name=ext_queue_name)
            for ext_queue_name in self._config.external_queues
        }
        self._all_queues = {self._queue.name: self._queue, **self._external_queues}
        self._config = config
        self.handlers_fabrics_by_request_name: dict[
            str, tuple[Callable[..., AsyncRequestHandler[Any, Any]], type[AsyncRequest[Any, Any]]]
        ] = {}

    def set_persist_request_callback(
        self: Self, persist_task_callback: Callable[[dict[str, Any], RequestToPersist], Awaitable[None]]
    ) -> None:
        self._persist_task_callback = persist_task_callback

    def update_injected_dependencies(self: Self, **dependencies: dict[str, Any]) -> None:
        self._config.dependencies.update(**dependencies)

    def _setup_queue(self: Self) -> TaskQueue:
        return TaskQueue.from_url(
            self._config.redis_url,
            name=self._config.queue_name,
            max_concurrent_ops=self._config.queue_max_concurrent_opts,
            persist_task_send_stream=self.persist_task_send_stream,
        )

    def _setup_worker(self: Self) -> Worker:
        queue = self._queue

        # Assign a human-readable ID to the queue.
        # SAQ workers update statistics in the queue using this ID.
        # This ID is considered the ID of the worker processing the queue.
        #
        # It's okay to modify the Queue object when a worker starts processing it.
        # The queue's UUID is only needed for tracking queue statistics if workers are processing it.

        queue.uuid = f"{self._config.key}_{queue.uuid}"
        functions: list[tuple[str, Callable[..., Awaitable[Any]]]] = []
        for handler_config in self._config.request_handlers_cfgs:
            request_cls = handler_config.request_cls
            request_handler_cls = handler_config.request_handler_cls
            request_handler_init_arg_names = inspect.signature(request_handler_cls).parameters
            request_handler_init_args = {
                **filter_dict_by_keys(self._config.dependencies, request_handler_init_arg_names),
                **filter_dict_by_keys(handler_config.dependencies, request_handler_init_arg_names),
            }
            self.handlers_fabrics_by_request_name[request_cls.name()] = (
                partial(request_handler_cls, request_handler_init_args),
                request_cls,
            )
            functions.append(
                (
                    request_cls.name(),
                    partial(_worker_request_handler, request_cls, request_handler_cls, request_handler_init_args, self),
                )
            )
        return Worker(
            queue,
            functions=[*functions],
            concurrency=self._config.concurrency,
            timers=self._config.timers,
            dequeue_timeout=self._config.dequeue_timeout,
            after_process=partial(_after_process_task, self),
        )

    async def send_request(  # noqa: PLR0913
        self: Self,
        request: AsyncRequest[_T_request_payload, _T_request_result],
        queue: str,
        *,
        timeout: int = 10,
        heartbeat: int = 0,
        retries: int = 1,
        ttl: int = 600,
        retry_delay: float = 0.0,
        retry_backoff: bool | float = False,
    ) -> AsyncTask:
        target_queue = self._all_queues[queue]
        task_name = request.name()
        stage = None
        stage_name = None
        stages_stack = stages_stack_ctx.get()
        parent_attempt = None
        if stages_stack:
            stage = stages_stack[-1]
            stage_name = stage["name"]
            parent_attempt = stage["attempt"]
        task = AsyncTask(
            task_name,
            queue=target_queue,
            kwargs={"request": request.model_dump()},
            key=request.request_id,
            correlation_id=request.correlation_id,
            causation_id=request.causation_id,
            timeout=timeout,
            heartbeat=heartbeat,
            retries=retries,
            ttl=ttl,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
            stage=stage_name,
            parent_attempt=parent_attempt,
            result_cls_encoded=stringify_result_cls(request.result_cls()),
        )
        if stage:
            stage["task"] = task
        await task.enqueue()
        return task

    async def refresh_task(self: Self, task: AsyncTask, until_complete: float | None = None) -> None:
        await task.refresh(until_complete)
        self._deserialize_stages_data(task.stages)

    async def try_pause_task(self: Self, task: AsyncTask) -> None:
        await task.queue.redis.publish(task.key.encode(), "pause")

    async def try_pause_task_by_request_id(self: Self, request_id: str, queue: str) -> None:
        target_queue = self._all_queues[queue]
        task = await target_queue.job(request_id)
        if not task:
            msg = "No such task"
            raise ValueError(msg)
        await self.try_pause_task(cast(AsyncTask, task))

    async def resume_task(self: Self, task: AsyncTask) -> tuple[bool, str]:
        lock_key = f"task_resume_lock:{task.id}"
        async with task.queue.redis.lock(lock_key, timeout=5):
            await self.refresh_task(task)
            if task.status in [Status.ACTIVE, Status.QUEUED]:
                msg = f"Cannot retry task {task.id} because it in status {task.status}"
                self.logger.warning(msg)
                return False, msg
            if task.paused is not True:
                msg = f"Cannot retry task {task.id} because it is not paused"
                self.logger.warning(msg)
                return False, msg

            async def retry_callback(_id: str, status: Status) -> bool:
                return status == Status.QUEUED

            # we wait for a callback indicating that the job has changed its status to Queued.
            # Consequently, all parallel attempts to compensate will not retry due to the status,
            # and the lock can be safely released
            async with anyio.create_task_group() as tg:
                tg.start_soon(task.queue.listen, [task.key], retry_callback, None)
                await anyio.sleep(0.01)
                await task.retry("resume from pause")
            return True, f"Job {task.id} resumed"

    async def resume_task_by_request_id(self: Self, request_id: str, queue: str) -> tuple[bool, str]:
        target_queue = self._all_queues[queue]
        task = await target_queue.job(request_id)
        if not task:
            msg = "No such task"
            raise ValueError(msg)
        return await self.resume_task(cast(AsyncTask, task))

    async def compensate_task(self: Self, task: AsyncTask, *, force_need_compensate: bool = True) -> tuple[bool, str]:
        lock_key = f"task_compensate_lock:{task.id}"
        async with task.queue.redis.lock(lock_key, timeout=5):
            await self.refresh_task(task)
            if task.status not in TERMINAL_STATUSES:
                msg = f"Cannot compensate task {task.id} because it in status {task.status}"
                self.logger.warning(msg)
                return False, msg
            if task.compensation_status == "started":
                msg = f"Cannot compensate task {task.id} because compensation already started"
                self.logger.warning(msg)
                return False, msg
            if task.compensation_status == "complete":
                msg = f"Cannot compensate task {task.id} because it already compensated"
                self.logger.warning(msg)
                return False, msg
            if task.compensation_status == "paused":
                msg = f"Cannot compensate task {task.id} because compensation already paused. Resume task"
                self.logger.warning(msg)
                return False, msg
            if force_need_compensate:
                task.need_compensate = True
                await task.update()
            if not task.need_compensate:
                msg = f"Cannot compensate task {task.id} because compensate event is not set"
                self.logger.warning(msg)
                return False, msg

            async def retry_callback(_id: str, status: Status) -> bool:
                return status == Status.QUEUED

            # we wait for a callback indicating that the job has changed its status to Queued.
            # Consequently, all parallel attempts to compensate will not retry due to the status,
            # and the lock can be safely released
            async with anyio.create_task_group() as tg:
                tg.start_soon(task.queue.listen, [task.key], retry_callback, None)
                await anyio.sleep(0.01)
                await task.retry(task.error)
            return True, f"Job {task.id} compensation starts"

    async def startup(
        self: Self,
    ) -> None:
        self._worker: Worker = self._setup_worker()
        async with anyio.create_task_group() as startup_task_group:
            startup_task_group.start_soon(self.pubsub.start)
            startup_task_group.start_soon(self._persist_task)
            startup_task_group.start_soon(self._worker.start)

    async def _persist_task(self: Self) -> None:
        async for task_dict in self.persist_task_receive_stream:
            if self._persist_task_callback:
                try:
                    await self._persist_task_callback(
                        self._config.persist_task_config,
                        RequestToPersist.model_validate({**task_dict, "request": task_dict["kwargs"]["request"]}),
                    )
                except Exception as error:
                    print(error)

    async def shutdown(self: Self) -> None:
        # stop the worker first to correctly cancel and finish all tasks
        await self._worker.stop()
        async with anyio.create_task_group() as shutdown_task_group:
            for queue in self._all_queues.values():
                shutdown_task_group.start_soon(queue.disconnect)
            shutdown_task_group.start_soon(self.pubsub.close)
        self.persist_task_send_stream.close()
        self.persist_task_receive_stream.close()

    async def await_task_completion(self: Self, task: AsyncTask, timeout: int = 0) -> None:
        try:
            with anyio.fail_after(timeout if timeout else None):
                if not task.completed or (task.need_compensate and task.compensation_status != "paused"):
                    while True:
                        try:
                            if task.completed and (
                                (task.need_compensate and task.compensation_status == "paused")
                                or not task.need_compensate
                            ):
                                break
                            await self.refresh_task(task, 1)
                        except asyncio_exceptions.TimeoutError:
                            continue
        except TimeoutError as error:
            msg = f"Wait for async request result of request id {task.key!r} timeout after {timeout} sec"
            raise TimeoutError(msg) from error

    async def await_task_completion_and_get_result(self: Self, task: AsyncTask, timeout: int = 0) -> Any:  # noqa: ANN401
        await self.await_task_completion(task, timeout)
        if task.status == Status.ABORTED:
            if task.paused:
                raise PausedError(task.error or "paused by unknown reason")
            raise AbortedError(task.error or "unknown aborted error")
        if task.compensation_status == "complete":
            raise CompensatedSuccess(task.error or "compensate error was success")
        if task.compensation_status == "paused":
            raise PausedError(task.compensation_error or "compensation paused by unknown reason")
        if task.compensation_status == "failed":
            raise CompensatedError(task.compensation_error or "unknown compensated error")
        if task.status in UNSUCCESSFUL_TERMINAL_STATUSES:
            raise UnprocessedError(task.error or "unknown unprocessed error")
        return task.result

    async def await_task_completion_and_get_validated_result(self: Self, task: AsyncTask, timeout: int = 0) -> Any:  # noqa: ANN401
        result_cls = result_cls_from_encoded_string(task.result_cls_encoded)
        return TypeAdapter(result_cls).validate_python(await self.await_task_completion_and_get_result(task, timeout))

    async def queue_info(
        self: Self, name: str | None = None, *, tasks: bool = False, offset: int = 0, limit: int = 10
    ) -> QueueInfo:
        queue_name = self._config.queue_name if name is None else name
        return await self._all_queues[queue_name].info(jobs=tasks, offset=offset, limit=limit)

    async def queue_stats(self: Self, name: str | None = None, *, ttl: int = 60) -> QueueStats:
        queue_name = self._config.queue_name if name is None else name
        return await self._all_queues[queue_name].stats(ttl=ttl)

    async def queue_count(self: Self, name: str | None = None, *, kind: CountKind) -> int:
        queue_name = self._config.queue_name if name is None else name
        return await self._all_queues[queue_name].count(kind=kind)

    def _deserialize_stages_data(self: Self, stages: list[Stage]) -> None:
        for stage in stages:
            if stage["task"]:
                stage["task"]["queue"] = self._all_queues.get(stage["task"]["queue"])  # type: ignore
                self._deserialize_stages_data(stage["task"]["stages"])  # type: ignore AsyncTask is dict here
                stage["task"] = TypeAdapter(AsyncTask).validate_python(stage["task"])

    @asynccontextmanager
    async def task_execution_context(
        self: Self, task: AsyncTask, request: AsyncRequest[_T_request_payload, _T_request_result]
    ) -> AsyncGenerator[TaskExecutionContext[_T_request_payload, _T_request_result], Any]:
        signals = await self.pubsub.subscribe(request.request_id)  # type: ignore

        await self.refresh_task(task)

        current_attempt = task.attempts
        attempts_info: dict[int, AttemptInfo] = {}
        for attempt_num in range(1, current_attempt + 1):
            attempt_stages = {s["name"]: s for s in task.stages if s["attempt"] == attempt_num}
            attempts_info[attempt_num] = AttemptInfo(attempt_stages=attempt_stages)
        task_execution_context = TaskExecutionContext(request=request, task=task, attempts_info=attempts_info)  # type: ignore
        stop_receiver = False

        async def receive_pause_events() -> None:
            nonlocal stop_receiver
            while True:
                while not signals.empty():
                    signal: dict[str, Any] = signals.get_nowait()
                    if signal and signal["data"] == b"pause":
                        task_execution_context.set_pause_event()
                        for stage in task_execution_context.latest_attempt_info.attempt_stages.values():
                            if stage["task"]:
                                # attempt to pause child tasks when receiving a pause event
                                # if some of them are paused, the result will be processed by the corresponding stages
                                with anyio.CancelScope(shield=True):
                                    await self.try_pause_task(stage["task"])
                        stop_receiver = True
                if stop_receiver:
                    break
                await anyio.sleep(0.1)

        task_processing_error = None
        async with anyio.create_task_group() as tg:
            tg.start_soon(receive_pause_events)
            try:
                yield task_execution_context
            except BaseException as error:  # noqa: BLE001
                task_processing_error = error
            finally:
                with anyio.CancelScope(shield=True):
                    stop_receiver = True
                    task_execution_context.set_pause_event(pause_event=False)
                    await task_execution_context.update_task()
                    await self.pubsub.unsubscribe(signals)  # type: ignore
        if task_processing_error:
            raise task_processing_error


async def _worker_request_handler(  # noqa: PLR0913
    request_cls: type[AsyncRequest[_T_request_payload, _T_request_result]],
    request_handler_cls: type[AsyncRequestHandler[_T_request_payload, _T_request_result]],
    request_handler_init_args: dict[str, Any],
    task_manager: AsyncTaskManager,
    ctx: JobContext,
    *,
    request: dict[str, Any],
) -> _T_request_result | None:
    request_handler = request_handler_cls(**request_handler_init_args)
    request_handler.task_manager = task_manager
    request_handler.logger = task_manager.logger
    request_model = request_cls.model_validate(request)

    result = None
    processor_error = None

    async def processor():
        nonlocal result
        nonlocal processor_error
        try:
            async with task_manager.task_execution_context(
                cast(AsyncTask, ctx["job"]), request_model
            ) as request_context:  # type: ignore
                request_handler.request_context = request_context
                if not request_context.task.need_compensate:
                    result = await process_request(request_handler)
                else:
                    await process_compensate(request_handler)
        except BaseException as error:
            processor_error = error

    async with anyio.create_task_group() as tg:
        # start processing the request in anyio.task_group so that anyio.CancelScope shielded scopes work within it
        tg.start_soon(processor)
    if processor_error:
        raise processor_error
    return result


async def process_request(
    request_handler: AsyncRequestHandler[_T_request_payload, _T_request_result],
) -> _T_request_result:
    try:
        result = await request_handler.process_request()
    except anyio.get_cancelled_exc_class():
        with anyio.CancelScope(shield=True):
            await request_handler.on_request_abort()
        raise
    except Exception as error:
        with anyio.CancelScope(shield=True):
            processed_error = await request_handler.on_request_error(error)
        raise processed_error from error
    finally:
        with anyio.CancelScope(shield=True):
            await request_handler.request_context.update_task()

    return result


async def process_compensate(
    request_handler: AsyncRequestHandler[_T_request_payload, _T_request_result],
) -> None:
    try:
        await request_handler.process_compensate()
        request_handler.request_context.task.need_compensate = False
        request_handler.request_context.task.compensation_status = "complete"
    except anyio.get_cancelled_exc_class():
        with anyio.CancelScope(shield=True):
            request_handler.request_context.task.need_compensate = False
            request_handler.request_context.task.compensation_status = "failed"
            request_handler.request_context.task.compensation_error = "cancelled"
            await request_handler.on_compensate_abort()
        raise
    except Exception as error:
        with anyio.CancelScope(shield=True):
            if isinstance(error, PausedError):
                request_handler.request_context.task.need_compensate = True
                request_handler.request_context.task.compensation_status = "paused"
                request_handler.request_context.task.compensation_error = str(error)
            else:
                request_handler.request_context.task.need_compensate = False
                request_handler.request_context.task.compensation_status = "failed"
                request_handler.request_context.task.compensation_error = str(error)
            processed_error = await request_handler.on_compensate_error(error)
        raise processed_error from error
    else:
        raise CompensatedSuccess(request_handler.request_context.task.error)
    finally:
        with anyio.CancelScope(shield=True):
            await request_handler.request_context.update_task()


async def _after_process_task(task_manager: AsyncTaskManager, ctx: JobContext | None) -> None:
    if ctx is None:
        return

    async_task = cast(AsyncTask, ctx["job"])  # type: ignore

    if async_task.status in UNSUCCESSFUL_TERMINAL_STATUSES and async_task.compensation_status == "failed":
        # If the compensation handler has exhausted all its retries before the unsuccessful completion of the task,
        # (UNSUCCESSFUL_TERMINAL_STATUSES) it means an error occurred during compensation.
        async_task.heartbeats_history.append(
            Heartbeat(attempt="compensate_attempt", timestamp=int(time.time()), message="Compensation failed")
        )
        await async_task.update()

    # try compensate
    # these checks are also performed inside compensate_task.
    # But we perform them here to reduce the number of operations and not use the compensate lock
    if (
        async_task.status in UNSUCCESSFUL_TERMINAL_STATUSES
        and async_task.need_compensate
        and async_task.compensation_status not in ["started", "complete", "paused"]
    ):
        await task_manager.compensate_task(async_task, force_need_compensate=False)
