from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Awaitable, Callable  # noqa: TCH003
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar, copy_context
from copy import deepcopy
from dataclasses import dataclass
from traceback import format_exception
from typing import TYPE_CHECKING, Any, Generic, Literal, NoReturn, TypeVar

import anyio
from exceptiongroup import ExceptionGroup, catch
from pydantic import ValidationError
from typing_extensions import Concatenate, ParamSpec, Self

from .core.task import AsyncTask, Heartbeat, Stage, stage_to_dict
from .exceptions import AbortedError, CompensatedError, CompensatedSuccess, PausedError, UnprocessedError

if TYPE_CHECKING:
    from logging import Logger
    from types import TracebackType

    from anyio.abc import TaskGroup
    from typing_extensions import Self

    from .core.manager import AsyncTaskManager
    from .models.request import AsyncRequest

stages_stack_ctx: ContextVar[list[Stage] | None] = ContextVar("stages_stack_ctx", default=None)

_T_request_payload = TypeVar("_T_request_payload")
_T_request_result = TypeVar("_T_request_result")
_T_request_stage_payload = TypeVar("_T_request_stage_payload")
_T_request_stage_result = TypeVar("_T_request_stage_result")

T = TypeVar("T")
P = ParamSpec("P")


@dataclass
class AttemptInfo:
    attempt_stages: dict[str, Stage]


@dataclass
class TaskExecutionContext(Generic[_T_request_payload, _T_request_result]):
    request: AsyncRequest[_T_request_payload, _T_request_result]
    task: AsyncTask
    attempts_info: dict[int, AttemptInfo]

    @property
    def current_attempt(self: Self) -> int | Literal["compensate_attempt"]:
        return "compensate_attempt" if self.task.compensation_status == "started" else self.task.attempts

    @property
    def request_id(self: Self) -> str:
        return self.request.request_id

    @property
    def correlation_id(self: Self) -> str:
        return self.request.correlation_id

    @property
    def causation_id(self: Self) -> str:
        return self.request.causation_id

    @property
    def payload(self: Self) -> _T_request_payload:
        return self.request.payload

    @property
    def latest_attempt_info(self: Self) -> AttemptInfo:
        return self.attempts_info[self.task.attempts]

    @property
    def prelatest_attempt_info(self: Self) -> AttemptInfo | None:
        if self.task.attempts > 1:
            return self.attempts_info[self.task.attempts - 1]
        return None

    async def update_task(self: Self) -> None:
        await self.task.update()

    @property
    def is_pause_event_set(self: Self) -> bool:
        return self.task.pause_event_set

    def set_pause_event(self: Self, *, pause_event: bool = True) -> None:
        self.task.pause_event_set = pause_event

    def set_need_compensate(self: Self) -> None:
        self.task.need_compensate = True

    @property
    def is_last_attempt(self: Self) -> bool:
        return self.current_attempt == "compensate_attempt" or self.current_attempt >= self.task.retries

    async def heartbeat(self: Self, message: str) -> None:
        self.add_heartbeat(message)
        await self.update_task()

    def add_heartbeat(self: Self, message: str) -> None:
        self.task.heartbeats_history.append(
            Heartbeat(attempt=self.current_attempt, timestamp=int(time.time()), message=message)
        )

    async def check_pause(self: Self, pause_error_desc: str = "Pause event was set") -> None:
        if self.is_pause_event_set:
            self.add_heartbeat("Paused")
            await self.update_task()
            raise PausedError(pause_error_desc)


class StageManager(AbstractAsyncContextManager):  # type: ignore
    def __init__(
        self: Self, stage_name: str, request_handler: AsyncRequestHandler[_T_request_payload, _T_request_result]
    ) -> None:
        self.request_handler = request_handler
        self.request_context = request_handler.request_context
        self.stage_name = stage_name

    async def __aenter__(self: Self) -> Stage:
        with anyio.CancelScope(shield=True):
            return await copy_context().run(self._start_stage)

    async def _start_stage(self: Self) -> Stage:
        current_stage_prefix = ""
        sstack = stages_stack_ctx.get()
        if sstack:
            self.stages_stack = sstack
            current_stage_prefix = self.stages_stack[-1]["name"] + "."
        else:
            self.stages_stack: list[Stage] = []
            stages_stack_ctx.set(self.stages_stack)

        await self.request_context.check_pause(
            f"Before execute stage {current_stage_prefix}{self.stage_name} the pause check is successful"
        )

        if f"{current_stage_prefix}{self.stage_name}" in self.request_context.latest_attempt_info.attempt_stages:
            self.current_stage = self.request_context.latest_attempt_info.attempt_stages[
                f"{current_stage_prefix}{self.stage_name}"
            ]
            if self.current_stage["status"] == "paused" and self.current_stage["task"]:
                self.current_stage["previous_states"].append(stage_to_dict(self.current_stage))
                task = self.current_stage["task"]
                # The job may not be in a paused state if someone has manually resumed it.
                # Only in this case do we resume it.
                resumed, resumed_msg = await self.request_handler.task_manager.resume_task(task)
                if resumed:
                    self.request_handler.logger.warning(
                        "While start previously paused stage %(stage)s in %(handler)r handler for request %(request)s "
                        "according stage's task not resumed, by reason: %(reason)s",
                        {
                            "stage": self.current_stage,
                            "handler": self.request_handler,
                            "request": self.request_context.request.name(),
                            "reason": resumed_msg,
                        },
                    )
                else:
                    self.request_handler.logger.info(
                        "Successfully resume task for previously paused stage %(stage)s in %(handler)r handler "
                        "for request %(request)s",
                        {
                            "stage": self.current_stage,
                            "handler": self.request_handler,
                            "request": self.request_context.request.name(),
                        },
                    )
                self.current_stage["error"] = None
                self.current_stage["status"] = "resumed"
                # Non-job stages can be paused only if there were other stages inside the stage
                # that raised a PausedError. But in this case, we do the same thing as in the completed branch below
            elif self.current_stage["status"] != "completed":
                # if the stage was already in the current attempt, it's possible that its status != "completed"
                # this means it didn't complete successfully in the previous attempt,
                # and for history, it needs to be saved in the previous stage states for this attempt
                self.current_stage["previous_states"].append(stage_to_dict(self.current_stage))
                self.current_stage["error"] = None
                self.current_stage["status"] = "started"
                self.current_stage["task"] = None
        else:
            self.current_stage = Stage(
                name=f"{current_stage_prefix}{self.stage_name}",
                attempt=self.request_context.current_attempt,
                status="started",
                result=None,
                error=None,
                skipped_due_previous_attempt_success=False,
                task=None,
                previous_states=[],
            )
            self.request_context.task.stages.append(self.current_stage)
            self.request_context.latest_attempt_info.attempt_stages[f"{current_stage_prefix}{self.stage_name}"] = (
                self.current_stage
            )
        await self.request_context.update_task()
        self.stages_stack.append(self.current_stage)
        return self.current_stage

    async def __aexit__(  # noqa: C901, PLR0915
        self: Self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> None:
        with anyio.CancelScope(shield=True):
            if isinstance(__exc_value, anyio.get_cancelled_exc_class()):
                self.current_stage["status"] = "aborted"
                self.current_stage["error"] = "Stage aborted by cancellation"
                if self.current_stage["task"]:
                    task = self.current_stage["task"]
                    if self.request_context.is_pause_event_set:
                        # if a cancellation error occurs in some stage, and pause_event_set is set,
                        # it is assumed that this pause event caused the cancellation
                        # so we pause the child task
                        await self.request_handler.task_manager.try_pause_task(task)
                    else:
                        await task.abort("aborted by parent abort")
                    # although the child job should be paused/cancelled, we don't know exactly what's happening with it
                    # it may have already completed its work. Therefore, we check all possibilities
                    try:
                        result = await self.request_handler.task_manager.await_task_completion_and_get_validated_result(
                            task
                        )
                        self.current_stage["result"] = result
                        self.current_stage["status"] = "completed"
                        self.current_stage["error"] = None
                    except PausedError:
                        self.current_stage["status"] = "paused"
                        self.current_stage["error"] = (
                            f"Stage {self.current_stage['name']} paused by pause in task with id {task.key}: {task.error}"
                        )
                    except AbortedError:
                        self.current_stage["status"] = "aborted"
                        self.current_stage["error"] = (
                            f"Stage {self.current_stage['name']} aborted by abort in task with id {task.key}: {task.error}"
                        )
                    except CompensatedSuccess:
                        self.current_stage["status"] = "failed"
                        self.current_stage["error"] = (
                            f"Stage {self.current_stage['name']} failed (and sucessfully compensated) by fail in task with id {task.key}: {task.error}"
                        )
                    except CompensatedError as error:
                        # if a compensation error occurred for some task,
                        # we pause the current task to investigate manually
                        self.current_stage["status"] = "failed"
                        self.current_stage["error"] = (
                            f"Stage {self.current_stage['name']} failed (and error occurs while compensated) by fail in task with id {task.key}: {task.error}"
                        )
                        self.stages_stack.pop()
                        await self.request_context.update_task()
                        raise PausedError(self.current_stage["error"]) from error
                    except (UnprocessedError, ValidationError):
                        self.current_stage["status"] = "failed"
                        self.current_stage["error"] = f"Stage failed by fail in task with id {task.key}: {task.error}"
                self.stages_stack.pop()
                await self.request_context.update_task()
                raise
            formatted_exc = format_exception(__exc_type, __exc_value, __traceback)
            if isinstance(__exc_value, PausedError):
                self.current_stage["status"] = "paused"
                if self.current_stage["task"]:
                    task = self.current_stage["task"]
                    self.current_stage["error"] = (
                        f"Stage {self.current_stage['name']} paused by pause in task with id {task.key}: {task.error}"
                    )
                else:
                    self.current_stage["error"] = f"Stage paused by pause error raised in stage: {formatted_exc}"
                self.stages_stack.pop()
                await self.request_context.update_task()
                raise
            if isinstance(__exc_value, CompensatedSuccess):
                self.current_stage["status"] = "failed"
                self.current_stage["error"] = f"Stage failed (and succesfully compensated) by error: {formatted_exc}"
                self.stages_stack.pop()
                await self.request_context.update_task()
                raise
            if isinstance(__exc_value, CompensatedError):
                # if a compensation error occurred for some stage,
                # we pause the current task to investigate manually
                self.current_stage["status"] = "failed"
                self.current_stage["error"] = f"Stage (and compensation error occurs) failed by error: {formatted_exc}"
                self.stages_stack.pop()
                await self.request_context.update_task()
                raise PausedError(self.current_stage["error"]) from __exc_value
            if isinstance(__exc_value, Exception):
                self.current_stage["status"] = "failed"
                self.current_stage["error"] = f"Stage failed by error: {formatted_exc}"
                self.stages_stack.pop()
                await self.request_context.update_task()
                raise
            if __exc_type is None:
                self.current_stage["status"] = "completed"
                self.stages_stack.pop()
                await self.request_context.update_task()

    @classmethod
    async def run_func_as_stage(
        cls: type[Self],
        fn: Callable[Concatenate[P], Awaitable[T]],
        stage_name: str,
        request_handler: AsyncRequestHandler[_T_request_payload, _T_request_result],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        stage_manager = cls(stage_name, request_handler)
        async with stage_manager as current_stage:
            # skip the stage if it can be skipped
            # (there were previous attempts, and we take the result from the last successful one)
            # (or the current stage in the attempt has already been completed -
            # this can happen if the handler is resuming work from a pause)
            if not stage_manager.skip_stage(request_handler.request_context, current_stage):
                if current_stage["status"] == "resumed":
                    # if stage management returns a stage marked as resumed, then
                    # a job retry from pause has occurred, and we need to pass
                    # it to the _wait_async_result method, which won't create a new job, but will wait for this one
                    kwargs["task"] = current_stage["task"]
                result = await fn(*args, **kwargs)
                current_stage["result"] = result
            else:
                result = current_stage["result"]
        return result

    @classmethod
    def skip_stage(cls: type[Self], request_context: TaskExecutionContext[Any, Any], stage: Stage) -> bool:
        # the check should be at the beginning of the stage, if it returns True we should immediately exit the current stage
        # so that the stage is marked as completed
        if stage["status"] == "completed":
            # is the current stage completed? this can happen due to the handler resuming work from a pause
            return True
        if bool(
            # skip compensation stages only if the stage has a completed status
            # (check above). We don't check previous attempts, because there are no attempts for compensation
            request_context.current_attempt != "compensate_attempt"
            and request_context.prelatest_attempt_info
            and stage["name"] in request_context.prelatest_attempt_info.attempt_stages
            and request_context.prelatest_attempt_info.attempt_stages[stage["name"]]["status"] == "completed"
        ):
            # we know there's a previous attempt, and there's a stage with exactly the same name
            # so we can take the result from there, but make a note that the stage was skipped.
            prev_stage = request_context.prelatest_attempt_info.attempt_stages[stage["name"]]
            result = prev_stage["result"]
            if isinstance(result, (dict, list)):
                stage["result"] = deepcopy(result)
            else:
                stage["result"] = result
            stage["skipped_due_previous_attempt_success"] = True
            stage["task"] = prev_stage["task"]
            return True
        return False


class AsyncRequestHandler(ABC, Generic[_T_request_payload, _T_request_result]):
    task_manager: AsyncTaskManager
    logger: Logger
    request_context: TaskExecutionContext[_T_request_payload, _T_request_result]

    @classmethod
    def _paused_errors_handler(cls: type[Self], excgroup: ExceptionGroup[PausedError]) -> NoReturn:
        exceptions: tuple[PausedError | ExceptionGroup[PausedError], ...] = excgroup.exceptions
        raise PausedError("\n".join(str(exc) for exc in exceptions))

    @asynccontextmanager
    async def create_task_group(self: Self) -> AsyncGenerator[TaskGroup, Any]:
        with catch(
            {
                PausedError: self._paused_errors_handler,  # type: ignore
            }
        ):
            async with anyio.create_task_group() as tg:
                yield tg

    async def _request_stage_result(  # noqa: PLR0913
        self: Self,
        request_cls: type[AsyncRequest[_T_request_stage_payload, _T_request_stage_result]],
        payload: _T_request_stage_payload,
        queue: str,
        *,
        timeout: int,
        heartbeat: int,
        retries: int,
        ttl: int,
        retry_delay: float,
        retry_backoff: bool | float,
        task: AsyncTask | None = None,
    ) -> _T_request_stage_result:
        if not task:
            task = await self.task_manager.send_request(
                request_cls(
                    correlation_id=self.request_context.correlation_id,
                    causation_id=self.request_context.request_id,
                    payload=payload,
                ),
                queue,
                timeout=timeout,
                heartbeat=heartbeat,
                retries=retries,
                ttl=ttl,
                retry_delay=retry_delay,
                retry_backoff=retry_backoff,
            )
        # if a task is passed, this is the case where the stage management code decided to provide existing task
        return await self.task_manager.await_task_completion_and_get_validated_result(task)

    async def request_stage(  # noqa: PLR0913
        self: Self,
        stage: str,
        request_cls: type[AsyncRequest[_T_request_stage_payload, _T_request_stage_result]],
        payload: _T_request_stage_payload,
        queue: str,
        *,
        timeout: int = 10,
        heartbeat: int = 0,
        retries: int = 1,
        ttl: int = 600,
        retry_delay: float = 0.0,
        retry_backoff: bool | float = False,
    ) -> _T_request_stage_result:
        return await self.function_stage(
            self._request_stage_result,
            stage,
            request_cls,
            payload,
            queue,
            timeout=timeout,
            heartbeat=heartbeat,
            retries=retries,
            ttl=ttl,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
        )

    async def compensate_request_stage(self: Self, stage_name: str) -> None:
        await self.function_stage(self._compensate_request_stage, f"compensate.{stage_name}", stage_name)

    async def _compensate_request_stage(self: Self, stage_name: str) -> tuple[bool, str] | None:
        latest_attempt_info = self.request_context.latest_attempt_info
        if (request_stage := latest_attempt_info.attempt_stages.get(stage_name)) and request_stage["task"]:
            task = request_stage["task"]
            compensate_result = await self.task_manager.compensate_task(task)
            await self.task_manager.await_task_completion(task)
            if task.compensation_status != "complete":
                msg = f"stage {request_stage} with task {task.key} not compensated"
                raise CompensatedError(msg)
            return compensate_result
        return None

    async def function_stage(
        self: Self,
        fn: Callable[Concatenate[P], Awaitable[T]],
        stage_name: str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        return await StageManager.run_func_as_stage(fn, stage_name, self, *args, **kwargs)

    @abstractmethod
    async def process_request(
        self: Self,
    ) -> _T_request_result:
        """Handle."""

    async def on_request_abort(self: Self) -> None:
        pass

    async def on_request_error(
        self: Self,
        error: Exception,
    ) -> Exception:
        return error

    async def process_compensate(
        self: Self,
    ) -> None:
        """Handle."""

    async def on_compensate_abort(self: Self) -> None:
        pass

    async def on_compensate_error(
        self: Self,
        error: Exception,
    ) -> Exception:
        return error
