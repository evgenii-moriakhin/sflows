from __future__ import annotations

import asyncio
import logging
import traceback
from typing import TYPE_CHECKING, Any, cast

import saq
from saq.utils import now
from saq.worker import ensure_coroutine_function, logger  # type: ignore
from typing_extensions import Self

from ..exceptions import PausedError  # noqa: TID252
from .task import AsyncTask

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Collection
    from signal import Signals

    from saq.job import CronJob

    from ..types import Context, PartialTimersDict  # noqa: TID252


class Worker(saq.Worker):
    def __init__(  # noqa: PLR0913
        self: Self,
        queue: saq.Queue,
        functions: Collection[Callable[..., Any] | tuple[str, Callable[..., Any]]],
        *,
        concurrency: int = 10,
        cron_jobs: Collection[CronJob] | None = None,
        startup: Callable[[Context], Awaitable[Any]] | None = None,
        shutdown: Callable[[Context], Awaitable[Any]] | None = None,
        before_process: Callable[[Context], Awaitable[Any]] | None = None,
        after_process: Callable[[Context], Awaitable[Any]] | None = None,
        timers: PartialTimersDict | None = None,
        dequeue_timeout: float = 0,
        signals: list[Signals] | None = None,
    ) -> None:
        if signals is not None:
            # same issue: https://github.com/samuelcolvin/arq/issues/182
            self.SIGNALS = signals
        super().__init__(
            queue,
            functions,
            concurrency=concurrency,
            cron_jobs=cron_jobs,
            startup=startup,
            shutdown=shutdown,
            before_process=before_process,
            after_process=after_process,
            timers=timers
            or {
                "schedule": 1,
                "stats": 10,
                "sweep": 1,
                "abort": 1,
            },
            dequeue_timeout=dequeue_timeout,
        )

    async def process(self: Self) -> None:  # noqa: PLR0912, C901
        # pylint: disable=too-many-branches
        context: Context | None = None
        job: AsyncTask | None = None

        try:
            job = await self.queue.dequeue(self.dequeue_timeout)  # type: ignore

            if job is None:
                return

            job.started = now()
            job.status = saq.Status.ACTIVE
            if not job.paused and not job.need_compensate:
                job.attempts += 1
            else:
                if job.paused:
                    job.heartbeats_history.append({"attempt": job.attempts, "message": "Resumed", "timestamp": now()})
                if job.need_compensate:
                    job.compensation_status = "started"
                    job.heartbeats_history.append(
                        {"attempt": "compensate_attempt", "message": "Compensation", "timestamp": now()}
                    )
                job.paused = False

            await job.update()
            context = {**self.context, "job": job}
            await self._before_process(context)
            logger.info("Processing %s", job.info(logger.isEnabledFor(logging.DEBUG)))

            function = ensure_coroutine_function(self.functions[job.function])  # type: ignore
            task = asyncio.create_task(function(context, **(job.kwargs or {})))  # type: ignore
            self.job_task_contexts[job] = {"task": task, "aborted": None}
            result = await asyncio.wait_for(task, job.timeout if job.timeout else None)  # type: ignore
            await job.finish(saq.Status.COMPLETE, result=result)
        except asyncio.CancelledError:
            if job:
                aborted = self.job_task_contexts.get(job, {}).get("aborted")
                if aborted:
                    await job.finish(saq.Status.ABORTED, error=aborted)
                else:
                    await job.retry("cancelled")
        except PausedError:
            logger.exception("Paused job %s", job)
            job = cast(AsyncTask, job)
            job.paused = True
            await job.update()
            await job.finish(saq.Status.ABORTED, error=traceback.format_exc())
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error processing job %s", job)
            if job:
                error = traceback.format_exc()

                if job.attempts >= job.retries:
                    if isinstance(exc, asyncio.exceptions.TimeoutError):
                        await job.finish(saq.Status.ABORTED, error=error)
                    else:
                        await job.finish(saq.Status.FAILED, error=error)
                else:
                    await job.retry(error)
        finally:
            if context:
                if job is not None:
                    self.job_task_contexts.pop(job, None)

                try:
                    await self._after_process(context)
                except (Exception, asyncio.CancelledError):
                    logger.exception("Failed to run after process hook")
