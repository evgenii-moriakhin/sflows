import anyio
import pytest
from anyio.abc import TaskGroup

from sflows.core.manager import AsyncTaskManager
from sflows.exceptions import CompensatedSuccess
from sflows.models.config import AsyncTaskManagerConfig, RequestHandlerConfig
from sflows.models.request import AsyncRequest
from sflows.request_handler import AsyncRequestHandler

pytestmark = pytest.mark.anyio
TEST_REDIS_URL = "redis://localhost:6379"


@pytest.mark.parametrize("retries", [1, 2, 3])
async def test_compensate_simple_activity(task_group: TaskGroup, retries: int):
    compensated = 0

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            msg = "some value error"
            raise ValueError(msg)
            return "success"

        async def on_request_error(self, error: Exception) -> Exception:
            if self.request_context.is_last_attempt:
                self.request_context.set_need_compensate()
            return error

        async def process_compensate(self) -> None:
            nonlocal compensated
            compensated += 1

    manager = AsyncTaskManager(
        config=AsyncTaskManagerConfig(
            key="test_worker",
            redis_url=TEST_REDIS_URL,
            queue_name="main",
            request_handlers_cfgs=[RequestHandlerConfig(SimpleRequest, SimpleActivity)],
        )
    )
    task_group.start_soon(manager.startup)
    await anyio.sleep(0.1)
    try:
        simple_request = SimpleRequest(
            correlation_id="1",
            causation_id="1",
            payload=None,
        )
        task = await manager.send_request(simple_request, "main", retries=retries)
        with pytest.raises(CompensatedSuccess):
            await manager.await_task_completion_and_get_result(task)

        # wait the task settle
        await anyio.sleep(1)

        assert task.paused is False
        assert task.need_compensate is False
        assert task.pause_event_set is False

        assert task.attempts == retries
        assert task.status == "failed"
        assert task.completed
        assert task.compensation_status == "complete"
        assert not task.compensation_error

        assert compensated == 1
    finally:
        await manager.shutdown()


@pytest.mark.parametrize("retries", [1, 2, 3])
async def test_compensate_workflow_with_child_activity_failed(task_group: TaskGroup, retries: int):
    workflow_compensated = 0
    activity_compensated = 0

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.request_stage("simple_activity_stage", SimpleActivityRequest, None, "main")

        async def on_request_error(self, error: Exception) -> Exception:
            if self.request_context.is_last_attempt:
                self.request_context.set_need_compensate()
            return error

        async def process_compensate(self) -> None:
            nonlocal workflow_compensated
            workflow_compensated += 1

    class SimpleActivityRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            msg = "some value error"
            raise ValueError(msg)
            return "success"

        async def on_request_error(self, error: Exception) -> Exception:
            if self.request_context.is_last_attempt:
                self.request_context.set_need_compensate()
            return error

        async def process_compensate(self) -> None:
            nonlocal activity_compensated
            activity_compensated += 1

    manager = AsyncTaskManager(
        config=AsyncTaskManagerConfig(
            key="test_worker",
            redis_url=TEST_REDIS_URL,
            queue_name="main",
            request_handlers_cfgs=[
                RequestHandlerConfig(SimpleRequest, SimpleWorkflow),
                RequestHandlerConfig(SimpleActivityRequest, SimpleActivity),
            ],
        )
    )
    task_group.start_soon(manager.startup)
    await anyio.sleep(0.1)
    try:
        simple_request = SimpleRequest(
            correlation_id="1",
            causation_id="1",
            payload=None,
        )
        task = await manager.send_request(simple_request, "main", retries=retries)
        with pytest.raises(CompensatedSuccess):
            await manager.await_task_completion_and_get_result(task)

        # wait the task settle
        await anyio.sleep(1)

        assert task.paused is False
        assert task.need_compensate is False
        assert task.pause_event_set is False

        assert task.attempts == retries
        assert task.status == "failed"
        assert task.completed
        assert task.compensation_status == "complete"
        assert not task.compensation_error

        assert workflow_compensated == 1

        # Workflow on retry starts new activity each time because it was unsuccessful on the previous attempt
        assert activity_compensated == retries
    finally:
        await manager.shutdown()


@pytest.mark.parametrize("retries", [1, 2, 3])
async def test_compensate_stage_that_already_compensated(task_group: TaskGroup, retries: int):
    workflow_compensated = 0
    activity_compensated = 0

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.request_stage("simple_activity_stage", SimpleActivityRequest, None, "main")

        async def on_request_error(self, error: Exception) -> Exception:
            if self.request_context.is_last_attempt:
                self.request_context.set_need_compensate()
            return error

        async def process_compensate(self) -> None:
            nonlocal workflow_compensated
            workflow_compensated += 1
            # The activity was already compensated for itself after the error occurred in it
            await self.compensate_request_stage("simple_activity_stage")

    class SimpleActivityRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            msg = "some value error"
            raise ValueError(msg)
            return "success"

        async def on_request_error(self, error: Exception) -> Exception:
            if self.request_context.is_last_attempt:
                self.request_context.set_need_compensate()
            return error

        async def process_compensate(self) -> None:
            nonlocal activity_compensated
            activity_compensated += 1

    manager = AsyncTaskManager(
        config=AsyncTaskManagerConfig(
            key="test_worker",
            redis_url=TEST_REDIS_URL,
            queue_name="main",
            request_handlers_cfgs=[
                RequestHandlerConfig(SimpleRequest, SimpleWorkflow),
                RequestHandlerConfig(SimpleActivityRequest, SimpleActivity),
            ],
        )
    )
    task_group.start_soon(manager.startup)
    await anyio.sleep(0.1)
    try:
        simple_request = SimpleRequest(
            correlation_id="1",
            causation_id="1",
            payload=None,
        )
        task = await manager.send_request(simple_request, "main", retries=retries)
        with pytest.raises(CompensatedSuccess):
            await manager.await_task_completion_and_get_result(task)

        # wait the task settle
        await anyio.sleep(1)

        assert task.paused is False
        assert task.need_compensate is False
        assert task.pause_event_set is False

        assert task.attempts == retries
        assert task.status == "failed"
        assert task.completed
        assert task.compensation_status == "complete"
        assert not task.compensation_error

        assert workflow_compensated == 1

        # Workflow on retry starts new activity each time because it was unsuccessful on the previous attempt.
        # despite “double” compensation attempts (activity is compensated on error in last attempt
        # and activity is compensated from the workflow"), the number of compensations is correct
        assert activity_compensated == retries
    finally:
        await manager.shutdown()
