import anyio
import pytest
from anyio.abc import TaskGroup

from sflows.core.manager import AsyncTaskManager
from sflows.core.task import stage_to_dict
from sflows.exceptions import PausedError
from sflows.models.config import AsyncTaskManagerConfig, RequestHandlerConfig
from sflows.models.request import AsyncRequest
from sflows.request_handler import AsyncRequestHandler

pytestmark = pytest.mark.anyio
TEST_REDIS_URL = "redis://localhost:6379"


async def test_pause_error_raised_in_sequential_stage(task_group: TaskGroup):
    first_stage_executed = 0
    second_stage_executed = 0

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            await self.function_stage(self.first_stage, "first_stage")
            await self.function_stage(self.second_stage, "second_stage")
            return "success"

        async def first_stage(self) -> str:
            nonlocal first_stage_executed
            first_stage_executed += 1
            await anyio.sleep(1)
            return "success_first_stage"

        async def second_stage(self) -> str:
            nonlocal second_stage_executed
            second_stage_executed += 1
            await anyio.sleep(1)
            return "success_second_stage"

    manager = AsyncTaskManager(
        config=AsyncTaskManagerConfig(
            key="test_worker",
            redis_url=TEST_REDIS_URL,
            queue_name="main",
            request_handlers_cfgs=[RequestHandlerConfig(SimpleRequest, SimpleWorkflow)],
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
        task = await manager.send_request(simple_request, "main")
        await anyio.sleep(0.5)
        await manager.try_pause_task(task)

        with pytest.raises(PausedError):
            await manager.await_task_completion_and_get_result(task)

        assert task.paused is True
        assert task.status == "aborted"
        assert task.attempts == 1
        assert len(task.stages) == 1
        stage = task.stages[0]
        assert stage["name"] == "first_stage"
        assert stage["result"] == "success_first_stage"
        assert stage["status"] == "completed"
        assert first_stage_executed == 1

        resumed, _ = await manager.resume_task(task)
        assert resumed

        assert await manager.await_task_completion_and_get_validated_result(task) == "success"
        assert task.paused is False
        assert task.status == "complete"
        assert task.attempts == 1
        assert len(task.stages) == 2
        first_stage, second_stage = task.stages
        assert first_stage["name"] == "first_stage"
        assert first_stage["result"] == "success_first_stage"
        assert first_stage["status"] == "completed"
        # to restore from pause, this flag is not set, because stages continue to work from the current attempt
        assert first_stage["skipped_due_previous_attempt_success"] is False
        assert not first_stage["previous_states"]
        assert first_stage_executed == 1

        assert second_stage["name"] == "second_stage"
        assert second_stage["result"] == "success_second_stage"
        assert second_stage["status"] == "completed"
        assert first_stage["skipped_due_previous_attempt_success"] is False
        assert not second_stage["previous_states"]
        assert second_stage_executed == 1
    finally:
        await manager.shutdown()


async def test_pause_error_raised_in_nested_stage_of_workflow(task_group: TaskGroup):
    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.function_stage(self.first_stage, "first_stage")

        async def first_stage(self) -> str:
            await anyio.sleep(1)
            return await self.function_stage(self.second_stage, "second_stage")

        async def second_stage(self) -> str:
            await anyio.sleep(1)
            return "success"

    manager = AsyncTaskManager(
        config=AsyncTaskManagerConfig(
            key="test_worker",
            redis_url=TEST_REDIS_URL,
            queue_name="main",
            request_handlers_cfgs=[RequestHandlerConfig(SimpleRequest, SimpleWorkflow)],
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
        task = await manager.send_request(simple_request, "main")
        await anyio.sleep(0.5)
        await manager.try_pause_task(task)

        with pytest.raises(PausedError):
            await manager.await_task_completion_and_get_result(task)

        assert task.paused is True
        assert task.status == "aborted"
        assert task.attempts == 1
        assert len(task.stages) == 1
        stage = task.stages[0]
        assert stage["name"] == "first_stage"
        assert stage["result"] is None
        assert stage["status"] == "paused"
        first_stage_state_after_pause = stage_to_dict(stage)

        resumed, _ = await manager.resume_task(task)
        assert resumed

        assert await manager.await_task_completion_and_get_validated_result(task) == "success"
        assert task.paused is False
        assert task.status == "complete"
        assert task.attempts == 1
        assert len(task.stages) == 2
        first_stage, second_stage = task.stages
        assert first_stage["name"] == "first_stage"
        assert first_stage["result"] == "success"
        assert first_stage["status"] == "completed"
        assert first_stage["previous_states"] == [first_stage_state_after_pause]

        assert second_stage["name"] == "first_stage.second_stage"
        assert second_stage["result"] == "success"
        assert second_stage["status"] == "completed"
        assert not second_stage["previous_states"]
    finally:
        await manager.shutdown()


async def test_pause_resume_workflow_but_parallel_processing_request_stage_was_successful_without_pausing(  # noqa: PLR0915
    task_group: TaskGroup,
):
    activity_executed = 0

    class SimpleWorkflowRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.function_stage(self.first_stage, "first_stage")

        async def first_stage(self) -> str:
            async with self.create_task_group() as tg:
                tg.start_soon(self.request_stage, "simple_activity_stage", SimpleActivityRequest, None, "main")
                await anyio.sleep(1)
                return await self.function_stage(self.second_stage, "second_stage")  # pause error raised here

        async def second_stage(self) -> str:
            await anyio.sleep(1)
            return "success"

    class SimpleActivityRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            nonlocal activity_executed
            activity_executed += 1
            await anyio.sleep(2)
            return "success"

    manager = AsyncTaskManager(
        config=AsyncTaskManagerConfig(
            key="test_worker",
            redis_url=TEST_REDIS_URL,
            queue_name="main",
            request_handlers_cfgs=[
                RequestHandlerConfig(SimpleWorkflowRequest, SimpleWorkflow),
                RequestHandlerConfig(SimpleActivityRequest, SimpleActivity),
            ],
        )
    )
    task_group.start_soon(manager.startup)
    await anyio.sleep(0.1)
    try:
        simple_request = SimpleWorkflowRequest(
            correlation_id="1",
            causation_id="1",
            payload=None,
        )
        task = await manager.send_request(simple_request, "main")
        await anyio.sleep(0.5)
        await manager.try_pause_task(task)

        with pytest.raises(PausedError):
            await manager.await_task_completion_and_get_result(task)

        assert task.paused is True
        assert task.status == "aborted"
        assert task.attempts == 1
        assert len(task.stages) == 2
        first_stage, activity_stage = task.stages
        assert first_stage["name"] == "first_stage"
        assert first_stage["status"] == "paused"
        assert first_stage["result"] is None
        first_stage_state_after_pause = stage_to_dict(first_stage)

        assert activity_stage["name"] == "first_stage.simple_activity_stage"
        assert activity_stage["status"] == "completed"
        assert activity_stage["result"] == "success"
        assert not activity_stage["error"]
        assert activity_executed == 1

        resumed, _ = await manager.resume_task(task)
        assert resumed

        assert await manager.await_task_completion_and_get_validated_result(task) == "success"
        assert task.paused is False
        assert task.status == "complete"
        assert task.attempts == 1
        assert len(task.stages) == 3
        first_stage, activity_stage, second_stage = task.stages
        assert first_stage["name"] == "first_stage"
        assert first_stage["result"] == "success"
        assert first_stage["status"] == "completed"
        assert first_stage["previous_states"] == [first_stage_state_after_pause]

        assert activity_stage["name"] == "first_stage.simple_activity_stage"
        assert activity_stage["status"] == "completed"
        assert activity_stage["result"] == "success"
        assert not activity_stage["error"]
        assert activity_executed == 1

        assert second_stage["name"] == "first_stage.second_stage"
        assert second_stage["result"] == "success"
        assert second_stage["status"] == "completed"
        assert not second_stage["previous_states"]
    finally:
        await manager.shutdown()


async def test_pause_resume_workflow_and_parallel_processing_request_paused_too(
    task_group: TaskGroup,
):
    activity_executed = 0

    class SimpleWorkflowRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.function_stage(self.first_stage, "first_stage")

        async def first_stage(self) -> str:
            async with self.create_task_group() as tg:
                tg.start_soon(self.request_stage, "simple_activity_stage", SimpleActivityRequest, None, "main")
            return "success"

    class SimpleActivityRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            await anyio.sleep(1)
            # pause event propagates from parent workflow to child activity and raised here
            return await self.function_stage(self.inner_activity_stage, "inner_activity_stage")

        async def inner_activity_stage(self) -> str:
            nonlocal activity_executed
            activity_executed += 1
            return "success"

    manager = AsyncTaskManager(
        config=AsyncTaskManagerConfig(
            key="test_worker",
            redis_url=TEST_REDIS_URL,
            queue_name="main",
            request_handlers_cfgs=[
                RequestHandlerConfig(SimpleWorkflowRequest, SimpleWorkflow),
                RequestHandlerConfig(SimpleActivityRequest, SimpleActivity),
            ],
        )
    )
    task_group.start_soon(manager.startup)
    await anyio.sleep(0.1)
    try:
        simple_request = SimpleWorkflowRequest(
            correlation_id="1",
            causation_id="1",
            payload=None,
        )
        task = await manager.send_request(simple_request, "main")
        await anyio.sleep(0.5)
        await manager.try_pause_task(task)

        with pytest.raises(PausedError):
            await manager.await_task_completion_and_get_result(task)

        assert task.paused is True
        assert task.status == "aborted"
        assert task.attempts == 1
        assert len(task.stages) == 2
        first_stage, activity_stage = task.stages
        assert first_stage["name"] == "first_stage"
        assert first_stage["status"] == "paused"
        assert first_stage["result"] is None
        first_stage_state_after_pause = stage_to_dict(first_stage)

        assert activity_stage["name"] == "first_stage.simple_activity_stage"
        assert activity_stage["status"] == "paused"
        assert activity_stage["result"] is None
        assert activity_stage["error"]
        assert activity_stage["error"].startswith(
            "Stage first_stage.simple_activity_stage paused by pause in task with id"
        )
        assert activity_executed == 0

        resumed, _ = await manager.resume_task(task)
        assert resumed

        assert await manager.await_task_completion_and_get_validated_result(task) == "success"
        assert task.paused is False
        assert task.status == "complete"
        assert task.attempts == 1
        assert len(task.stages) == 2
        first_stage, activity_stage = task.stages
        assert first_stage["name"] == "first_stage"
        assert first_stage["result"] == "success"
        assert first_stage["status"] == "completed"
        assert first_stage["previous_states"] == [first_stage_state_after_pause]

        assert activity_stage["name"] == "first_stage.simple_activity_stage"
        assert activity_stage["status"] == "completed"
        assert activity_stage["result"] == "success"
        assert not activity_stage["error"]
        assert activity_executed == 1
    finally:
        await manager.shutdown()

