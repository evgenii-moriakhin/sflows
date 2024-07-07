from typing import TYPE_CHECKING

import anyio
import pytest
from anyio.abc import TaskGroup

from sflows.core.manager import AsyncTaskManager
from sflows.exceptions import UnprocessedError
from sflows.models.config import AsyncTaskManagerConfig, RequestHandlerConfig
from sflows.models.request import AsyncRequest
from sflows.request_handler import AsyncRequestHandler

if TYPE_CHECKING:
    from sflows.core.task import AsyncTask

pytestmark = pytest.mark.anyio
TEST_REDIS_URL = "redis://localhost:6379"


async def test_single_stage_inside_workflow_success(task_group: TaskGroup):
    """Test successful execution of a single-stage workflow.
    Verifies that a workflow with one stage is processed correctly, returns the expected result,
    and properly records stage information.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.function_stage(self.simple_stage, "simple_stage")

        async def simple_stage(self) -> str:
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
        assert await manager.await_task_completion_and_get_validated_result(task) == "success"
        assert task.stages
        stage = task.stages[0]
        assert stage["name"] == "simple_stage"
        assert stage["result"] == "success"
        assert stage["skipped_due_previous_attempt_success"] is False
        assert stage["status"] == "completed"
        assert stage["previous_states"] == []
        assert stage["error"] is None
        assert stage["attempt"] == 1
    finally:
        await manager.shutdown()


async def test_multiple_workflows_with_single_stage_inside_success(task_group: TaskGroup):
    """Test concurrent execution of multiple single-stage workflows.
    Ensures that the system can handle and correctly process multiple workflows simultaneously,
    each containing a single stage, and properly record stage information for each.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.function_stage(self.simple_stage, "simple_stage")

        async def simple_stage(self) -> str:
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
        tasks: list[AsyncTask] = []
        for number in range(10):
            simple_request = SimpleRequest(
                correlation_id=str(number),
                causation_id=str(number),
                payload=None,
            )
            tasks.append(await manager.send_request(simple_request, "main"))
        results = [await manager.await_task_completion_and_get_result(task) for task in tasks]
        assert results == ["success"] * 10
        for task in tasks:
            assert task.stages
            stage = task.stages[0]
            assert stage["name"] == "simple_stage"
            assert stage["result"] == "success"
            assert stage["skipped_due_previous_attempt_success"] is False
            assert stage["status"] == "completed"
            assert stage["previous_states"] == []
            assert stage["error"] is None
            assert stage["attempt"] == 1
    finally:
        await manager.shutdown()


async def test_single_stage_inside_workflow_error(task_group: TaskGroup):
    """Test error handling in a single-stage workflow.
    Verifies that errors occurring within a workflow stage are properly handled,
    reported, and recorded in the stage information.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.function_stage(self.simple_stage, "simple_stage")

        async def simple_stage(self) -> str:
            msg = "some value error"
            raise ValueError(msg)

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
        with pytest.raises(UnprocessedError, match="some value error"):
            await manager.await_task_completion_and_get_result(task)
        assert task.stages
        stage = task.stages[0]
        assert stage["name"] == "simple_stage"
        assert stage["result"] is None
        assert stage["skipped_due_previous_attempt_success"] is False
        assert stage["status"] == "failed"
        assert stage["previous_states"] == []
        assert stage["error"]
        assert stage["error"].startswith("Stage failed by error")
        assert stage["attempt"] == 1
    finally:
        await manager.shutdown()


async def test_several_nested_stages_inside_workflow_success(task_group: TaskGroup):
    """Test successful execution of a workflow with nested stages.
    Verifies that a workflow with multiple nested stages is processed correctly,
    returns the expected result, and properly records information for each stage,
    including their hierarchical relationship.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.function_stage(self.first_stage, "first_stage")

        async def first_stage(self) -> str:
            return await self.function_stage(self.second_stage, "second_stage")

        async def second_stage(self) -> str:
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
        assert await manager.await_task_completion_and_get_validated_result(task) == "success"
        assert len(task.stages) == 2
        first_stage, second_stage = task.stages
        assert first_stage["name"] == "first_stage"
        assert first_stage["result"] == "success"
        assert first_stage["skipped_due_previous_attempt_success"] is False
        assert first_stage["status"] == "completed"
        assert first_stage["previous_states"] == []
        assert first_stage["error"] is None
        assert first_stage["attempt"] == 1

        assert second_stage["name"] == "first_stage.second_stage"
        assert second_stage["result"] == "success"
        assert second_stage["skipped_due_previous_attempt_success"] is False
        assert second_stage["status"] == "completed"
        assert second_stage["previous_states"] == []
        assert second_stage["error"] is None
        assert second_stage["attempt"] == 1
    finally:
        await manager.shutdown()


async def test_several_nested_parallel_stages_inside_workflow_success(task_group: TaskGroup):
    """Test successful execution of a workflow with nested parallel stages.
    Verifies that a workflow with multiple nested stages executing in parallel
    is processed correctly, returns the expected result, and properly records
    information for each stage.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.function_stage(self.first_stage, "first_stage")

        async def first_stage(self) -> str:
            async with self.create_task_group() as tg:
                tg.start_soon(self.function_stage, self.second_stage, "second_stage")
                tg.start_soon(self.function_stage, self.third_stage, "third_stage")
            return "first_stage_success"

        async def second_stage(self) -> str:
            return "second_stage_success"

        async def third_stage(self) -> str:
            return "third_stage_success"

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
        assert await manager.await_task_completion_and_get_validated_result(task) == "first_stage_success"
        assert len(task.stages) == 3
        first_stage, second_stage, third_stage = task.stages
        assert first_stage["name"] == "first_stage"
        assert first_stage["result"] == "first_stage_success"
        assert first_stage["skipped_due_previous_attempt_success"] is False
        assert first_stage["status"] == "completed"
        assert first_stage["previous_states"] == []
        assert first_stage["error"] is None
        assert first_stage["attempt"] == 1

        assert second_stage["name"] == "first_stage.second_stage"
        assert second_stage["result"] == "second_stage_success"
        assert second_stage["skipped_due_previous_attempt_success"] is False
        assert second_stage["status"] == "completed"
        assert second_stage["previous_states"] == []
        assert second_stage["error"] is None
        assert second_stage["attempt"] == 1

        assert third_stage["name"] == "first_stage.third_stage"
        assert third_stage["result"] == "third_stage_success"
        assert third_stage["skipped_due_previous_attempt_success"] is False
        assert third_stage["status"] == "completed"
        assert third_stage["previous_states"] == []
        assert third_stage["error"] is None
        assert third_stage["attempt"] == 1
    finally:
        await manager.shutdown()


async def test_single_request_stage_inside_workflow_success(task_group: TaskGroup):
    """Test successful execution of a workflow with a single request stage.
    Verifies that a workflow containing a single request stage (which triggers another activity)
    is processed correctly.
    """

    class SimpleWorkflowRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.request_stage("activity_request_stage", SimpleActivityRequest, None, "main")

    class SimpleActivityRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
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
        simple_workflow_request = SimpleWorkflowRequest(
            correlation_id="1",
            causation_id="1",
            payload=None,
        )
        task = await manager.send_request(simple_workflow_request, "main")
        assert await manager.await_task_completion_and_get_validated_result(task) == "success"
        assert task.stages
        stage = task.stages[0]
        assert stage["name"] == "activity_request_stage"
        assert stage["result"] == "success"
        assert stage["skipped_due_previous_attempt_success"] is False
        assert stage["status"] == "completed"
        assert stage["previous_states"] == []
        assert stage["error"] is None
        assert stage["attempt"] == 1
        assert stage["task"]
        assert stage["task"].function == SimpleActivityRequest.name()
        assert stage["task"].status == "complete"
        assert stage["task"].result == "success"
        assert stage["task"].causation_id == "1"
        assert stage["task"].correlation_id == "1"
    finally:
        await manager.shutdown()


async def test_single_request_stage_inside_workflow_error(task_group: TaskGroup):
    """Test error handling in a workflow with a single request stage.
    Verifies that errors occurring in the activity triggered by the request stage
    are properly handled, reported, and recorded in the workflow's stage information.
    """

    class SimpleWorkflowRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.request_stage("activity_request_stage", SimpleActivityRequest, None, "main")

    class SimpleActivityRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            msg = "some value error"
            raise ValueError(msg)

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
        simple_workflow_request = SimpleWorkflowRequest(
            correlation_id="1",
            causation_id="1",
            payload=None,
        )
        task = await manager.send_request(simple_workflow_request, "main")
        with pytest.raises(UnprocessedError, match="some value error"):
            assert await manager.await_task_completion_and_get_result(task)
        assert task.stages
        stage = task.stages[0]
        assert stage["name"] == "activity_request_stage"
        assert stage["result"] is None
        assert stage["skipped_due_previous_attempt_success"] is False
        assert stage["status"] == "failed"
        assert stage["previous_states"] == []
        assert stage["error"]
        assert stage["error"].startswith("Stage failed by error")
        assert stage["attempt"] == 1
        assert stage["task"]
        assert stage["task"].function == SimpleActivityRequest.name()
        assert stage["task"].status == "failed"
        assert stage["task"].result is None
        assert stage["task"].causation_id == "1"
        assert stage["task"].correlation_id == "1"
    finally:
        await manager.shutdown()


async def test_multiple_nested_parallel_request_stages_inside_workflow_success(task_group: TaskGroup):  # noqa: PLR0915
    """Test successful execution of a workflow with multiple parallel request stages.
    Verifies that a workflow containing nested parallel request stages executes correctly,
    returns the expected result, and properly records information for each stage and subtask.
    """

    class SimpleWorkflowRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return await self.function_stage(self.group_requests_stage, "group_requests_stage")

        async def group_requests_stage(self) -> str:
            async with self.create_task_group() as tg:
                tg.start_soon(self.request_stage, "activity_request_stage1", SimpleActivityRequest, None, "main")
                tg.start_soon(self.request_stage, "activity_request_stage2", SimpleActivityRequest, None, "main")
                tg.start_soon(self.request_stage, "activity_request_stage3", SimpleActivityRequest, None, "main")
            return "success"

    class SimpleActivityRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
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
        simple_workflow_request = SimpleWorkflowRequest(
            correlation_id="1",
            causation_id="1",
            payload=None,
        )
        task = await manager.send_request(simple_workflow_request, "main")
        assert await manager.await_task_completion_and_get_validated_result(task) == "success"
        assert len(task.stages) == 4
        first_request_stage, second_request_stage, third_request_stage = task.stages[1:]
        assert first_request_stage["name"] == "group_requests_stage.activity_request_stage1"
        assert first_request_stage["result"] == "success"
        assert first_request_stage["skipped_due_previous_attempt_success"] is False
        assert first_request_stage["status"] == "completed"
        assert first_request_stage["previous_states"] == []
        assert first_request_stage["error"] is None
        assert first_request_stage["attempt"] == 1
        assert first_request_stage["task"]
        assert first_request_stage["task"].function == SimpleActivityRequest.name()
        assert first_request_stage["task"].status == "complete"
        assert first_request_stage["task"].result == "success"
        assert first_request_stage["task"].causation_id == "1"
        assert first_request_stage["task"].correlation_id == "1"

        assert second_request_stage["name"] == "group_requests_stage.activity_request_stage2"
        assert second_request_stage["result"] == "success"
        assert second_request_stage["skipped_due_previous_attempt_success"] is False
        assert second_request_stage["status"] == "completed"
        assert second_request_stage["previous_states"] == []
        assert second_request_stage["error"] is None
        assert second_request_stage["attempt"] == 1
        assert second_request_stage["task"]
        assert second_request_stage["task"].function == SimpleActivityRequest.name()
        assert second_request_stage["task"].status == "complete"
        assert second_request_stage["task"].result == "success"
        assert second_request_stage["task"].causation_id == "1"
        assert second_request_stage["task"].correlation_id == "1"

        assert third_request_stage["name"] == "group_requests_stage.activity_request_stage3"
        assert third_request_stage["result"] == "success"
        assert third_request_stage["skipped_due_previous_attempt_success"] is False
        assert third_request_stage["status"] == "completed"
        assert third_request_stage["previous_states"] == []
        assert third_request_stage["error"] is None
        assert third_request_stage["attempt"] == 1
        assert third_request_stage["task"]
        assert third_request_stage["task"].function == SimpleActivityRequest.name()
        assert third_request_stage["task"].status == "complete"
        assert third_request_stage["task"].result == "success"
        assert third_request_stage["task"].causation_id == "1"
        assert third_request_stage["task"].correlation_id == "1"
    finally:
        await manager.shutdown()


async def test_skip_stage_if_completed_on_retry(task_group: TaskGroup):
    """Test skipping of completed stages during workflow retry.
    Verifies that a workflow with multiple stages correctly skips previously completed
    stages when retried, only re-executing the failed stage.
    """

    first_stage_executed = 0
    second_stage_executed = 0
    third_stage_executed = 0
    error_raised_in_third_stage = False

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleWorkflow(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            await self.function_stage(self.first_stage, "first_stage")
            await self.function_stage(self.second_stage, "second_stage")
            await self.function_stage(self.third_stage, "third_stage")
            return "success"

        async def first_stage(self) -> str:
            nonlocal first_stage_executed
            first_stage_executed += 1
            return "success_first_stage"

        async def second_stage(self) -> str:
            nonlocal second_stage_executed
            second_stage_executed += 1
            return "success_second_stage"

        async def third_stage(self) -> str:
            nonlocal third_stage_executed
            nonlocal error_raised_in_third_stage
            if not error_raised_in_third_stage:
                error_raised_in_third_stage = True
                msg = "some value error"
                raise ValueError(msg)
            third_stage_executed += 1
            return "success_third_stage"

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
        task = await manager.send_request(simple_request, "main", retries=2)
        assert await manager.await_task_completion_and_get_validated_result(task) == "success"
        assert first_stage_executed == 1
        assert second_stage_executed == 1
        assert third_stage_executed == 1
        assert task.stages
        task_stages_attempt_1 = [stage for stage in task.stages if stage["attempt"] == 1]
        task_stages_attempt_2 = [stage for stage in task.stages if stage["attempt"] == 2]
        first_stage_attempt_1, second_stage_attempt_1, third_stage_attempt_1 = task_stages_attempt_1
        first_stage_attempt_2, second_stage_attempt_2, third_stage_attempt_2 = task_stages_attempt_2
        assert first_stage_attempt_1["name"] == first_stage_attempt_2["name"]
        assert first_stage_attempt_1["result"] == first_stage_attempt_2["result"]
        assert first_stage_attempt_1["skipped_due_previous_attempt_success"] is False
        assert first_stage_attempt_2["skipped_due_previous_attempt_success"] is True

        assert second_stage_attempt_1["name"] == second_stage_attempt_2["name"]
        assert second_stage_attempt_1["result"] == second_stage_attempt_2["result"]
        assert second_stage_attempt_1["skipped_due_previous_attempt_success"] is False
        assert second_stage_attempt_2["skipped_due_previous_attempt_success"] is True

        assert third_stage_attempt_1["name"] == third_stage_attempt_2["name"]
        assert third_stage_attempt_1["status"] == "failed"
        assert third_stage_attempt_1["error"]
        assert third_stage_attempt_1["error"].startswith("Stage failed by error")

        assert third_stage_attempt_2["status"] == "completed"
        assert third_stage_attempt_2["error"] is None
    finally:
        await manager.shutdown()
