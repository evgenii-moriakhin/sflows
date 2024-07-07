from typing import TYPE_CHECKING

import anyio
import pytest
from anyio.abc import TaskGroup
from pydantic import ValidationError

from sflows.core.manager import AsyncTaskManager
from sflows.exceptions import AbortedError, UnprocessedError
from sflows.models.config import AsyncTaskManagerConfig, RequestHandlerConfig
from sflows.models.request import AsyncRequest
from sflows.request_handler import AsyncRequestHandler

if TYPE_CHECKING:
    from sflows.core.task import AsyncTask

pytestmark = pytest.mark.anyio
TEST_REDIS_URL = "redis://localhost:6379"


async def test_task_execution_success(task_group: TaskGroup):
    """Test successful execution of a simple asynchronous task.
    Verifies that a basic task is processed correctly and returns the expected result.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return "success"

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
        task = await manager.send_request(simple_request, "main")
        assert await manager.await_task_completion_and_get_validated_result(task) == "success"
    finally:
        await manager.shutdown()


async def test_multiple_concurrent_tasks_execution_success(task_group: TaskGroup):
    """Test concurrent execution of multiple simple tasks.
    Ensures that the system can handle and correctly process multiple tasks simultaneously.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return "success"

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
    finally:
        await manager.shutdown()


async def test_task_execution_error(task_group: TaskGroup):
    """Test error handling during task execution.
    Verifies that the system properly handles and reports errors that occur during task processing.
    """

    class SimpleRequest(AsyncRequest[None, str]):
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
        task = await manager.send_request(simple_request, "main")
        with pytest.raises(UnprocessedError, match="some value error"):
            await manager.await_task_completion_and_get_validated_result(task)
    finally:
        await manager.shutdown()


async def test_task_return_value_validation_error(task_group: TaskGroup):
    """Test validation of task return values.
    Ensures that the system correctly handles and reports validation errors for task results.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            return 1  # type: ignore

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
        task = await manager.send_request(simple_request, "main")
        with pytest.raises(ValidationError):
            await manager.await_task_completion_and_get_validated_result(task)
    finally:
        await manager.shutdown()


async def test_different_await_task_completion_calls(task_group: TaskGroup):
    """Test various methods of awaiting task completion.
    Compares different ways of waiting for and retrieving task results, including error scenarios.
    """

    class SimpleRequest(AsyncRequest[None, str]):
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
        task = await manager.send_request(simple_request, "main")
        await manager.await_task_completion(task)
        with pytest.raises(UnprocessedError, match="some value error"):
            await manager.await_task_completion_and_get_result(task)
        with pytest.raises(UnprocessedError, match="some value error"):
            await manager.await_task_completion_and_get_validated_result(task)
    finally:
        await manager.shutdown()


async def test_task_retries_on_error(task_group: TaskGroup):
    """Test the retry mechanism for failed tasks.
    Verifies that tasks are retried the specified number of times when they encounter errors.
    """
    retries_count = 0

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            nonlocal retries_count
            retries_count += 1
            msg = "some value error"
            raise ValueError(msg)

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
        task = await manager.send_request(simple_request, "main", retries=3)
        await manager.await_task_completion(task)
        assert retries_count == 3
    finally:
        await manager.shutdown()


async def test_task_timeout(task_group: TaskGroup):
    """Test task timeout functionality.
    Ensures that tasks are properly terminated when they exceed their specified timeout period.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            await anyio.sleep(10)
            return "success"

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
        task = await manager.send_request(simple_request, "main", timeout=1)
        with pytest.raises(AbortedError, match="Timeout"):
            await manager.await_task_completion_and_get_result(task)
    finally:
        await manager.shutdown()


async def test_task_heartbeat_timeout(task_group: TaskGroup):
    """Test task heartbeat timeout mechanism.
    Verifies that tasks are terminated when they fail to send heartbeats within the specified interval.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            await anyio.sleep(10)
            return "success"

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
        task = await manager.send_request(simple_request, "main", heartbeat=1)
        with pytest.raises(AbortedError, match="swept"):
            await manager.await_task_completion_and_get_result(task)
    finally:
        await manager.shutdown()


async def test_task_abort(task_group: TaskGroup):
    """Test manual task abortion functionality.
    Verifies that tasks can be manually aborted and that the abortion is properly handled.
    """

    class SimpleRequest(AsyncRequest[None, str]):
        pass

    class SimpleActivity(AsyncRequestHandler[None, str]):
        async def process_request(self) -> str:
            await anyio.sleep(10)
            return "success"

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
        task = await manager.send_request(simple_request, "main")
        await anyio.sleep(1)
        await task.abort("aborted by test")
        with pytest.raises(AbortedError, match="aborted by test"):
            await manager.await_task_completion_and_get_result(task)
    finally:
        await manager.shutdown()
