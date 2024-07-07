from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import pytest

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from anyio.abc import TaskGroup


@pytest.fixture()
def anyio_backend():
    return "asyncio"


@pytest.fixture()
async def task_group() -> AsyncGenerator[TaskGroup, None]:
    task_group = anyio.create_task_group()
    async with task_group:
        yield task_group
