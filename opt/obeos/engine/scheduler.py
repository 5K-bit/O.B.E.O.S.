from __future__ import annotations

import asyncio
from typing import Awaitable, Callable


JobFn = Callable[[], Awaitable[None]]


class Scheduler:
    """
    Minimal job control.
      - queue of async jobs
      - single worker for deterministic ordering
    """

    def __init__(self) -> None:
        self._q: asyncio.Queue[tuple[int, JobFn]] = asyncio.Queue()
        self._worker_task: asyncio.Task[None] | None = None
        self._closing = asyncio.Event()

    def start(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker(), name="obeos.scheduler.worker")

    async def stop(self) -> None:
        self._closing.set()
        if self._worker_task is not None:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

    async def enqueue(self, job_id: int, fn: JobFn) -> None:
        await self._q.put((job_id, fn))

    async def _worker(self) -> None:
        while not self._closing.is_set():
            job_id, fn = await self._q.get()
            try:
                await fn()
            finally:
                self._q.task_done()
