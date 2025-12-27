from __future__ import annotations

import asyncio
import time
from typing import Any


class Module:
    name = "build"
    commands = ["build"]

    def __init__(self) -> None:
        self._bus = None
        self._state = None

    def register(self, bus, state) -> None:  # noqa: ANN001
        self._bus = bus
        self._state = state

    async def handle(self, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        if command != "build":
            raise ValueError(f"build module cannot handle: {command}")

        args = payload.get("args", [])
        ident = payload.get("identity", {}) or {}
        owner = f"uid:{ident.get('uid')}" if ident.get("uid") is not None else None

        job_id = self._state.create_job("build", args, owner=owner)
        self._bus.publish(
            source="module:build",
            type="job.created",
            payload={"job_id": job_id, "command": "build", "args": args},
        )
        return {"job_id": job_id, "status": "queued"}

    async def run_job(self, job_id: int, payload: dict[str, Any]) -> None:
        args = payload.get("args", [])
        self._state.update_job(job_id, status="running")
        self._bus.publish(source="module:build", type="job.started", payload={"job_id": job_id})

        # v0.1: simulate work (no privileged operations, no subprocesses).
        await asyncio.sleep(0.2)

        result = {
            "built": True,
            "args": args,
            "finished_at": time.time(),
        }
        self._state.update_job(job_id, status="completed", result=result)
        self._bus.publish(
            source="module:build",
            type="job.finished",
            payload={"job_id": job_id, "status": "completed"},
        )
