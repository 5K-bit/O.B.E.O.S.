from __future__ import annotations

import collections
from typing import Any


class Module:
    name = "memory"
    commands = ["memory"]

    def __init__(self) -> None:
        self._bus = None
        self._state = None

    def register(self, bus, state) -> None:  # noqa: ANN001
        self._bus = bus
        self._state = state

    async def handle(self, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        if command != "memory":
            raise ValueError(f"memory module cannot handle: {command}")

        args = payload.get("args", [])
        sub = args[0] if args else "summary"
        if sub != "summary":
            raise ValueError("memory supports: summary")

        jobs = self._state.list_jobs(limit=200)
        by_status = collections.Counter(j["status"] for j in jobs)

        return {
            "summary": {
                "stats": self._state.stats(),
                "jobs_by_status": dict(by_status),
                "heartbeat": self._state.get_system_state("engine.heartbeat", default=None),
            }
        }
