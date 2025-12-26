from __future__ import annotations

import time
from typing import Any


class Module:
    name = "control"
    commands = ["shutdown"]

    def __init__(self) -> None:
        self._bus = None
        self._state = None

    def register(self, bus, state) -> None:  # noqa: ANN001
        self._bus = bus
        self._state = state

    async def handle(self, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        if command != "shutdown":
            raise ValueError(f"control module cannot handle: {command}")

        self._bus.publish(
            source="module:control",
            type="engine.shutdown.requested",
            payload={
                "ts": time.time(),
                "identity": payload.get("identity", {}),
            },
        )
        return {"shutdown_requested": True}
