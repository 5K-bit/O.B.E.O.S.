from __future__ import annotations

import time
from typing import Any, Optional


def _parse_duration_seconds(s: str) -> float:
    """
    Accepts: "10s", "5m", "1h", "2d" or plain seconds ("30").
    """
    s = s.strip().lower()
    if not s:
        raise ValueError("empty duration")
    suffix = s[-1]
    if suffix.isdigit():
        return float(s)
    n = float(s[:-1])
    if suffix == "s":
        return n
    if suffix == "m":
        return n * 60
    if suffix == "h":
        return n * 3600
    if suffix == "d":
        return n * 86400
    raise ValueError(f"unknown duration suffix: {suffix}")


class Module:
    name = "observe"
    commands = ["status", "jobs", "logs", "ping"]

    def __init__(self) -> None:
        self._bus = None
        self._state = None

    def register(self, bus, state) -> None:  # noqa: ANN001
        self._bus = bus
        self._state = state

    async def handle(self, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        args = payload.get("args", [])

        if command == "ping":
            return {"pong": True, "ts": time.time()}

        if command == "status":
            hb = self._state.get_system_state("engine.heartbeat", default=None)
            hb_updated = self._state.get_system_state_updated_at("engine.heartbeat")
            return {
                "heartbeat": hb,
                "heartbeat_updated_at": hb_updated,
                "stats": self._state.stats(),
            }

        if command == "jobs":
            limit = 50
            status: Optional[str] = None
            if len(args) >= 1:
                try:
                    limit = int(args[0])
                except Exception:  # noqa: BLE001
                    limit = 50
            if len(args) >= 2 and isinstance(args[1], str) and args[1]:
                status = args[1]
            return {"jobs": self._state.list_jobs(limit=limit, status=status)}

        if command == "logs":
            # bf logs last 1h
            # bf logs last 200
            limit = 200
            since_ts: Optional[float] = None
            if len(args) >= 1 and args[0] == "last":
                if len(args) >= 2:
                    v = args[1]
                    if isinstance(v, (int, float)):
                        limit = int(v)
                    elif isinstance(v, str):
                        # duration form
                        seconds = _parse_duration_seconds(v)
                        since_ts = time.time() - seconds
            elif len(args) >= 1 and args[0] == "since" and len(args) >= 2:
                try:
                    since_ts = float(args[1])
                except Exception as e:  # noqa: BLE001
                    raise ValueError(f"invalid since timestamp: {e}") from e

            events = self._state.list_events(since_ts=since_ts, limit=limit)
            # Return in chronological order for humans
            events = list(reversed(events))
            return {
                "events": [
                    {
                        "id": e.id,
                        "timestamp": e.timestamp,
                        "source": e.source,
                        "type": e.type,
                        "payload": e.payload,
                    }
                    for e in events
                ]
            }

        raise ValueError(f"observe module cannot handle: {command}")
