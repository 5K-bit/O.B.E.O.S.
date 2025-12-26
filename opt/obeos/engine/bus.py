from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import asdict
from typing import Any, Callable, DefaultDict

from state import Event, StateStore

Subscriber = Callable[[Event], None]


class Bus:
    """
    Simple internal bus:
      - ordering via sqlite autoincrement event ids
      - logging via sqlite events table
      - fan-out to subscribers
    """

    def __init__(self, state: StateStore) -> None:
        self._state = state
        self._subs: DefaultDict[str, list[Subscriber]] = defaultdict(list)

    def subscribe(self, event_type: str, cb: Subscriber) -> None:
        """
        Subscribe to an event type.
          - exact match: "job.finished"
          - wildcard: "*"
        """
        self._subs[event_type].append(cb)

    def publish(self, *, source: str, type: str, payload: dict[str, Any]) -> Event:
        ts = time.time()
        ev = self._state.append_event(ts, source, type, payload)

        # Order: wildcard first, then exact. Both receive the same Event.
        for cb in list(self._subs.get("*", [])):
            cb(ev)
        for cb in list(self._subs.get(type, [])):
            cb(ev)
        return ev

    @staticmethod
    def event_to_dict(ev: Event) -> dict[str, Any]:
        return asdict(ev)
