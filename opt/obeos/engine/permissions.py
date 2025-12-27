from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Identity:
    """
    Minimal identity model for v0.1.

    In production, map this to:
      - unix socket peer credentials (SO_PEERCRED)
      - explicit auth tokens
      - policy rules in config
    """

    uid: Optional[int] = None
    gid: Optional[int] = None
    pid: Optional[int] = None


class PermissionError(Exception):
    pass


class Permissions:
    """
    v0.1: allow everything.
    """

    def assert_allowed(self, identity: Identity, command: str) -> None:
        _ = identity, command
        return
