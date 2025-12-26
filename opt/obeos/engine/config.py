from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class EngineConfig:
    root: Path
    socket_path: str
    db_path: str
    heartbeat_interval_sec: float
    enabled_modules: list[str]


def load_config(root: Path) -> EngineConfig:
    cfg_path = root / "config.yaml"
    raw: dict[str, Any] = {}
    if cfg_path.exists():
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}

    engine = raw.get("engine", {}) if isinstance(raw, dict) else {}
    modules = raw.get("modules", {}) if isinstance(raw, dict) else {}
    enabled = modules.get("enabled", []) if isinstance(modules, dict) else []

    socket_path = str(engine.get("socket_path", "/run/obeos.sock"))
    db_path = str(engine.get("db_path", "storage/obeos.db"))
    heartbeat = float(engine.get("heartbeat_interval_sec", 2.0))

    # Env overrides (dev-friendly)
    socket_path = os.environ.get("OBEOS_SOCKET", socket_path)
    db_path = os.environ.get("OBEOS_DB", db_path)

    return EngineConfig(
        root=root,
        socket_path=socket_path,
        db_path=db_path,
        heartbeat_interval_sec=heartbeat,
        enabled_modules=list(enabled) if isinstance(enabled, list) else [],
    )
