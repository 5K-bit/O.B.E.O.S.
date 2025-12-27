from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol


class ModuleContract(Protocol):
    name: str
    commands: list[str]

    def register(self, bus: Any, state: Any) -> None: ...
    async def handle(self, command: str, payload: dict[str, Any]) -> Any: ...


@dataclass(frozen=True)
class LoadedModule:
    name: str
    instance: ModuleContract
    py_module: ModuleType


def load_module_from_path(module_name: str, module_py_path: Path) -> LoadedModule:
    spec = importlib.util.spec_from_file_location(f"obeos.modules.{module_name}", str(module_py_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to create import spec for {module_name} from {module_py_path}")

    py_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(py_mod)  # type: ignore[call-arg]

    if not hasattr(py_mod, "Module"):
        raise RuntimeError(f"Module file missing Module class: {module_py_path}")

    cls = getattr(py_mod, "Module")
    inst = cls()
    if not hasattr(inst, "name"):
        raise RuntimeError(f"Module {module_name} missing name attribute")
    if not hasattr(inst, "register") or not hasattr(inst, "handle"):
        raise RuntimeError(f"Module {module_name} missing register/handle")

    # Normalize optional commands list
    if not getattr(inst, "commands", None):
        setattr(inst, "commands", [getattr(inst, "name")])

    return LoadedModule(name=getattr(inst, "name"), instance=inst, py_module=py_mod)
