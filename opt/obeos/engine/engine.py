from __future__ import annotations

import argparse
import asyncio
import os
import signal
import time
from pathlib import Path
from typing import Any, Optional

from bus import Bus
from config import EngineConfig, load_config
from ipc import IPCConfig, IPCServer
from module_loader import LoadedModule, load_module_from_path
from permissions import Identity, Permissions
from scheduler import Scheduler
from state import StateStore


def resolve_root() -> Path:
    return Path(os.environ.get("OBEOS_ROOT", "/opt/obeos")).resolve()


class Engine:
    def __init__(self, cfg: EngineConfig) -> None:
        self.cfg = cfg
        self.started_at = time.time()

        db_abs = (cfg.root / cfg.db_path).resolve() if not os.path.isabs(cfg.db_path) else Path(cfg.db_path)
        self.state = StateStore(str(db_abs))
        self.bus = Bus(self.state)
        self.perms = Permissions()
        self.scheduler = Scheduler()
        self.ipc = IPCServer(IPCConfig(socket_path=cfg.socket_path), self._handle_ipc_request)

        self._modules: list[LoadedModule] = []
        self._shutdown = asyncio.Event()

        # Engine listens to control-plane events from modules.
        self.bus.subscribe("engine.shutdown.requested", lambda ev: self.request_shutdown(reason=ev.payload))

    async def start(self) -> None:
        self.scheduler.start()
        await self._load_modules()
        await self.ipc.start()
        self.bus.publish(source="engine", type="engine.started", payload={"pid": os.getpid()})

    async def stop(self) -> None:
        try:
            self.bus.publish(source="engine", type="engine.stopping", payload={})
        except Exception:  # noqa: BLE001
            pass
        await self.ipc.stop()
        await self.scheduler.stop()
        self.state.close()

    def request_shutdown(self, reason: Optional[dict[str, Any]] = None) -> None:
        self._shutdown.set()
        try:
            self.bus.publish(source="engine", type="engine.shutdown.set", payload={"reason": reason or {}})
        except Exception:  # noqa: BLE001
            pass

    async def run_forever(self, *, run_for: Optional[float] = None) -> None:
        async def _heartbeat_loop() -> None:
            while not self._shutdown.is_set():
                uptime = time.time() - self.started_at
                self.state.set_system_state(
                    "engine.heartbeat",
                    {
                        "ts": time.time(),
                        "uptime_sec": uptime,
                        "pid": os.getpid(),
                        "modules": [m.name for m in self._modules],
                    },
                )
                await asyncio.sleep(self.cfg.heartbeat_interval_sec)

        hb_task = asyncio.create_task(_heartbeat_loop(), name="obeos.engine.heartbeat")
        try:
            if run_for is None:
                await self._shutdown.wait()
            else:
                try:
                    await asyncio.wait_for(self._shutdown.wait(), timeout=run_for)
                except asyncio.TimeoutError:
                    self.request_shutdown(reason={"run_for_timeout": run_for})
        finally:
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass

    async def _load_modules(self) -> None:
        modules_dir = self.cfg.root / "modules"
        loaded: list[LoadedModule] = []
        for name in self.cfg.enabled_modules:
            mod_path = modules_dir / name / "module.py"
            if not mod_path.exists():
                raise RuntimeError(f"Enabled module missing: {name} ({mod_path})")
            lm = load_module_from_path(name, mod_path)
            lm.instance.register(self.bus, self.state)
            loaded.append(lm)
            self.bus.publish(source="engine", type="module.loaded", payload={"name": lm.name})
        self._modules = loaded

    def _find_module_for_command(self, command: str) -> Optional[LoadedModule]:
        for m in self._modules:
            cmds = getattr(m.instance, "commands", []) or []
            if command == m.name or command in cmds:
                return m
        return None

    async def _handle_ipc_request(self, req: dict[str, Any], ident: Identity) -> dict[str, Any]:
        request_id = req.get("request_id")
        command = req.get("command")
        args = req.get("args", [])

        if not isinstance(command, str) or not command:
            return {"ok": False, "request_id": request_id, "error": "missing_command"}
        if not isinstance(args, list):
            return {"ok": False, "request_id": request_id, "error": "invalid_args"}

        try:
            self.perms.assert_allowed(ident, command)
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "request_id": request_id, "error": f"permission_denied: {e}"}

        m = self._find_module_for_command(command)
        if m is None:
            return {"ok": False, "request_id": request_id, "error": f"unknown_command: {command}"}

        payload = {"args": args, "identity": {"uid": ident.uid, "gid": ident.gid, "pid": ident.pid}}
        try:
            result = await m.instance.handle(command, payload)
        except Exception as e:  # noqa: BLE001
            self.bus.publish(
                source="engine",
                type="command.error",
                payload={"command": command, "args": args, "error": str(e)},
            )
            return {"ok": False, "request_id": request_id, "error": str(e)}

        # Module may return a job_id (int) or {"job_id": int, ...}
        job_id: Optional[int] = None
        if isinstance(result, int):
            job_id = result
        elif isinstance(result, dict) and isinstance(result.get("job_id"), int):
            job_id = int(result["job_id"])

        if job_id is not None and hasattr(m.instance, "run_job"):
            async def _run() -> None:
                try:
                    await getattr(m.instance, "run_job")(job_id, payload)  # type: ignore[misc]
                except Exception as e:  # noqa: BLE001
                    try:
                        self.state.update_job(job_id, status="failed", error=str(e))
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        self.bus.publish(
                            source=f"module:{m.name}",
                            type="job.failed",
                            payload={"job_id": job_id, "error": str(e)},
                        )
                    except Exception:  # noqa: BLE001
                        pass

            await self.scheduler.enqueue(job_id, _run)

        # Default response shape
        return {"ok": True, "request_id": request_id, "result": result}


def main() -> int:
    ap = argparse.ArgumentParser(description="OBEOS blackfong-engine (v0.1)")
    ap.add_argument("--check", action="store_true", help="Initialize and validate config/modules then exit")
    ap.add_argument(
        "--run-for",
        type=float,
        default=None,
        help="Development/testing only: run engine loop for N seconds then exit",
    )
    args = ap.parse_args()

    root = resolve_root()
    cfg = load_config(root)

    async def _run() -> None:
        eng = Engine(cfg)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, eng.request_shutdown, {"signal": sig.name})
            except NotImplementedError:
                pass

        await eng.start()
        if args.check:
            eng.request_shutdown(reason={"check": True})
        await eng.run_forever(run_for=args.run_for)
        await eng.stop()

    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

