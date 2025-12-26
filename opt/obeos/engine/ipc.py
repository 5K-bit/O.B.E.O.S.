from __future__ import annotations

import asyncio
import json
import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from permissions import Identity


RequestHandler = Callable[[dict[str, Any], Identity], Awaitable[dict[str, Any]]]
SubscribeHandler = Callable[[dict[str, Any], Identity, asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


@dataclass(frozen=True)
class IPCConfig:
    socket_path: str


class IPCServer:
    def __init__(self, cfg: IPCConfig, handler: RequestHandler, subscribe_handler: SubscribeHandler) -> None:
        self._cfg = cfg
        self._handler = handler
        self._subscribe_handler = subscribe_handler
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        sock_path = self._cfg.socket_path
        Path(os.path.dirname(sock_path) or ".").mkdir(parents=True, exist_ok=True)
        try:
            os.unlink(sock_path)
        except FileNotFoundError:
            pass

        self._server = await asyncio.start_unix_server(self._on_client, path=sock_path)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _on_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        ident = self._get_peer_identity(writer)
        try:
            # Protocol: newline-delimited JSON objects.
            # Two modes:
            #   - request/response (default): one request -> one response -> close
            #   - subscription: command == "subscribe" -> keep connection open and stream events
            line = await reader.readline()
            if not line:
                return
            try:
                req = json.loads(line.decode("utf-8"))
            except Exception as e:  # noqa: BLE001
                await self._write(writer, {"ok": False, "error": f"invalid_json: {e}"})
                return

            if not isinstance(req, dict):
                await self._write(writer, {"ok": False, "error": "invalid_request: expected object"})
                return

            if req.get("command") == "subscribe":
                await self._subscribe_handler(req, ident, reader, writer)
                return

            resp = await self._handler(req, ident)
            await self._write(writer, resp)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass

    async def _write(self, writer: asyncio.StreamWriter, obj: dict[str, Any]) -> None:
        data = (json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
        writer.write(data)
        await writer.drain()

    def _get_peer_identity(self, writer: asyncio.StreamWriter) -> Identity:
        sock: Optional[socket.socket] = writer.get_extra_info("socket")
        if sock is None:
            return Identity()

        # Linux: SO_PEERCRED gives (pid, uid, gid)
        try:
            creds = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
            pid = int.from_bytes(creds[0:4], "little", signed=True)
            uid = int.from_bytes(creds[4:8], "little", signed=True)
            gid = int.from_bytes(creds[8:12], "little", signed=True)
            return Identity(uid=uid, gid=gid, pid=pid)
        except Exception:  # noqa: BLE001
            return Identity()
