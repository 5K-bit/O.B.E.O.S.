#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import socket
import sys
import time
from typing import Any


def _sock_path() -> str:
    return os.environ.get("OBEOS_SOCKET", "/run/obeos.sock")


def _call_engine(command: str, args: list[Any]) -> dict[str, Any]:
    req = {"command": command, "args": args}
    data = (json.dumps(req, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(_sock_path())
        s.sendall(data)
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
        line = buf.split(b"\n", 1)[0].decode("utf-8", errors="replace")
        return json.loads(line) if line else {"ok": False, "error": "no_response"}


def main() -> int:
    try:
        import gi  # type: ignore

        gi.require_version("Gtk", "3.0")
        from gi.repository import GLib, Gtk  # type: ignore
    except Exception as e:  # noqa: BLE001
        sys.stderr.write(
            "GTK console is optional. Gtk not available in this environment.\n"
            f"Error: {e}\n"
        )
        return 2

    win = Gtk.Window(title="OBEOS Console (v0.1)")
    win.set_default_size(700, 300)

    vbox = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=8)
    vbox.set_border_width(12)
    win.add(vbox)

    status = Gtk.Label(label="Connecting…")
    status.set_xalign(0.0)
    vbox.pack_start(status, False, False, 0)

    logs = Gtk.TextView()
    logs.set_editable(False)
    logs.set_monospace(True)
    buf = logs.get_buffer()
    vbox.pack_start(logs, True, True, 0)

    def refresh() -> bool:
        try:
            resp = _call_engine("status", [])
            if resp.get("ok"):
                hb = (resp.get("result") or {}).get("heartbeat") or {}
                status.set_text(
                    f"Engine PID {hb.get('pid')} • uptime {hb.get('uptime_sec', 0):.1f}s • last {time.ctime(hb.get('ts', time.time()))}"
                )
            else:
                status.set_text(f"Engine error: {resp.get('error')}")

            evs = _call_engine("logs", ["last", "10"]).get("result", {}).get("events", [])
            text = "\n".join(
                f"{e.get('id'):>6} {time.strftime('%H:%M:%S', time.localtime(e.get('timestamp', 0)))} "
                f"{e.get('type')} {e.get('source')} {json.dumps(e.get('payload', {}), separators=(',', ':'))}"
                for e in evs
            )
            buf.set_text(text)
        except Exception as e:  # noqa: BLE001
            status.set_text(f"Disconnected: {e}")
        return True

    GLib.timeout_add(1000, refresh)
    refresh()

    win.connect("destroy", Gtk.main_quit)
    win.show_all()
    Gtk.main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

