#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import socket
import sys
import threading
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

def _subscribe(patterns: list[str] | None = None, *, last_event_id: int | None = None) -> tuple[socket.socket, Any]:
    req: dict[str, Any] = {"command": "subscribe", "args": patterns or ["*"]}
    if last_event_id is not None:
        req["last_event_id"] = int(last_event_id)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(_sock_path())
    s.sendall((json.dumps(req, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8"))
    f = s.makefile("r", encoding="utf-8", newline="\n")
    return s, f


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

    # Prefer subscription streaming. If it fails, fall back to periodic polling.
    state = {"lines": [], "last_event_id": None}

    def render() -> None:
        buf.set_text("\n".join(state["lines"][-200:]))

    def set_status_from_hb(hb: dict[str, Any]) -> None:
        status.set_text(
            f"Engine PID {hb.get('pid')} • uptime {hb.get('uptime_sec', 0):.1f}s • last {time.ctime(hb.get('ts', time.time()))}"
        )

    def start_subscription() -> bool:
        try:
            s, f = _subscribe(["engine.*", "job.*", "module.*", "client.*"], last_event_id=state["last_event_id"])
            first = f.readline()
            ack = json.loads(first) if first else {"ok": False}
            if not ack.get("ok") or ack.get("type") != "subscribed":
                s.close()
                return False

            def run() -> None:
                try:
                    for line in f:
                        try:
                            msg = json.loads(line)
                        except Exception:
                            continue
                        if msg.get("type") != "event":
                            continue
                        ev = msg.get("event") or {}
                        ev_id = ev.get("id")
                        if isinstance(ev_id, int):
                            state["last_event_id"] = ev_id
                        ev_type = ev.get("type", "")
                        if ev_type == "engine.heartbeat":
                            hb = ev.get("payload") or {}
                            GLib.idle_add(set_status_from_hb, hb)
                        # Append a readable log line
                        ts = time.strftime("%H:%M:%S", time.localtime(ev.get("timestamp", 0)))
                        line_txt = (
                            f"{ev.get('id', 0):>6} {ts} {ev.get('type')} {ev.get('source')} "
                            f"{json.dumps(ev.get('payload', {}), separators=(',', ':'))}"
                        )
                        state["lines"].append(line_txt)
                        GLib.idle_add(render)
                except Exception as e:  # noqa: BLE001
                    GLib.idle_add(status.set_text, f"Disconnected: {e}")
                finally:
                    try:
                        s.close()
                    except Exception:
                        pass

            t = threading.Thread(target=run, daemon=True)
            t.start()
            status.set_text("Subscribed (streaming)…")
            return True
        except Exception:
            return False

    def refresh_polling() -> bool:
        try:
            resp = _call_engine("status", [])
            if resp.get("ok"):
                hb = (resp.get("result") or {}).get("heartbeat") or {}
                set_status_from_hb(hb)
            else:
                status.set_text(f"Engine error: {resp.get('error')}")

            evs = _call_engine("logs", ["last", "10"]).get("result", {}).get("events", [])
            state["lines"] = [
                f"{e.get('id'):>6} {time.strftime('%H:%M:%S', time.localtime(e.get('timestamp', 0)))} "
                f"{e.get('type')} {e.get('source')} {json.dumps(e.get('payload', {}), separators=(',', ':'))}"
                for e in evs
            ]
            render()
        except Exception as e:  # noqa: BLE001
            status.set_text(f"Disconnected: {e}")
        return True

    if not start_subscription():
        status.set_text("Subscription failed; polling…")
        GLib.timeout_add(1000, refresh_polling)
        refresh_polling()

    win.connect("destroy", Gtk.main_quit)
    win.show_all()
    Gtk.main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

