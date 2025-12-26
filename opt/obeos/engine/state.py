from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def _json_loads(s: str) -> Any:
    return json.loads(s)


@dataclass(frozen=True)
class Event:
    id: int
    timestamp: float
    source: str
    type: str
    payload: dict[str, Any]


class StateStore:
    """
    Authoritative persistence for OBEOS.

    Notes:
      - Single SQLite DB.
      - Append-only mindset for events.
      - Thread-safe via a coarse lock; engine uses a single asyncio loop,
        but IPC handlers may interleave reads/writes.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(os.path.dirname(db_path) or ".").mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_db(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS system_state (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL,
                  updated_at REAL NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at REAL NOT NULL,
                  updated_at REAL NOT NULL,
                  status TEXT NOT NULL,
                  command TEXT NOT NULL,
                  args TEXT NOT NULL,
                  owner TEXT,
                  result TEXT,
                  error TEXT
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts REAL NOT NULL,
                  source TEXT NOT NULL,
                  type TEXT NOT NULL,
                  payload TEXT NOT NULL
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS artifacts (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at REAL NOT NULL,
                  type TEXT NOT NULL,
                  metadata TEXT NOT NULL,
                  path TEXT
                );
                """
            )
            self._conn.commit()

    # ---- system_state ----
    def set_system_state(self, key: str, value: Any) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO system_state(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  value=excluded.value,
                  updated_at=excluded.updated_at;
                """,
                (key, _json_dumps(value), now),
            )
            self._conn.commit()

    def get_system_state(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._conn.execute("SELECT value FROM system_state WHERE key=?;", (key,)).fetchone()
            if not row:
                return default
            return _json_loads(row["value"])

    def get_system_state_updated_at(self, key: str) -> Optional[float]:
        with self._lock:
            row = self._conn.execute(
                "SELECT updated_at FROM system_state WHERE key=?;", (key,)
            ).fetchone()
            if not row:
                return None
            return float(row["updated_at"])

    # ---- jobs ----
    def create_job(self, command: str, args: list[Any], owner: Optional[str] = None) -> int:
        now = time.time()
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO jobs(created_at, updated_at, status, command, args, owner)
                VALUES(?, ?, ?, ?, ?, ?);
                """,
                (now, now, "queued", command, _json_dumps(args), owner),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def update_job(
        self,
        job_id: int,
        *,
        status: Optional[str] = None,
        result: Any = None,
        error: Optional[str] = None,
    ) -> None:
        now = time.time()
        fields: list[str] = ["updated_at=?"]
        params: list[Any] = [now]
        if status is not None:
            fields.append("status=?")
            params.append(status)
        if result is not None:
            fields.append("result=?")
            params.append(_json_dumps(result))
        if error is not None:
            fields.append("error=?")
            params.append(error)
        params.append(job_id)
        with self._lock:
            self._conn.execute(f"UPDATE jobs SET {', '.join(fields)} WHERE id=?;", tuple(params))
            self._conn.commit()

    def get_job(self, job_id: int) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM jobs WHERE id=?;", (job_id,)).fetchone()
            if not row:
                return None
            return self._row_to_job(row)

    def list_jobs(self, limit: int = 50, status: Optional[str] = None) -> list[dict[str, Any]]:
        with self._lock:
            if status:
                rows = self._conn.execute(
                    "SELECT * FROM jobs WHERE status=? ORDER BY id DESC LIMIT ?;", (status, limit)
                ).fetchall()
            else:
                rows = self._conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?;", (limit,)).fetchall()
            return [self._row_to_job(r) for r in rows]

    def _row_to_job(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "created_at": float(row["created_at"]),
            "updated_at": float(row["updated_at"]),
            "status": str(row["status"]),
            "command": str(row["command"]),
            "args": _json_loads(row["args"]),
            "owner": row["owner"],
            "result": _json_loads(row["result"]) if row["result"] else None,
            "error": row["error"],
        }

    # ---- events ----
    def append_event(self, timestamp: float, source: str, type_: str, payload: dict[str, Any]) -> Event:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO events(ts, source, type, payload) VALUES(?, ?, ?, ?);",
                (timestamp, source, type_, _json_dumps(payload)),
            )
            self._conn.commit()
            event_id = int(cur.lastrowid)
        return Event(id=event_id, timestamp=timestamp, source=source, type=type_, payload=payload)

    def list_events(
        self,
        *,
        since_ts: Optional[float] = None,
        limit: int = 200,
        type_: Optional[str] = None,
    ) -> list[Event]:
        clauses: list[str] = []
        params: list[Any] = []
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(float(since_ts))
        if type_ is not None:
            clauses.append("type = ?")
            params.append(type_)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM events {where} ORDER BY id DESC LIMIT ?;"
        params.append(int(limit))
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        out: list[Event] = []
        for r in rows:
            out.append(
                Event(
                    id=int(r["id"]),
                    timestamp=float(r["ts"]),
                    source=str(r["source"]),
                    type=str(r["type"]),
                    payload=_json_loads(r["payload"]),
                )
            )
        return out

    def stats(self) -> dict[str, Any]:
        with self._lock:
            jobs = int(self._conn.execute("SELECT COUNT(*) AS c FROM jobs;").fetchone()["c"])
            events = int(self._conn.execute("SELECT COUNT(*) AS c FROM events;").fetchone()["c"])
        return {"jobs": jobs, "events": events, "db_path": self.db_path}
