"""Where step results live between attempts.

A store keeps one row per (run_id, step_key). The first completed result
wins: ``put`` never overwrites, so two workers that race on the same step
can't replace each other's stored result.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class StoredResult:
    """One completed step, as a store keeps it."""

    run_id: str
    step_key: str
    step_name: str
    value: str
    """The step's return value, JSON-encoded."""
    created_at: float
    """Unix timestamp of when the result was stored."""


@runtime_checkable
class Store(Protocol):
    """Anything that can keep step results. Implement these four methods to
    back durastep with another database."""

    def get(self, run_id: str, step_key: str) -> StoredResult | None: ...

    def put(self, result: StoredResult) -> None:
        """Store ``result`` unless a result for the same key already exists."""
        ...

    def list_run(self, run_id: str) -> list[StoredResult]:
        """Every stored step in the run, oldest first."""
        ...

    def clear_run(self, run_id: str) -> int:
        """Forget a run's steps. Returns how many were removed."""
        ...


class MemoryStore:
    """A store that lives in this process only. Good for tests; results are
    gone when the process exits."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], StoredResult] = {}
        self._lock = threading.Lock()

    def get(self, run_id: str, step_key: str) -> StoredResult | None:
        with self._lock:
            return self._rows.get((run_id, step_key))

    def put(self, result: StoredResult) -> None:
        with self._lock:
            self._rows.setdefault((result.run_id, result.step_key), result)

    def list_run(self, run_id: str) -> list[StoredResult]:
        with self._lock:
            rows = [r for (rid, _), r in self._rows.items() if rid == run_id]
        return sorted(rows, key=lambda r: r.created_at)

    def clear_run(self, run_id: str) -> int:
        with self._lock:
            keys = [k for k in self._rows if k[0] == run_id]
            for k in keys:
                del self._rows[k]
            return len(keys)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS durastep_steps (
    run_id     TEXT NOT NULL,
    step_key   TEXT NOT NULL,
    step_name  TEXT NOT NULL,
    value      TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (run_id, step_key)
)
"""


class SQLiteStore:
    """A store in a SQLite file, safe to share between threads and processes.

    Each thread gets its own connection. The database runs in WAL mode, so
    readers don't block the writer, and every write is ``INSERT OR IGNORE``
    against the ``(run_id, step_key)`` primary key: the first completed
    result is the one that's kept.

    Use a file path. ``":memory:"`` would give every thread its own empty
    database; use :class:`MemoryStore` for that instead.
    """

    def __init__(self, path: str | Path = "durastep.db", *, timeout: float = 30.0) -> None:
        self.path = str(path)
        self.timeout = timeout
        self._local = threading.local()

    def _conn(self) -> sqlite3.Connection:
        conn: sqlite3.Connection | None = getattr(self._local, "conn", None)
        if conn is None:
            # Autocommit: each statement is its own transaction, so a stored
            # result is durable as soon as put() returns.
            conn = sqlite3.connect(self.path, timeout=self.timeout, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(_SCHEMA)
            self._local.conn = conn
        return conn

    def get(self, run_id: str, step_key: str) -> StoredResult | None:
        row = self._conn().execute(
            "SELECT run_id, step_key, step_name, value, created_at FROM durastep_steps"
            " WHERE run_id = ? AND step_key = ?",
            (run_id, step_key),
        ).fetchone()
        return StoredResult(*row) if row else None

    def put(self, result: StoredResult) -> None:
        self._conn().execute(
            "INSERT OR IGNORE INTO durastep_steps (run_id, step_key, step_name, value, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (result.run_id, result.step_key, result.step_name, result.value, result.created_at),
        )

    def list_run(self, run_id: str) -> list[StoredResult]:
        rows = self._conn().execute(
            "SELECT run_id, step_key, step_name, value, created_at FROM durastep_steps"
            " WHERE run_id = ? ORDER BY created_at, rowid",
            (run_id,),
        ).fetchall()
        return [StoredResult(*r) for r in rows]

    def clear_run(self, run_id: str) -> int:
        cur = self._conn().execute("DELETE FROM durastep_steps WHERE run_id = ?", (run_id,))
        return cur.rowcount

    def close(self) -> None:
        """Close this thread's connection. Other threads keep theirs."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
