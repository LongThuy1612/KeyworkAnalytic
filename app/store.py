"""Local SQLite state (WAL), same idea as topai-imageproxy's retry_queue.db.

This file is the service's own storage, so the crawl DB is read and written as
little as possible:

  jobs    the work queue (see app/jobqueue.py) -- what is pending / running /
          done / failed is answered here, never by asking MySQL.
  outbox  results waiting to be written to MySQL in one batch. The farm never
          blocks on, or loses a lookup to, a MySQL hiccup.
  kv      small state: the feeder's keyset cursor and pull timers.
"""

import json
import os
import sqlite3
import threading
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.environ.get("STATE_DB_PATH", os.path.join(_ROOT, "data", "state.sqlite"))

_lock = threading.Lock()
_conn: sqlite3.Connection | None = None


def _db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, timeout=15, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        _conn.commit()
    return _conn


def outbox_add(params_list: list[list]) -> None:
    now = int(time.time())
    with _lock:
        db = _db()
        db.executemany(
            "INSERT INTO outbox (payload, created_at) VALUES (?, ?)",
            [(json.dumps(p, default=str), now) for p in params_list],
        )
        db.commit()


def outbox_peek(limit: int = 200) -> list[tuple[int, list]]:
    with _lock:
        rows = _db().execute("SELECT id, payload FROM outbox ORDER BY id LIMIT ?", (limit,)).fetchall()
    return [(r[0], json.loads(r[1])) for r in rows]


def outbox_done(ids: list[int]) -> None:
    if not ids:
        return
    with _lock:
        db = _db()
        db.executemany("DELETE FROM outbox WHERE id = ?", [(i,) for i in ids])
        db.commit()


def outbox_size() -> int:
    with _lock:
        return _db().execute("SELECT COUNT(*) FROM outbox").fetchone()[0]


def outbox_oldest_age() -> float:
    with _lock:
        row = _db().execute("SELECT MIN(created_at) FROM outbox").fetchone()
    return time.time() - row[0] if row and row[0] else 0.0


def outbox_domains() -> set[str]:
    """Domains whose result is still waiting to be written (payload[0])."""
    with _lock:
        rows = _db().execute("SELECT payload FROM outbox").fetchall()
    return {json.loads(r[0])[0] for r in rows}


def kv_get(key: str, default: str | None = None) -> str | None:
    with _lock:
        row = _db().execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def kv_set(key: str, value: str | None) -> None:
    with _lock:
        db = _db()
        if value is None:
            db.execute("DELETE FROM kv WHERE key = ?", (key,))
        else:
            db.execute(
                "INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        db.commit()
