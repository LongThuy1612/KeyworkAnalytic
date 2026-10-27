"""Local SQLite state (WAL), same idea as topai-imageproxy's retry_queue.db.

Two tables, both surviving restarts and MySQL outages:

  outbox    results waiting to be written to MySQL. The farm never blocks on,
            or loses a lookup to, a MySQL hiccup: it appends here and a
            flusher drains it whenever MySQL answers.
  attempts  per-domain ledger of lookups that produced no row (Keyword Planner
            had no data), so the same domain is not re-picked every session
            until NO_DATA_RETRY_AFTER_DAYS has passed.
"""

import json
import os
import sqlite3
import threading
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.environ.get("STATE_DB_PATH", os.path.join(_ROOT, "data", "state.sqlite"))
NO_DATA_RETRY_AFTER_DAYS = int(os.environ.get("NO_DATA_RETRY_AFTER_DAYS", "14"))

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
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS attempts (
                domain TEXT PRIMARY KEY,
                last_attempt_at INTEGER NOT NULL,
                misses INTEGER NOT NULL DEFAULT 1
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


def outbox_peek(limit: int = 50) -> list[tuple[int, list]]:
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


def record_miss(domains: list[str]) -> None:
    now = int(time.time())
    with _lock:
        db = _db()
        db.executemany(
            "INSERT INTO attempts (domain, last_attempt_at) VALUES (?, ?) "
            "ON CONFLICT(domain) DO UPDATE SET last_attempt_at = excluded.last_attempt_at, misses = misses + 1",
            [(d, now) for d in domains],
        )
        db.commit()


def clear_attempts(domains: list[str]) -> None:
    with _lock:
        db = _db()
        db.executemany("DELETE FROM attempts WHERE domain = ?", [(d,) for d in domains])
        db.commit()


def recently_missed(domains: list[str]) -> set[str]:
    if not domains:
        return set()
    cutoff = int(time.time()) - NO_DATA_RETRY_AFTER_DAYS * 86400
    out: set[str] = set()
    with _lock:
        db = _db()
        for i in range(0, len(domains), 500):
            chunk = domains[i : i + 500]
            marks = ",".join("?" * len(chunk))
            out.update(
                r[0]
                for r in db.execute(
                    f"SELECT domain FROM attempts WHERE last_attempt_at > ? AND domain IN ({marks})",
                    [cutoff, *chunk],
                )
            )
    return out
