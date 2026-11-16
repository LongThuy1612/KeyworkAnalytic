"""Durable job queue on the service's SQLite file (WAL) -- same idea as
topai-imageproxy's retry_queue.db, extended with leases.

One row per (domain, tool). Single tool here (`keyword`); the table shape is shared with SimilarAnalytic. Original docstring: tools are
`ext` (browser extension) and `pro` (logged-in Pro accounts). Jobs come from two producers:

  api   POST /analyze (explicit list of domains, can jump the queue)
  auto  the feeder, topping up from the database candidates

The farm loops never see this table directly: their `db.get_next_domains()`
is a thin wrapper over claim(), and traffic_db.save_success()/save_miss()
call finish(), so the loops keep their pacing/proxy/profile logic unchanged.

status: pending -> running (lease) -> done | failed | skipped (another tool finished the domain)
"""

import json
import time

from app import config
from app.store import _db, _lock

TOOLS = ("keyword",)


def _init() -> None:
    with _lock:
        db = _db()
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                tool TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                source TEXT NOT NULL DEFAULT 'auto',
                priority INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                lease_until INTEGER,
                result TEXT,
                error TEXT,
                created_at INTEGER NOT NULL,
                finished_at INTEGER,
                UNIQUE(domain, tool)
            );
            CREATE INDEX IF NOT EXISTS jobs_claim ON jobs(tool, status, priority DESC, id);
            CREATE INDEX IF NOT EXISTS jobs_domain ON jobs(domain, status);
            """
        )
        db.commit()


_ready = False


def _ensure() -> None:
    global _ready
    if not _ready:
        _init()
        _ready = True


def enqueue(domains: list[str], tools: list[str], source: str = "auto", priority: int = 0) -> int:
    """Adds jobs, returns how many were newly queued or re-queued. An existing
    pending/running job is left alone. A finished one is re-queued when asked
    by the API, or by the feeder once REQUEUE_AFTER_S has passed."""
    _ensure()
    now = int(time.time())
    min_age = 0 if source == "api" else config.REQUEUE_AFTER_S
    changed = 0
    with _lock:
        db = _db()
        for tool in tools:
            for domain in domains:
                cur = db.execute(
                    """
                    INSERT INTO jobs (domain, tool, source, priority, created_at) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(domain, tool) DO UPDATE SET
                        status = 'pending', source = excluded.source, priority = excluded.priority,
                        attempts = 0, lease_until = NULL, error = NULL, created_at = excluded.created_at
                    WHERE jobs.status IN ('done', 'failed', 'skipped') AND COALESCE(jobs.finished_at, 0) <= ?
                    """,
                    (domain, tool, source, priority, now, now - min_age),
                )
                changed += cur.rowcount
        db.commit()
    return changed


def claim(tool: str, limit: int) -> list[str]:
    """Moves up to `limit` pending jobs of `tool` to running and returns their
    domains, highest priority first. A domain already running under another
    tool is skipped so two farms never work the same domain."""
    _ensure()
    now = int(time.time())
    with _lock:
        db = _db()
        # Auto-fed jobs whose domain another tool already finished are closed
        # rather than left pending (they would inflate the queue depth the
        # feeder looks at and never be claimed).
        db.execute(
            """
            UPDATE jobs SET status = 'skipped', error = 'done by another tool', finished_at = ?
            WHERE tool = ? AND status = 'pending' AND source = 'auto' AND domain IN (
                SELECT domain FROM jobs WHERE tool != ? AND status IN ('done', 'failed') AND finished_at > ?)
            """,
            (now, tool, tool, now - config.REQUEUE_AFTER_S),
        )
        rows = db.execute(
            """
            SELECT id, domain FROM jobs
            WHERE tool = ? AND status = 'pending'
              AND domain NOT IN (SELECT domain FROM jobs WHERE status = 'running')
              AND (source = 'api' OR domain NOT IN (
                    SELECT domain FROM jobs
                    WHERE tool != ? AND status IN ('done', 'failed') AND finished_at > ?))
            ORDER BY priority DESC, id LIMIT ?
            """,
            (tool, tool, now - config.REQUEUE_AFTER_S, limit),
        ).fetchall()
        db.executemany(
            "UPDATE jobs SET status = 'running', lease_until = ? WHERE id = ?",
            [(now + config.JOB_LEASE_S, r[0]) for r in rows],
        )
        db.commit()
    return [r[1] for r in rows]


def finish(domain: str, ok: bool, result: dict | None = None, error: str | None = None) -> int:
    """Closes the running job(s) of `domain`. Called by traffic_db after the
    outcome is durable in the outbox (success -> done, miss -> failed)."""
    _ensure()
    now = int(time.time())
    with _lock:
        db = _db()
        cur = db.execute(
            "UPDATE jobs SET status = ?, result = ?, error = ?, finished_at = ?, lease_until = NULL "
            "WHERE domain = ? AND status = 'running'",
            ("done" if ok else "failed", json.dumps(result, default=str) if result else None, (error or None) and error[:255], now, domain),
        )
        db.commit()
        return cur.rowcount


def reclaim_stale() -> int:
    """Expired leases go back to pending; after JOB_MAX_ATTEMPTS they fail."""
    _ensure()
    now = int(time.time())
    with _lock:
        db = _db()
        db.execute(
            "UPDATE jobs SET status = 'failed', error = 'lease expired too often', finished_at = ?, lease_until = NULL "
            "WHERE status = 'running' AND lease_until < ? AND attempts + 1 >= ?",
            (now, now, config.JOB_MAX_ATTEMPTS),
        )
        cur = db.execute(
            "UPDATE jobs SET status = 'pending', attempts = attempts + 1, lease_until = NULL "
            "WHERE status = 'running' AND lease_until < ?",
            (now,),
        )
        db.commit()
        return cur.rowcount


def pending_count(tool: str | None = None) -> int:
    _ensure()
    with _lock:
        if tool:
            return _db().execute("SELECT COUNT(*) FROM jobs WHERE status = 'pending' AND tool = ?", (tool,)).fetchone()[0]
        return _db().execute("SELECT COUNT(*) FROM jobs WHERE status = 'pending'").fetchone()[0]


def counts() -> dict:
    _ensure()
    with _lock:
        rows = _db().execute("SELECT tool, status, COUNT(*) FROM jobs GROUP BY tool, status").fetchall()
    out: dict = {}
    for tool, status, n in rows:
        out.setdefault(tool, {})[status] = n
    return out


def list_jobs(status: str | None = None, tool: str | None = None, limit: int = 50) -> list[dict]:
    _ensure()
    q, args = "SELECT domain, tool, status, source, priority, attempts, result, error, created_at, finished_at FROM jobs WHERE 1=1", []
    if status:
        q += " AND status = ?"
        args.append(status)
    if tool:
        q += " AND tool = ?"
        args.append(tool)
    q += " ORDER BY id DESC LIMIT ?"
    args.append(min(limit, 500))
    with _lock:
        rows = _db().execute(q, args).fetchall()
    keys = ["domain", "tool", "status", "source", "priority", "attempts", "result", "error", "createdAt", "finishedAt"]
    out = []
    for r in rows:
        d = dict(zip(keys, r))
        d["result"] = json.loads(d["result"]) if d["result"] else None
        out.append(d)
    return out


def release(domains: list[str]) -> int:
    """Hands claimed-but-untouched domains back to the queue (no attempt
    counted): a farm session that ends early must not strand its claims until
    the lease expires."""
    if not domains:
        return 0
    _ensure()
    with _lock:
        db = _db()
        cur = db.executemany(
            "UPDATE jobs SET status = 'pending', lease_until = NULL WHERE domain = ? AND status = 'running'",
            [(d,) for d in domains],
        )
        db.commit()
        return cur.rowcount
