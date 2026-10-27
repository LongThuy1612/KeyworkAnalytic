"""Lean MySQL access for the shared crawl database.

Same contract as CrawlData's src/db/connection.py (fetch_all / execute) but
sized for a single-worker service: a small pool, a liveness ping on every
acquire (a pooled socket can die silently), a hard per-query timeout, and a
bounded retry on pure connectivity errors. The service never issues DDL --
tables are owned by BEWeb's Prisma migrations.
"""

import asyncio
import os
import re
from urllib.parse import unquote

import aiomysql
import pymysql

_QUERY_TIMEOUT_S = float(os.environ.get("DB_QUERY_TIMEOUT_S", "20"))
_PING_TIMEOUT_S = 3.0
_RETRIES = int(os.environ.get("DB_CALL_RETRIES", "2"))
_RETRY_BACKOFF_S = float(os.environ.get("DB_CALL_RETRY_BACKOFF_S", "2"))
_POOL_MAX = int(os.environ.get("DB_POOL_MAX", "4"))
_CONNECTIVITY_CODES = {2003, 2006, 2013, 1040}

_URL_RE = re.compile(r"^mysql://(?P<user>[^:]+):(?P<password>[^@]*)@(?P<host>[^:/]+)(?::(?P<port>\d+))?/(?P<db>[^?]+)")

_pool: aiomysql.Pool | None = None
_pool_lock = asyncio.Lock()


def is_connectivity_error(err: BaseException | None) -> bool:
    seen: set[int] = set()
    while err is not None and id(err) not in seen:
        seen.add(id(err))
        if isinstance(err, (asyncio.TimeoutError, ConnectionError)):
            return True
        if isinstance(err, pymysql.err.OperationalError) and err.args and err.args[0] in _CONNECTIVITY_CODES:
            return True
        err = err.__cause__ or err.__context__
    return False


async def _get_pool() -> aiomysql.Pool:
    global _pool
    if _pool is not None:
        return _pool
    async with _pool_lock:
        if _pool is None:
            # Strip stray quotes: a plain `docker run --env-file` keeps them.
            m = _URL_RE.match(os.environ["DATABASE_URL"].strip().strip("\"'"))
            if not m:
                raise ValueError("DATABASE_URL must look like mysql://user:pass@host:port/db")
            _pool = await aiomysql.create_pool(
                host=m["host"],
                port=int(m["port"] or 3306),
                user=unquote(m["user"]),
                password=unquote(m["password"]),
                db=m["db"],
                minsize=1,
                maxsize=_POOL_MAX,
                autocommit=True,
                charset="utf8mb4",
                connect_timeout=10,
                pool_recycle=1800,
                cursorclass=aiomysql.DictCursor,
            )
    return _pool


async def _acquire():
    """Returns a verified-alive connection. A failed ping closes the socket
    AND releases the slot -- skipping the release leaks one pool slot per
    dead connection (the bug that pinned CrawlData's pool at 80/80)."""
    pool = await _get_pool()
    for _ in range(3):
        conn = await asyncio.wait_for(pool.acquire(), timeout=_QUERY_TIMEOUT_S)
        try:
            await asyncio.wait_for(conn.ping(reconnect=True), timeout=_PING_TIMEOUT_S)
            return pool, conn
        except Exception:  # noqa: BLE001 -- any ping failure means this socket is unusable
            conn.close()
            pool.release(conn)
    raise ConnectionError("no live MySQL connection after 3 pings")


async def _run(sql: str, params: tuple, fetch: bool):
    pool, conn = await _acquire()
    try:
        async with conn.cursor() as cur:
            await asyncio.wait_for(cur.execute(sql, params), timeout=_QUERY_TIMEOUT_S)
            return await cur.fetchall() if fetch else cur.rowcount
    finally:
        pool.release(conn)


async def _with_retry(sql: str, params: tuple, fetch: bool):
    attempt = 0
    while True:
        try:
            return await _run(sql, params, fetch)
        except Exception as err:  # noqa: BLE001
            if attempt >= _RETRIES or not is_connectivity_error(err):
                raise
            attempt += 1
            await asyncio.sleep(_RETRY_BACKOFF_S * attempt)


async def _run_many(sql: str, rows: list) -> int:
    pool, conn = await _acquire()
    try:
        await conn.begin()
        async with conn.cursor() as cur:
            await asyncio.wait_for(cur.executemany(sql, rows), timeout=_QUERY_TIMEOUT_S * 3)
        await conn.commit()
        return len(rows)
    except BaseException:
        conn.close()  # never hand back a connection stuck inside a transaction
        raise
    finally:
        pool.release(conn)


async def executemany(sql: str, rows: list) -> int:
    """One connection, one transaction for the whole batch -- the point of the
    local outbox is to touch the crawl DB about once a minute, not once per row."""
    if not rows:
        return 0
    attempt = 0
    while True:
        try:
            return await _run_many(sql, rows)
        except Exception as err:  # noqa: BLE001
            if attempt >= _RETRIES or not is_connectivity_error(err):
                raise
            attempt += 1
            await asyncio.sleep(_RETRY_BACKOFF_S * attempt)


async def fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    return list(await _with_retry(sql, params, True))


async def execute(sql: str, params: tuple = ()) -> int:
    # Callers only issue idempotent upserts (INSERT ... ON DUPLICATE KEY UPDATE),
    # so retrying after a connectivity error cannot double-apply.
    return await _with_retry(sql, params, False)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None
