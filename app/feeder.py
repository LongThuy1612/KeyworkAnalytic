"""Tops the SQLite queue up from MySQL -- as rarely and as cheaply as possible --
and returns expired leases to the queue.

The candidate query is an anti-join over affiliate_programs x
domain_keyword_volume. Re-running it whenever the queue runs low (every few
minutes, returning the same top rows each time) is what hammers the crawl DB, so:

  * keyset pagination: the position is stored locally (`kv`: cursor) and each
    pull continues where the last one ended;
  * pages of FEED_BATCH, at most one pull per FEED_MIN_INTERVAL_S, and only
    while fewer than FEED_LOW_WATER jobs are pending (a session claims 200);
  * backlog exhausted -> wait FEED_EXHAUSTED_WAIT_S; restart from the head once
    a day (FEED_CURSOR_RESET_S) to pick up newly crawled domains.

What is pending / running / done is answered by SQLite, never by MySQL.
"""

import asyncio
import logging
import time

from app import config, jobqueue, store
from app.farm import db

log = logging.getLogger("feeder")
TOOL = db.TOOL


def _kv_float(key: str) -> float:
    return float(store.kv_get(key, "0") or 0)


async def pull_once(now: float) -> int:
    cursor = store.kv_get("cursor")
    if cursor and now - _kv_float("cursor_at") > config.FEED_CURSOR_RESET_S:
        cursor = None  # restart from the head
    rows = await db.fetch_fresh(config.FEED_BATCH, cursor)
    skip = store.outbox_domains()
    domains = [d for d, _ in rows if d not in skip]
    added = jobqueue.enqueue(domains, [TOOL], source="auto") if domains else 0

    if len(rows) < config.FEED_BATCH:
        store.kv_set("cursor", None)
        store.kv_set("next_pull", str(now + config.FEED_EXHAUSTED_WAIT_S))
    else:
        if not cursor:
            store.kv_set("cursor_at", str(now))
        store.kv_set("cursor", rows[-1][1])
        store.kv_set("next_pull", str(now + config.FEED_MIN_INTERVAL_S))
    return added


async def run_forever(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            reclaimed = jobqueue.reclaim_stale()
            if reclaimed:
                log.info("returned %d expired job lease(s) to the queue", reclaimed)
            now = time.time()
            if config.FEED_AUTO and jobqueue.pending_count(TOOL) < config.FEED_LOW_WATER and now >= _kv_float("next_pull"):
                added = await pull_once(now)
                log.info("pulled a page from MySQL, %d new job(s) queued", added)
        except Exception as err:  # noqa: BLE001 -- MySQL hiccup: retried next tick (next_pull not advanced)
            log.warning("feeder tick failed: %s", err)
        try:
            await asyncio.wait_for(stop.wait(), timeout=config.FEED_INTERVAL_S)
        except asyncio.TimeoutError:
            pass
