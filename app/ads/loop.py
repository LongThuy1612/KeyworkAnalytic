"""Advertiser-signal worker: a light background task (HTTP only, no browser, no Google account).

  MySQL candidates ──(rare, 2000-row keyset pages)──▶ in-memory page ──▶ Transparency Center (3 workers, ~1 req/s each)
        ▲                                                                           │
        └── domain_ads_signal ◀──(one multi-row INSERT, 100 rows or 5 min)── SQLite ads_outbox

* An explicit list (POST /ads/domains) is served before the candidate pages: use it for programmes already on the site.
* A MySQL outage only delays the write; results wait in the outbox and the loop keeps working.
* Google answering 429/403 pauses the whole loop (growing back-off); it says nothing about any domain.
"""

import asyncio
import logging
import time

import httpx

from app import config, store
from app.ads import db
from app.ads.transparency import RateLimited, TransparencyError, check_domain, registrable

log = logging.getLogger("ads")
_CHUNK = 60


class AdsState:
    def __init__(self) -> None:
        self.running = False
        self.checked = 0
        self.withAds = 0
        self.withOtherAdvertisers = 0
        self.errors = 0
        self.rateLimited = 0
        self.pausedUntil = 0.0
        self.lastError: str | None = None

    def snapshot(self) -> dict:
        return {
            "running": self.running, "checked": self.checked, "withAds": self.withAds,
            "withOtherAdvertisers": self.withOtherAdvertisers, "errors": self.errors, "rateLimited": self.rateLimited,
            "pausedForSeconds": max(0, int(self.pausedUntil - time.time())), "lastError": self.lastError,
            "outbox": store.ads_outbox_size(), "priorityQueue": store.ads_prio_size(),
        }


async def flush_outbox() -> int:
    batch = store.ads_outbox_peek(500)
    if not batch:
        return 0
    try:
        await db.upsert_many([p for _, p in batch])
    except Exception as err:  # noqa: BLE001
        log.warning("[ads] flush failed, %d row(s) stay queued: %s", len(batch), err)
        return 0
    store.ads_outbox_done([i for i, _ in batch])
    return len(batch)


async def _maybe_flush(force: bool = False) -> None:
    if force or store.ads_outbox_size() >= config.ADS_FLUSH_MIN_ROWS or store.ads_outbox_oldest_age() >= config.ADS_FLUSH_MAX_AGE_S:
        while await flush_outbox() >= 500:
            pass


async def _next_page(stop: asyncio.Event) -> list[str]:
    """Explicit priority domains first, else one page of MySQL candidates (cursor kept in SQLite)."""
    prio = store.ads_prio_take(_CHUNK * 5)
    if prio:
        return prio
    now = time.time()
    if now < float(store.kv_get("ads_next_pull", "0") or 0):
        return []
    cursor = store.kv_get("ads_cursor")
    if cursor and now - float(store.kv_get("ads_cursor_at", "0") or 0) > 86400:
        cursor = None                                  # restart from the head daily to catch new domains
    rows = await db.fetch_candidates(config.ADS_PULL_BATCH, cursor)
    skip = store.ads_outbox_domains()
    domains = [d for d, _ in rows if d not in skip]
    if len(rows) < config.ADS_PULL_BATCH:
        store.kv_set("ads_cursor", None)
        store.kv_set("ads_next_pull", str(now + 3600))  # backlog exhausted: look again in an hour
    else:
        if not cursor:
            store.kv_set("ads_cursor_at", str(now))
        store.kv_set("ads_cursor", rows[-1][1])
    log.info("[ads] pulled %d candidate domain(s) from MySQL", len(domains))
    return domains


async def run_ads_loop(state: AdsState, stop: asyncio.Event) -> None:
    sem = asyncio.Semaphore(config.ADS_CONCURRENCY)
    streak = 0
    state.running = True
    try:
        async with httpx.AsyncClient() as client:
            while not stop.is_set():
                try:
                    domains = await _next_page(stop)
                except Exception as err:  # noqa: BLE001 -- MySQL hiccup: try again shortly
                    state.lastError = f"candidate pull failed: {err}"
                    log.warning("[ads] %s", state.lastError)
                    domains = []
                    await asyncio.sleep(30)
                if not domains:
                    await _maybe_flush(force=True)
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=20)
                    except asyncio.TimeoutError:
                        pass
                    continue

                cache: dict[str, dict] = {}              # help.x.com and x.com share one lookup

                async def one(domain: str) -> None:
                    nonlocal streak
                    root = registrable(domain)
                    async with sem:
                        if stop.is_set():
                            return
                        wait = state.pausedUntil - time.time()
                        if wait > 0:
                            await asyncio.sleep(wait)
                        res = cache.get(root)
                        if res is None:
                            try:
                                res = await check_domain(client, domain, config.ADS_MAX_PAGES, config.ADS_DELAY_S)
                            except RateLimited as err:
                                state.rateLimited += 1
                                streak += 1
                                state.pausedUntil = time.time() + min(900, 60 * streak)
                                state.lastError = f"rate limited ({err}); pausing {int(state.pausedUntil - time.time())}s"
                                log.warning("[ads] %s", state.lastError)
                                return                      # not recorded: the domain is picked up again later
                            except TransparencyError as err:
                                state.errors += 1
                                state.lastError = str(err)
                                return
                            streak = 0
                            cache[root] = res
                            await asyncio.sleep(config.ADS_DELAY_S)
                    store.ads_outbox_add([db.to_params(domain, res)])
                    state.checked += 1
                    state.withAds += int(res["has_ads"])
                    state.withOtherAdvertisers += int(res["has_other_advertisers"])

                for i in range(0, len(domains), _CHUNK):
                    if stop.is_set():
                        break
                    await asyncio.gather(*(one(d) for d in domains[i:i + _CHUNK]))
                    await _maybe_flush()
                    if i % (_CHUNK * 5) == 0:
                        log.info("[ads] %s", state.snapshot())
    finally:
        state.running = False
        await _maybe_flush(force=True)
