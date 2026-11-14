"""Advertiser signal for an explicit list of domains, without the service (e.g. the programmes already on the website).

    python scripts/ads_backfill.py programs.json            # a JSON list of domains, or of objects with a "domain" key
    python scripts/ads_backfill.py domains.txt --force      # one domain per line; --force re-checks fresh rows too

Needs DATABASE_URL (the crawl DB). Writes only domain_ads_signal (sql/002_domain_ads_signal.sql). Resumable: domains
checked within ADS_RECHECK_DAYS are skipped unless --force. Google 429/403 -> growing pause, then the same domain again.
"""

import asyncio
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

from app import config  # noqa: E402
from app.ads import db  # noqa: E402
from app.ads.transparency import RateLimited, TransparencyError, check_domain, registrable  # noqa: E402
from app.mysql import close_pool, fetch_all  # noqa: E402


def load(path: str) -> list[str]:
    raw = open(path, encoding="utf-8").read()
    try:
        data = json.loads(raw)
        items = [d["domain"] if isinstance(d, dict) else d for d in data]
    except ValueError:
        items = [line.strip() for line in raw.splitlines() if line.strip()]
    return list(dict.fromkeys(str(d).lower() for d in items))


async def main() -> None:
    path = sys.argv[1]
    force = "--force" in sys.argv
    domains = load(path)
    if not force:
        fresh: set[str] = set()
        for i in range(0, len(domains), 1000):
            chunk = domains[i:i + 1000]
            rows = await fetch_all(
                f"SELECT domain FROM {db.TABLE} WHERE checked_at >= DATE_SUB(NOW(), INTERVAL {int(config.ADS_RECHECK_DAYS)} DAY) "
                f"AND domain IN ({','.join(['%s'] * len(chunk))})", tuple(chunk))
            fresh |= {r["domain"].lower() for r in rows}
        domains = [d for d in domains if d not in fresh]
    print(f"{len(domains)} domain(s) to check", flush=True)

    sem = asyncio.Semaphore(config.ADS_CONCURRENCY)
    cache: dict[str, dict] = {}
    pending: list[list] = []
    stats = {"checked": 0, "with_ads": 0, "with_other": 0, "errors": 0, "rate_limited": 0}
    pause_until = 0.0
    streak = 0
    lock = asyncio.Lock()
    t0 = time.time()

    async def flush(force_all: bool = False) -> None:
        async with lock:
            if pending and (force_all or len(pending) >= 100):
                batch = pending[:]
                try:
                    await db.upsert_many(batch)
                    del pending[:len(batch)]
                except Exception as err:  # noqa: BLE001
                    print("flush failed, will retry:", err, flush=True)

    async def one(client: httpx.AsyncClient, domain: str) -> None:
        nonlocal pause_until, streak
        root = registrable(domain)
        async with sem:
            while True:
                wait = pause_until - time.time()
                if wait > 0:
                    await asyncio.sleep(wait)
                res = cache.get(root)
                if res is not None:
                    break
                try:
                    res = await check_domain(client, domain, config.ADS_MAX_PAGES, config.ADS_DELAY_S)
                except RateLimited as err:
                    stats["rate_limited"] += 1
                    streak += 1
                    pause_until = time.time() + min(1800, 600 * streak)
                    print(f"rate limited ({err}); pausing {int(pause_until - time.time())}s", flush=True)
                    continue
                except TransparencyError:
                    stats["errors"] += 1
                    return
                streak = 0
                cache[root] = res
                await asyncio.sleep(config.ADS_DELAY_S * random.uniform(0.8, 1.4))
                break
        pending.append(db.to_params(domain, res))
        stats["checked"] += 1
        stats["with_ads"] += int(res["has_ads"])
        stats["with_other"] += int(res["has_other_advertisers"])
        if len(pending) >= 100:
            await flush()

    async with httpx.AsyncClient() as client:
        for i in range(0, len(domains), 300):
            await asyncio.gather(*(one(client, d) for d in domains[i:i + 300]))
            await flush()
            mins = (time.time() - t0) / 60
            print(f"[ads] {min(i + 300, len(domains))}/{len(domains)} | {stats} | {mins:.1f} min", flush=True)
    await flush(True)
    await close_pool()
    print("[ads] done:", stats, flush=True)


asyncio.run(main())
