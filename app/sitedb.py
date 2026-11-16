"""The website's own database (PostgreSQL): where the domains come from and where the results go.

The site's table "Program" is the clean dataset -- only programmes that passed every check are in it -- so the farm
follows it directly instead of being handed a list. The results land in the table "KeywordSignal" of the same
database (one row per domain), which the website reads to show the brand-search demand on a programme page.

    SITE_DB_URL=postgresql://keywords:password@127.0.0.1:5432/topaffiliate

(the container runs with network_mode: host, and the site's compose publishes Postgres on 127.0.0.1:5432.)
Give the role the least it needs: SELECT on "Program", SELECT/INSERT/UPDATE on "KeywordSignal", nothing else.
"""

import asyncio
import json
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit

from app import config

_pool = None
_lock = asyncio.Lock()


def enabled() -> bool:
    return bool(config.SITE_DB_URL)


def _dsn() -> str:
    # Prisma URLs end in ?schema=public, which asyncpg would treat as a server setting and refuse.
    p = urlsplit(config.SITE_DB_URL)
    return urlunsplit((p.scheme, p.netloc, p.path, "", ""))


async def _get_pool():
    global _pool
    async with _lock:
        if _pool is None:
            import asyncpg  # only needed when SITE_DB_URL is set

            _pool = await asyncpg.create_pool(_dsn(), min_size=1, max_size=2, command_timeout=30)
        return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def fetch_domains(limit: int, after: str | None) -> list[str]:
    """One page of published domains in alphabetical order, continuing after `after` (keyset, no OFFSET)."""
    pool = await _get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            'SELECT domain FROM "Program" WHERE ($1::text IS NULL OR domain > $1) ORDER BY domain LIMIT $2',
            after, limit,
        )
    return [r["domain"].lower() for r in rows]


async def fetch_uncovered(domains: list[str]) -> list[str]:
    """Of `domains`, those with no lookup yet or one older than KEYWORD_RECHECK_DAYS."""
    if not domains:
        return []
    pool = await _get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            'SELECT domain FROM "KeywordSignal" WHERE domain = ANY($1::text[]) '
            "AND \"fetchedAt\" >= (now() AT TIME ZONE 'utc') - make_interval(days => $2::int)",
            domains, config.KEYWORD_RECHECK_DAYS,
        )
    covered = {r["domain"] for r in rows}
    return [d for d in domains if d not in covered]


_UPSERT = """
    INSERT INTO "KeywordSignal"
        (domain, keyword, "avgMonthlySearches", "threeMonthChange", "yoyChange", competition, "competitionIndex",
         "lowBid", "highBid", "monthlyTrend", "fetchedAt")
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
    ON CONFLICT (domain) DO UPDATE SET
        keyword = EXCLUDED.keyword,
        "avgMonthlySearches" = EXCLUDED."avgMonthlySearches",
        "threeMonthChange" = EXCLUDED."threeMonthChange",
        "yoyChange" = EXCLUDED."yoyChange",
        competition = EXCLUDED.competition,
        "competitionIndex" = EXCLUDED."competitionIndex",
        "lowBid" = EXCLUDED."lowBid",
        "highBid" = EXCLUDED."highBid",
        "monthlyTrend" = EXCLUDED."monthlyTrend",
        "fetchedAt" = EXCLUDED."fetchedAt"
"""


async def upsert_many(rows: list[list]) -> int:
    """All rows in one transaction (asyncpg's executemany is atomic); idempotent on the domain.
    Row layout (what the outbox stores): [domain, keyword, is_seed, avg, 3m, yoy, competition, competition_index,
    low_bid, high_bid, monthly_trend_json, fetched_at_iso]. Only seed rows count; a domain's last row wins."""
    latest: dict[str, tuple] = {}
    for r in rows:
        if not r[2]:
            continue
        trend = r[10]
        if isinstance(trend, str):
            json.loads(trend)  # reject a malformed value here, not inside the database
        latest[r[0]] = (r[0], r[1], r[3], r[4], r[5], r[6], r[7], r[8], r[9], trend, datetime.fromisoformat(r[11]).replace(tzinfo=None))
    if not latest:
        return 0
    pool = await _get_pool()
    async with pool.acquire() as con:
        await con.executemany(_UPSERT, list(latest.values()))
    return len(latest)
