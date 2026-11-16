"""The website's own database (PostgreSQL): where the domains come from and where the results go.

The site's table "Program" is the clean dataset -- only programmes that passed every check are in it -- so the farm
follows it directly instead of being handed a list. The results land in the table "Keyword" of the same
database (one row per domain AND keyword: the brand keyword and its related searches), which the website reads.

    SITE_DB_URL=postgresql://keywords:password@127.0.0.1:5432/topaffiliate

(the container runs with network_mode: host, and the site's compose publishes Postgres on 127.0.0.1:5432.)
Give the role the least it needs: SELECT on "Program", SELECT/INSERT/UPDATE/DELETE on "Keyword", nothing else.
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
            'SELECT DISTINCT domain FROM "Keyword" WHERE "isSeed" AND domain = ANY($1::text[]) '
            "AND \"fetchedAt\" >= (now() AT TIME ZONE 'utc') - make_interval(days => $2::int)",
            domains, config.KEYWORD_RECHECK_DAYS,
        )
    covered = {r["domain"] for r in rows}
    return [d for d in domains if d not in covered]


_UPSERT = """
    INSERT INTO "Keyword"
        (domain, keyword, "isSeed", "avgMonthlySearches", "threeMonthChange", "yoyChange", competition, "competitionIndex",
         "lowBid", "highBid", "monthlyTrend", currency, "fetchedAt")
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13)
    ON CONFLICT (domain, keyword) DO UPDATE SET
        "isSeed" = EXCLUDED."isSeed",
        "avgMonthlySearches" = EXCLUDED."avgMonthlySearches",
        "threeMonthChange" = EXCLUDED."threeMonthChange",
        "yoyChange" = EXCLUDED."yoyChange",
        competition = EXCLUDED.competition,
        "competitionIndex" = EXCLUDED."competitionIndex",
        "lowBid" = EXCLUDED."lowBid",
        "highBid" = EXCLUDED."highBid",
        "monthlyTrend" = EXCLUDED."monthlyTrend",
        currency = EXCLUDED.currency,
        "fetchedAt" = EXCLUDED."fetchedAt"
"""

# A related search that is already stored as a brand keyword keeps its row.
_IDEA_UPSERT = _UPSERT.replace("ON CONFLICT (domain, keyword) DO UPDATE SET", "ON CONFLICT (domain, keyword) DO NOTHING -- ").split("-- ")[0]


async def upsert_many(rows: list[list]) -> int:
    """One transaction. Row layout (what the outbox stores): [domain, keyword, is_seed, avg, 3m, yoy, competition,
    competition_index, low_bid, high_bid, monthly_trend_json, fetched_at_iso, currency].
    Brand keywords (is_seed) are upserted by (domain, keyword). Related searches replace the previous ones of every domain
    in the batch (the KEYWORD_IDEAS_MAX best-searched are kept)."""
    seeds: dict[tuple[str, str], tuple] = {}
    ideas: dict[str, dict[str, tuple]] = {}
    for r in rows:
        at = datetime.fromisoformat(r[11]).replace(tzinfo=None)
        currency = r[12] if len(r) > 12 else None      # rows queued before the currency was kept have none
        trend = r[10]
        if isinstance(trend, str):
            json.loads(trend)  # reject a malformed value here, not inside the database
        key = r[1].strip()
        record = (r[0], key, bool(r[2]), r[3], r[4], r[5], r[6], r[7], r[8], r[9], trend, currency, at)
        if r[2]:
            seeds[(r[0], key.lower())] = record
        else:
            ideas.setdefault(r[0], {})[key.lower()] = record
    if not seeds and not ideas:
        return 0
    idea_rows: list[tuple] = []
    for per_domain in ideas.values():
        idea_rows += sorted(per_domain.values(), key=lambda t: -(t[3] or 0))[:config.KEYWORD_IDEAS_MAX]
    pool = await _get_pool()
    async with pool.acquire() as con:
        async with con.transaction():
            if ideas:
                await con.execute('DELETE FROM "Keyword" WHERE domain = ANY($1::text[]) AND NOT "isSeed"', list(ideas))
            if seeds:
                await con.executemany(_UPSERT, list(seeds.values()))
            if idea_rows:
                await con.executemany(_IDEA_UPSERT, idea_rows)
    return len(seeds) + len(idea_rows)
