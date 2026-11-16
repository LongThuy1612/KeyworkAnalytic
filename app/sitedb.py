"""Read-only view of the website's own database (PostgreSQL): the programmes that are published on the site.

The site's table "Program" is the clean dataset -- only programmes that passed every check are in it -- so the
keyword and advertiser lookups can follow it directly instead of being handed a list. Set SITE_DB_URL to turn it on:

    SITE_DB_URL=postgresql://user:password@127.0.0.1:5432/topaffiliate

(the container runs with network_mode: host, and the site's compose publishes Postgres on 127.0.0.1:5432.)
Use a role that can only SELECT. Nothing is ever written there.
"""

import asyncio
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


async def fetch_domains(limit: int, after: str | None) -> list[str]:
    """One page of published domains in alphabetical order, continuing after `after` (keyset, no OFFSET)."""
    pool = await _get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            'SELECT domain FROM "Program" WHERE ($1::text IS NULL OR domain > $1) ORDER BY domain LIMIT $2',
            after, limit,
        )
    return [r["domain"].lower() for r in rows]
