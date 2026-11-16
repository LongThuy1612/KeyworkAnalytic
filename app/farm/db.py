"""Keyword rows: the queue claim, the row builder, and thin wrappers over the website's database (app/sitedb.py).

The table is "KeywordSignal" there: one row per domain, the SEED keyword only (isSeed is always true here, the
"Keywords you provided" row). "Keyword ideas" suggestion rows are downloaded as part of the same CSV export (see
app/farm/scrape.py's lookup_keywords()) but dropped during parsing per an explicit operator instruction (2026-08-17):
only the seed keywords a batch was run with matter, not Google's own related-term suggestions. monthlyTrend holds the
CSV's 12 "Searches: <Mon Year>" columns as {"2025-08": 2240000, ...}.
"""

from datetime import datetime

from app import jobqueue, sitedb

TOOL = "keyword"


async def fetch_uncovered(domains: list[str]) -> list[str]:
    return await sitedb.fetch_uncovered(domains)


async def upsert_many(rows: list[list]) -> int:
    return await sitedb.upsert_many(rows)


async def get_next_domains(limit: int) -> list[str]:
    """What the farm loop calls: work comes from the local SQLite queue."""
    return jobqueue.claim(TOOL, limit)


def build_params(keyword_to_domain: dict[str, str], rows: list[dict]) -> list[list]:
    """One upsert parameter list per SEED row of a (possibly multi-keyword)
    CSV export. Each row's own "keyword" text is mapped back to its domain
    (case-insensitive) because a batch shares one CSV with no per-row domain
    marker; a row whose keyword is not in the mapping is skipped rather than
    guessed at. "Keyword ideas" rows were already dropped during parsing
    (operator's explicit instruction: only seed keywords are kept). The last
    element is the lookup time (ISO) so a delayed flush keeps the real
    fetched_at."""
    lookup = {k.lower(): v for k, v in keyword_to_domain.items()}
    at = datetime.now().isoformat()
    out: list[list] = []
    for row in rows:
        domain = lookup.get(row["keyword"].strip().lower())
        if domain is None:
            continue
        out.append(
            [
                domain,
                row["keyword"],
                row["is_seed"],
                row["avg_monthly_searches"],
                row["three_month_change"],
                row["yoy_change"],
                row["competition"],
                row["competition_index"],
                row["low_top_of_page_bid"],
                row["high_top_of_page_bid"],
                row["monthly_trend_json"],
                at,
            ]
        )
    return out
