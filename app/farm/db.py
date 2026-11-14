"""DB access for the Keyword Planner UI farm (app/farm/).

REQUIRES a `domain_keyword_volume` table added via a Prisma migration in
BEWeb (this service must never issue DDL -- see CrawlData's src/db/connection.py). Expected Prisma model:

    model DomainKeywordVolume {
      id     Int    @id @default(autoincrement())
      domain String
      keyword String @db.VarChar(255)
      isSeed  Boolean @default(false) @map("is_seed")

      avgMonthlySearches Int?    @map("avg_monthly_searches")
      threeMonthChange   String? @map("three_month_change") @db.VarChar(20)
      yoyChange          String? @map("yoy_change") @db.VarChar(20)
      competition        String? @db.VarChar(20)
      competitionIndex   Int?    @map("competition_index")
      lowTopOfPageBid    Int?    @map("low_top_of_page_bid")
      highTopOfPageBid   Int?    @map("high_top_of_page_bid")
      monthlyTrend       Json?   @map("monthly_trend")

      fetchedAt DateTime @default(now()) @map("fetched_at")

      @@unique([domain, keyword])
      @@index([domain])
      @@map("domain_keyword_volume")
    }

One row per (domain, keyword) -- ONLY the seed keyword itself (isSeed
always true here, the "Keywords you provided" row, one per domain).
"Keyword ideas" suggestion rows are downloaded as part of the same CSV
export (see app/farm/scrape.py's lookup_keywords()) but dropped
during parsing per an explicit operator instruction (2026-08-17): only the
seed keywords a batch was run with matter, not Google's own related-term
suggestions. monthlyTrend holds the CSV's 12 "Searches: <Mon Year>" columns
as {"2025-08": 2240000, ...}.
"""

import logging
import os
from datetime import datetime

from app import jobqueue
from app.mysql import executemany, fetch_all

TABLE = "domain_keyword_volume"
TOOL = "keyword"

# Re-check a domain this many days after its last lookup -- keeps the farm
# from re-querying the same already-covered domain forever once the initial
# backlog is done, while still refreshing stale volume numbers periodically.
_RECHECK_AFTER_DAYS = int(os.getenv("KEYWORD_RECHECK_DAYS", "30"))

log = logging.getLogger("keyword_db")
HISTORY_TABLE = "domain_keyword_volume_history"   # see sql/001_domain_keyword_volume_history.sql


async def fetch_fresh(limit: int, cursor: str | None) -> list[tuple[str, str]]:
    """One page of candidate domains for the feeder: no seed-keyword row yet
    (is_seed=1 specifically, so a domain whose lookup returned zero ideas is
    not mistaken for "never looked up"), or a seed row older than
    _RECHECK_AFTER_DAYS. Newest-crawled first, continuing at `cursor` (a
    crawled_at value, inclusive so boundary ties are not skipped). The feeder
    calls this a few times an hour at most -- everything else is answered by
    the local SQLite queue. Returns (domain, crawled_at)."""
    where = "AND ap.crawled_at <= %s" if cursor else ""
    rows = await fetch_all(
        f"""
        SELECT ap.domain, MAX(ap.crawled_at) AS crawled_at
        FROM affiliate_programs ap
        LEFT JOIN {TABLE} dkv ON dkv.domain = ap.domain AND dkv.is_seed = 1
        WHERE (dkv.domain IS NULL
               OR dkv.fetched_at < DATE_SUB(NOW(), INTERVAL {_RECHECK_AFTER_DAYS} DAY)) {where}
        GROUP BY ap.domain
        ORDER BY crawled_at DESC
        LIMIT %s
        """,
        ((cursor, limit) if cursor else (limit,)),
    )
    return [(r["domain"], str(r["crawled_at"])) for r in rows]


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


# Only %s placeholders, so the driver turns a whole batch into ONE multi-row
# INSERT (a NOW() in VALUES would force it back to one statement per row).
_UPSERT_SQL = f"""
    INSERT INTO {TABLE}
        (domain, keyword, is_seed, avg_monthly_searches, three_month_change,
         yoy_change, competition, competition_index, low_top_of_page_bid,
         high_top_of_page_bid, monthly_trend, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        is_seed = VALUES(is_seed),
        avg_monthly_searches = VALUES(avg_monthly_searches),
        three_month_change = VALUES(three_month_change),
        yoy_change = VALUES(yoy_change),
        competition = VALUES(competition),
        competition_index = VALUES(competition_index),
        low_top_of_page_bid = VALUES(low_top_of_page_bid),
        high_top_of_page_bid = VALUES(high_top_of_page_bid),
        monthly_trend = VALUES(monthly_trend),
        fetched_at = VALUES(fetched_at)
"""


_HISTORY_SQL = f"""
    INSERT IGNORE INTO {HISTORY_TABLE}
        (domain, keyword, fetched_at, avg_monthly_searches, three_month_change, yoy_change, competition,
         competition_index, low_top_of_page_bid, high_top_of_page_bid, monthly_trend)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


async def upsert_many(rows: list[list]) -> int:
    """Latest numbers: idempotent (unique key domain+keyword), one transaction for the whole batch. Then one history row
    per lookup (unique on domain+keyword+fetched_at, so a retried flush cannot duplicate it) so a domain checked again
    later shows a trend. The history write is best-effort: if its table is not there yet the main write still stands."""
    params = [tuple(r[:-1]) + (datetime.fromisoformat(r[-1]),) for r in rows]
    n = await executemany(_UPSERT_SQL, params)
    try:
        # r = [domain, keyword, is_seed, avg, 3m, yoy, comp, comp_idx, low, high, trend, at]
        hist = [(r[0], r[1], datetime.fromisoformat(r[-1]), r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10]) for r in rows]
        await executemany(_HISTORY_SQL, hist)
    except Exception as err:  # noqa: BLE001
        log.warning("history write skipped (%d row(s)): %s", len(rows), err)
    return n
