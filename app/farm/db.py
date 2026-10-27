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

from app.mysql import execute, fetch_all

TABLE = "domain_keyword_volume"

# Re-check a domain this many days after its last lookup -- keeps the farm
# from re-querying the same already-covered domain forever once the initial
# backlog is done, while still refreshing stale volume numbers periodically
# (Google's own trend data moves month to month, same rationale as
# affiliate_programs.staleDays elsewhere in this codebase).
_RECHECK_AFTER_DAYS = 30


async def get_next_domains(limit: int) -> list[str]:
    """Domains from affiliate_programs that either have no seed-keyword row
    yet in domain_keyword_volume, or whose seed row is older than
    _RECHECK_AFTER_DAYS. Checks is_seed=1 specifically (not any row) so a
    domain whose seed lookup succeeded but returned zero keyword ideas isn't
    mistaken for "never looked up." Ordered by ap.crawled_at DESC (2026-08-27,
    operator's explicit call: keep this consistent with the similarweb services' own get_next_domains(), both of
    which already prioritize the most recently (re)crawled domains) --
    previously affiliate_score DESC (highest-signal domains first), changed
    since a domain just confirmed by Tier 1-3 is the one most likely to
    still be actionable/relevant right now, not necessarily whichever domain
    happens to have accumulated the highest score over its whole history."""
    rows = await fetch_all(
        f"""
        SELECT ap.domain
        FROM affiliate_programs ap
        LEFT JOIN {TABLE} dkv ON dkv.domain = ap.domain AND dkv.is_seed = 1
        WHERE dkv.domain IS NULL
           OR dkv.fetched_at < DATE_SUB(NOW(), INTERVAL {_RECHECK_AFTER_DAYS} DAY)
        GROUP BY ap.domain
        ORDER BY ap.crawled_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [r["domain"] for r in rows]


def build_params(keyword_to_domain: dict[str, str], rows: list[dict]) -> list[list]:
    """One upsert parameter list per SEED row of a (possibly multi-keyword)
    CSV export. Each row's own "keyword" text is mapped back to its domain
    (case-insensitive) because a batch shares one CSV with no per-row domain
    marker; a row whose keyword is not in the mapping is skipped rather than
    guessed at. "Keyword ideas" rows were already dropped during parsing
    (operator's explicit instruction: only seed keywords are kept)."""
    lookup = {k.lower(): v for k, v in keyword_to_domain.items()}
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
            ]
        )
    return out


_UPSERT_SQL = f"""
    INSERT INTO {TABLE}
        (domain, keyword, is_seed, avg_monthly_searches, three_month_change,
         yoy_change, competition, competition_index, low_top_of_page_bid,
         high_top_of_page_bid, monthly_trend, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
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
        fetched_at = NOW()
"""


async def upsert_params(params: list) -> None:
    """Idempotent: a re-run only refreshes fetched_at and the numbers on the
    (domain, keyword) unique key, so a retried outbox row cannot duplicate."""
    await execute(_UPSERT_SQL, tuple(params))
