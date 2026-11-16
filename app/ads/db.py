"""MySQL side of the advertiser signal (table domain_ads_signal, see sql/002_domain_ads_signal.sql)."""

import json
from datetime import datetime

from app import config
from app.mysql import executemany, fetch_all

TABLE = "domain_ads_signal"


async def fetch_candidates(limit: int, cursor: str | None) -> list[tuple[str, str]]:
    """Domains of affiliate_programs with no signal yet, or one older than ADS_RECHECK_DAYS. Newest-crawled first,
    continuing at `cursor` (a crawled_at value, inclusive). Returns (domain, crawled_at)."""
    where = []
    args: list = []
    if cursor:
        where.append("ap.crawled_at <= %s")
        args.append(cursor)
    if config.ADS_MIN_SCORE:
        where.append("ap.affiliate_score >= %s")
        args.append(config.ADS_MIN_SCORE)
    extra = (" AND " + " AND ".join(where)) if where else ""
    rows = await fetch_all(
        f"""
        SELECT ap.domain, MAX(ap.crawled_at) AS crawled_at
        FROM affiliate_programs ap
        LEFT JOIN {TABLE} s ON s.domain = ap.domain
        WHERE (s.domain IS NULL OR s.checked_at < DATE_SUB(NOW(), INTERVAL {int(config.ADS_RECHECK_DAYS)} DAY)){extra}
        GROUP BY ap.domain
        ORDER BY crawled_at DESC
        LIMIT %s
        """,
        tuple(args + [limit]),
    )
    return [(r["domain"], str(r["crawled_at"])) for r in rows]


async def fetch_uncovered(domains: list[str]) -> list[str]:
    """Of `domains`, those with no signal yet or one older than ADS_RECHECK_DAYS. One IN query per page."""
    if not domains:
        return []
    marks = ",".join(["%s"] * len(domains))
    rows = await fetch_all(
        f"SELECT domain FROM {TABLE} WHERE checked_at >= DATE_SUB(NOW(), INTERVAL {int(config.ADS_RECHECK_DAYS)} DAY) "
        f"AND domain IN ({marks})",
        tuple(domains),
    )
    covered = {r["domain"] for r in rows}
    return [d for d in domains if d not in covered]


def to_params(domain: str, res: dict, at: datetime | None = None) -> list:
    return [
        domain,
        int(res["has_ads"]),
        int(res["has_other_advertisers"]),
        res["advertiser_count"],
        res["other_advertiser_count"],
        res["owner_match"],
        res["sampled_ads"],
        res["ads_range_low"],
        res["ads_range_high"],
        json.dumps(res["other_names"], ensure_ascii=False) if res["other_names"] else None,
        (at or datetime.now()).isoformat(),
    ]


_UPSERT_SQL = f"""
    INSERT INTO {TABLE}
        (domain, has_ads, has_other_advertisers, advertiser_count, other_advertiser_count, owner_match,
         sampled_ads, ads_range_low, ads_range_high, other_names, checked_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        has_ads = VALUES(has_ads),
        has_other_advertisers = VALUES(has_other_advertisers),
        advertiser_count = VALUES(advertiser_count),
        other_advertiser_count = VALUES(other_advertiser_count),
        owner_match = VALUES(owner_match),
        sampled_ads = VALUES(sampled_ads),
        ads_range_low = VALUES(ads_range_low),
        ads_range_high = VALUES(ads_range_high),
        other_names = VALUES(other_names),
        checked_at = VALUES(checked_at)
"""


async def upsert_many(rows: list[list]) -> int:
    """One multi-row statement in one transaction; idempotent on the primary key."""
    params = [tuple(r[:-1]) + (datetime.fromisoformat(r[-1]),) for r in rows]
    return await executemany(_UPSERT_SQL, params)
