# KeyworkAnalytic

Google Ads Keyword Planner search-volume farm, split out of CrawlData.
Browser automation of a real, logged-in Google Ads account (Terms-of-Service
risk accepted by the operator; session/batch throttling is unchanged from
CrawlData).

```
POST /keywords ─┐                            ┌──▶ farm (Chrome + Keyword Planner UI)
                ├─▶ SQLite job queue ─claim─▶┘            │
MySQL feeder ───┘   (pending→running→done|failed)         ▼
 affiliate_programs                          SQLite outbox ──batch flush──▶ MySQL domain_keyword_volume
```

**Crawl-DB access is kept to a minimum; this service's own state lives in
`data/state.sqlite`:**

| what | where it lives | MySQL traffic |
|---|---|---|
| work queue (pending / running / done / failed, leases, priority) | SQLite `jobs` | none |
| which domains are done or have no Keyword Planner data | SQLite `jobs` (re-queued after 14 days) | none |
| candidate discovery | feeder: keyset-paged (`crawled_at` cursor kept in SQLite `kv`), 1000-row pages, ≤ 1 pull / 10 min and only while < 200 jobs are pending; backlog exhausted → looks again after 1 h | one anti-join page per pull |
| results | SQLite `outbox` → one multi-row `INSERT … ON DUPLICATE KEY UPDATE` in one transaction when 20 rows are waiting or the oldest is 5 min old | ~ one write per few minutes |

A MySQL outage therefore only delays writes; nothing is lost, and the farm keeps
working from its local queue.

## Run

```bash
cp .env.example .env        # set DATABASE_URL
docker compose up -d --build
docker compose logs -f keyword
```

First time only — log into Google Ads inside the container's Chrome:

```bash
curl -X POST localhost:8101/auth/start-login
# open http://<host>:6081/vnc.html, sign in, then:
curl -X POST localhost:8101/auth/stop-login
curl localhost:8101/status      # hasSavedLogin should be true
```

The profile lives in `./data/profile`, so it survives rebuilds. Copy the old
`CrawlData/.keyword_farm_profile` there to skip the login.

## Endpoints (port 8101)

`POST /keywords` (queue domains now, optional priority) · `GET /keywords/jobs` · `GET /health` ·
`GET /status` (login, loop state, queue, outbox) · `POST /start` · `POST /stop` ·
`POST /auth/start-login` · `POST /auth/stop-login`

## Notes

* `network_mode: host` keeps `localhost` MySQL and its grants working; the
  container uses its own X display `:101` so it never touches the host's `:99`.
* Do not run this and CrawlData's built-in keyword farm
  (`enableKeywordFarm`) against the same profile at the same time.

## Keyword history

`domain_keyword_volume` keeps the latest numbers per domain. Every lookup also appends one row to
`domain_keyword_volume_history` (`sql/001_domain_keyword_volume_history.sql`), unique on (domain, keyword, fetched_at), so a
domain looked up again later shows a trend. A domain is looked up again after `KEYWORD_RECHECK_DAYS` (default 30).
The history write is best-effort: if its table does not exist yet, the main write still succeeds.

## Advertiser signal (Google Ads Transparency Center)

Answers one question per domain: **does anyone other than the website's own advertiser run Google ads that point to it?**
A third party paying for traffic to a merchant (an affiliate, a partner agency, a reseller) is a good sign for a publisher
deciding whether a programme is worth promoting.

* Plain HTTP to the public RPC the Transparency Center page itself uses; no browser, no Google account, no proxy. Up to
  `ADS_MAX_PAGES` pages of 40 creatives per domain, about 1 request/s in total (Google answers faster traffic with a redirect to google.com/sorry; the loop then pauses 10-30 min).
* Advertisers are grouped by name (one company often has several accounts). The owner is the advertiser whose name contains the
  domain's brand (a 3-letter brand needs a whole-word match), else the one that clearly dominates (>= 60% of the sampled
  ads). If neither can be told, `has_other_advertisers` stays 0 rather than guessing (`owner_match = 0`).
* Table `domain_ads_signal` (`sql/002_domain_ads_signal.sql`, apply once by hand): `has_ads`, `has_other_advertisers`,
  `advertiser_count`, `other_advertiser_count`, `owner_match`, `sampled_ads`, estimated ad range, at most 3 other names.
  Sample only: `has_other_advertisers = 0` means none were seen in the sampled creatives, not that none exist.
* The loop pulls candidates from `affiliate_programs` (2000-row keyset pages), writes through its own SQLite outbox in batches
  of 100, and pauses with a growing back-off when Google answers 429/403.
* `POST /ads/domains {"domains": [...]}` puts domains first in line; `GET /ads/status` shows progress.
* `python scripts/ads_backfill.py programs.json` checks an explicit list without the service.

## Only the clean dataset is looked up

Keyword Planner and Transparency lookups are scarce, so the automatic feeders (which walk every crawled domain) are **off by
default** (`FEED_AUTO=false`, `ADS_AUTO_FEED=false`). Hand in the domains worth the quota instead:

```bash
python scripts/queue_clean.py /path/to/clean_domains.txt --keywords http://localhost:8101 --ads http://localhost:8101
```

(`/keywords` queues the Keyword Planner jobs, `/ads/domains` the advertiser checks; both accept a list and run it first.)
