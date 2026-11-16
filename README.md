# KeyworkAnalytic

Google Ads Keyword Planner search-volume farm, split out of CrawlData.
Browser automation of a real, logged-in Google Ads account (Terms-of-Service
risk accepted by the operator; session/batch throttling is unchanged from
CrawlData).

```
POST /keywords ─┐                            ┌──▶ farm (Chrome + Keyword Planner UI)
                ├─▶ SQLite job queue ─claim─▶┘            │
website feeder ─┘   (pending→running→done|failed)         ▼
 Postgres "Program"                          SQLite outbox ──batch flush──▶ Postgres "Keyword"
```

The website reads "Keyword" and shows the brand-search demand on the programme page (paid plans). This service no
longer uses the crawl database.

**Database access is kept to a minimum; this service's own state lives in `data/state.sqlite`:**

| what | where it lives | database traffic |
|---|---|---|
| work queue (pending / running / done / failed, leases, priority) | SQLite `jobs` | none |
| which domains are done or have no Keyword Planner data | SQLite `jobs` (re-queued after 14 days) | none |
| candidate discovery | feeder: keyset-paged over the site's programmes (domain cursor kept in SQLite `kv`), 1000-row pages, ≤ 1 pull / 10 min and only while < 200 jobs are pending; end of the list → looks again after 1 h | one query per page, plus one to drop domains looked up within `KEYWORD_RECHECK_DAYS` |
| results | SQLite `outbox` → one multi-row upsert in one transaction when 20 rows are waiting or the oldest is 5 min old | ~ one write per few minutes |

A database outage therefore only delays writes; nothing is lost, and the farm keeps working from its local queue.

## Run

```bash
cp .env.example .env        # set SITE_DB_URL
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

* `network_mode: host` lets `127.0.0.1` reach the website's Postgres; the
  container uses its own X display `:101` so it never touches the host's `:99`.
* Do not run this and CrawlData's built-in keyword farm
  (`enableKeywordFarm`) against the same profile at the same time.

## What is stored

One row per domain in "Keyword": the brand keyword, average monthly searches, three-month and year-on-year change,
competition, top-of-page bid range and the 12-month trend. A domain is looked up again after `KEYWORD_RECHECK_DAYS`
(default 30) and its row is overwritten. The table is created by the website (`npx prisma db push` there), not by this
service. Give the service a role with the least it needs:

```sql
CREATE ROLE keywords LOGIN PASSWORD '...';
GRANT CONNECT ON DATABASE topaffiliate TO keywords;
GRANT USAGE ON SCHEMA public TO keywords;
GRANT SELECT ON "Program" TO keywords;
GRANT SELECT, INSERT, UPDATE, DELETE ON "Keyword" TO keywords;
```

## Advertiser signal

Moved to its own service: **GoogleAdsTransparencyCenter** (plain HTTP, no browser). It writes "AdsSignal" in the same Postgres.

## Only the clean dataset is looked up

Keyword Planner lookups are scarce. With `SITE_DB_URL` set the feeder follows the programmes published on the website. Without
it nothing is fed automatically (`FEED_AUTO=false`); hand in the domains worth the quota instead:

```bash
python scripts/queue_clean.py /path/to/clean_domains.txt --keywords http://localhost:8101
```

(`/keywords` queues the Keyword Planner jobs and accepts a list that runs first.)
