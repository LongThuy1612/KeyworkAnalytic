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
