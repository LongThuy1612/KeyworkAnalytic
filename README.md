# KeyworkAnalytic

Google Ads Keyword Planner search-volume farm, split out of CrawlData.
Browser automation of a real, logged-in Google Ads account (Terms-of-Service
risk accepted by the operator; session/batch throttling is unchanged from
CrawlData).

```
MySQL affiliate_programs ──candidates──▶ farm (Chrome + Keyword Planner UI)
                                              │
                                              ▼
                         SQLite outbox (data/state.sqlite)
                                              │ flush, retried
                                              ▼
                              MySQL domain_keyword_volume
```

* **Outbox**: every parsed result is written to local SQLite first and drained
  to MySQL; a MySQL outage delays writes but never loses a lookup.
* **Miss ledger**: a domain Keyword Planner had no data for is not re-picked for
  `NO_DATA_RETRY_AFTER_DAYS` (default 14). CrawlData re-picked it every session.
* Tables are owned by BEWeb's Prisma migrations; this service never runs DDL.

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

`GET /health` · `GET /status` (login, loop state, outbox size) · `POST /start` ·
`POST /stop` · `POST /auth/start-login` · `POST /auth/stop-login`

## Notes

* `network_mode: host` keeps `localhost` MySQL and its grants working; the
  container uses its own X display `:101` so it never touches the host's `:99`.
* Do not run this and CrawlData's built-in keyword farm
  (`enableKeywordFarm`) against the same profile at the same time.
