"""KeyworkAnalytic -- Google Ads Keyword Planner search-volume farm.

Standalone service split out of CrawlData. The crawl DB (MySQL) is touched as
little as possible; everything the farm needs day to day lives in its own
SQLite file (data/state.sqlite):

  * a work queue (jobs): filled from MySQL by the feeder in big, rare,
    cursor-paged pulls, or by POST /keywords for domains you want now;
  * an outbox: results are written locally and flushed to
    `domain_keyword_volume` in one transaction every few minutes, so a MySQL
    outage never loses a lookup.

Env:
  DATABASE_URL           mysql://user:pass@host:3306/db   (required)
  KEYWORD_FARM_ENABLED   run the farm loop on boot (default true); false
                         leaves only the login/status endpoints up.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app import logging_setup

logging_setup.install()

from app import config, feeder, jobqueue, store  # noqa: E402
from app.ads import loop as ads_loop  # noqa: E402
from app.farm import browser  # noqa: E402
from app.loop import KeywordFarmAutoState, flush_outbox, run_keyword_farm_auto_loop  # noqa: E402
from app.mysql import close_pool  # noqa: E402

log = logging.getLogger("service")
state = KeywordFarmAutoState()
ads_state = ads_loop.AdsState()
_task: asyncio.Task | None = None
_stop_event: asyncio.Event | None = None


def _alive() -> bool:
    return _task is not None and not _task.done()


def _start() -> bool:
    global _task, _stop_event
    if _alive():
        return False
    _stop_event = asyncio.Event()
    _task = asyncio.create_task(run_keyword_farm_auto_loop(state, _stop_event))
    return True


async def _stop() -> bool:
    if not _alive():
        return False
    _stop_event.set()
    try:
        await asyncio.wait_for(_task, timeout=30)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        _task.cancel()
    return True


async def _outbox_flusher(stop: asyncio.Event) -> None:
    """Timer flush, so rows left over after a MySQL outage (or after a session
    ends) land without waiting for the next lookup."""
    while not stop.is_set():
        try:
            while await flush_outbox() >= config.FLUSH_BATCH:
                pass
        except Exception as err:  # noqa: BLE001
            log.warning("outbox flusher: %s", err)
        try:
            await asyncio.wait_for(stop.wait(), timeout=120)
        except asyncio.TimeoutError:
            pass


@asynccontextmanager
async def lifespan(_: FastAPI):
    if os.environ.get("KEYWORD_FARM_ENABLED", "true").lower() == "true":
        _start()
        log.info("farm loop started")
    else:
        log.info("KEYWORD_FARM_ENABLED=false -- idle (login/status endpoints only)")
    feed_stop, flush_stop = asyncio.Event(), asyncio.Event()
    feed = asyncio.create_task(feeder.run_forever(feed_stop))
    flusher = asyncio.create_task(_outbox_flusher(flush_stop))
    cleanup = asyncio.create_task(logging_setup.log_cleanup_task())
    ads_stop = asyncio.Event()
    ads_task = asyncio.create_task(ads_loop.run_ads_loop(ads_state, ads_stop)) if config.ADS_ENABLED else None
    yield
    cleanup.cancel()
    feed_stop.set()
    flush_stop.set()
    ads_stop.set()
    if ads_task:
        await asyncio.gather(ads_task, return_exceptions=True)
    await _stop()
    await asyncio.gather(feed, flusher, return_exceptions=True)
    await flush_outbox()
    await browser.close_context()
    await close_pool()


app = FastAPI(title="KeyworkAnalytic", version="1.0.0", lifespan=lifespan)


class KeywordsRequest(BaseModel):
    domains: list[str] = Field(..., min_length=1, max_length=5000)
    priority: int = Field(1, description="higher runs first; auto-fed jobs have priority 0")


@app.get("/", summary="Health check")
async def root():
    return {"status": "running", "service": "KeyworkAnalytic", "version": "1.0.0"}


@app.get("/health")
async def health():
    # Unhealthy only when the loop was started but its task has died, so the
    # docker healthcheck restarts the container.
    return {"ok": _alive() or _task is None, "loopAlive": _alive()}


@app.get("/status")
async def status():
    return {
        "hasSavedLogin": browser.has_saved_login(),
        "loopAlive": _alive(),
        "queue": jobqueue.counts().get("keyword", {}),
        "outboxPending": store.outbox_size(),
        **state.snapshot(),
    }


@app.get("/ads/status", summary="Advertiser-signal worker (Google Ads Transparency Center)")
async def ads_status():
    return {"enabled": config.ADS_ENABLED, **ads_state.snapshot()}


class AdsDomainsRequest(BaseModel):
    domains: list[str] = Field(..., min_length=1, max_length=20000)


@app.post("/ads/domains", summary="Check these domains first (before the automatic candidate pages)")
async def ads_domains(body: AdsDomainsRequest):
    domains = sorted({d.strip().lower() for d in body.domains if d.strip()})
    return {"requested": len(domains), "queued": store.ads_prio_add(domains)}


@app.post("/keywords", summary="Queue domains for a Keyword Planner lookup")
async def queue_domains(body: KeywordsRequest):
    if jobqueue.pending_count() + len(body.domains) > config.MAX_QUEUE_PENDING:
        raise HTTPException(status_code=503, detail={"error_code": "QUEUE_FULL", "message": "Queue is full, retry later."})
    domains = sorted({d.strip().lower() for d in body.domains if d.strip()})
    return {"requested": len(domains), "queued": jobqueue.enqueue(domains, ["keyword"], source="api", priority=body.priority)}


@app.get("/keywords/jobs", summary="Recent jobs")
async def jobs(status: str | None = None, limit: int = 50):
    return jobqueue.list_jobs(status=status, limit=limit)


@app.post("/start")
async def start():
    return {"started": _start()}


@app.post("/stop")
async def stop():
    return {"stopped": await _stop()}


@app.post("/auth/start-login")
async def start_login():
    """Opens the Google Ads login page in the container's own Chrome; finish the
    login through noVNC (http://<host>:6081/vnc.html), then call /auth/stop-login."""
    try:
        await browser.start_login()
    except Exception as err:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Could not open the login page: {err}")
    return {"status": "success", "action_required": "Log in through noVNC, then POST /auth/stop-login."}


@app.post("/auth/stop-login")
async def stop_login():
    try:
        await browser.close_context()
    except Exception as err:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Could not close the login window: {err}")
    return {"status": "success", "hasSavedLogin": browser.has_saved_login()}
