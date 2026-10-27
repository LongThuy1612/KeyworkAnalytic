"""KeyworkAnalytic -- Google Ads Keyword Planner search-volume farm.

Standalone service split out of CrawlData. It talks to the crawl system only
through MySQL: candidates come from `affiliate_programs`, results go to
`domain_keyword_volume` (both owned by BEWeb's migrations). Results pass
through a local SQLite outbox first, so a MySQL outage never loses a lookup.

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

from app import logging_setup

logging_setup.install()

from app import store  # noqa: E402
from app.farm import browser  # noqa: E402
from app.loop import KeywordFarmAutoState, run_keyword_farm_auto_loop  # noqa: E402
from app.mysql import close_pool  # noqa: E402

log = logging.getLogger("service")
state = KeywordFarmAutoState()
_task: asyncio.Task | None = None
_stop_event: asyncio.Event | None = None


def _start() -> bool:
    global _task, _stop_event
    if _task is not None and not _task.done():
        return False
    _stop_event = asyncio.Event()
    _task = asyncio.create_task(run_keyword_farm_auto_loop(state, _stop_event))
    return True


async def _stop() -> bool:
    if _task is None or _task.done():
        return False
    _stop_event.set()
    try:
        await asyncio.wait_for(_task, timeout=30)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        _task.cancel()
    return True


@asynccontextmanager
async def lifespan(_: FastAPI):
    if os.environ.get("KEYWORD_FARM_ENABLED", "true").lower() == "true":
        _start()
        log.info("farm loop started")
    else:
        log.info("KEYWORD_FARM_ENABLED=false -- idle (login/status endpoints only)")
    yield
    await _stop()
    await browser.close_context()
    await close_pool()


app = FastAPI(title="KeyworkAnalytic", lifespan=lifespan)


@app.get("/health")
async def health():
    # Unhealthy only when the loop was started but its task has died, so the
    # docker healthcheck restarts the container.
    alive = _task is not None and not _task.done()
    return {"ok": alive or _task is None, "loopAlive": alive}


@app.get("/status")
async def status():
    return {
        "hasSavedLogin": browser.has_saved_login(),
        "loopAlive": _task is not None and not _task.done(),
        "outboxPending": store.outbox_size(),
        **state.snapshot(),
    }


@app.post("/start")
async def start():
    return {"started": _start()}


@app.post("/stop")
async def stop():
    return {"stopped": await _stop()}


@app.post("/auth/start-login")
async def start_login():
    """Opens the Google Ads login page in the container's own Chrome; finish
    the login through noVNC (http://<host>:6081/vnc.html), then call
    /auth/stop-login."""
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
