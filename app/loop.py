"""Runs the Keyword Planner UI farm (src/keyword_farm/) as a background task
alongside continuous crawl, started/stopped by src/api/main.py's discover-
auto lifecycle -- same wiring shape as lady_tools_auto.py/similarweb's own
auto loops (one task, own stop_event, RAM-pressure aware, StallWatchdog
death/orphan hooks).

Reuses the exact session/break/batch throttling scripts/run_keyword_farm.py
already validated by hand (30-90 min session, 90-180 min break, 20-40s
between batches, 3-5 domains/batch, all randomized -- see that script's own
module docstring for the full rationale) rather than inventing a second
throttle profile; operator's explicit call (2026-08-17) to keep that risk
profile unchanged when wiring this into the server. Only ONE worker ever --
unlike lady_tools_auto's LADY_TOOLS_AUTO_PARALLEL_BATCHES, this drives a
single real, logged-in Google Ads account's UI, not an anonymous pool, so
there's no concurrency to add here.

RAM-pressure handling mirrors lady_tools_auto.py's own Lady Gemini handling
(see that module's run_lady_tools_auto_loop and CLAUDE.md's session notes on
why -- a persistent Chrome profile like this farm's own src/keyword_farm/
browser.py holds RAM across calls, not just during a single fetch, so pausing
new work alone leaves it pinned indefinitely under sustained pressure).
Operator's explicit call (2026-08-17): close the browser outright at
stop_search/emergency pressure, same as Lady Gemini -- the next tick's own
get_context() lazily relaunches it once pressure clears.
"""

import asyncio
import logging
import random

from app import config, jobqueue, store
from app.farm import browser, db
from app.farm.keywordize import domain_to_keyword
from app.farm.scrape import KeywordPlannerAccountError, ensure_discover_page, lookup_keywords
from app.ram_guard import RamGuard

logger = logging.getLogger("keyword_farm")

_SESSION_MIN_MINUTES = 30
_SESSION_MAX_MINUTES = 90
_BREAK_MIN_MINUTES = 90
_BREAK_MAX_MINUTES = 180

# Confirmed live 2026-08-29 (see CLAUDE.md's session notes): a session that
# raises (MySQL hiccup, network blip, browser crash) used to fall through to
# the same 90-180min break as a normal completed session -- appropriate for
# throttling real UI activity, but wasteful when the session did little or
# no real Keyword Planner interaction before failing. A short break here
# lets the loop retry promptly instead of sitting idle for up to 3 hours
# waiting out an infra outage that may already be over in minutes.
# A session that found nothing to do (empty local queue -- e.g. the very first
# session starting a few seconds before the feeder's first pull -- or no saved
# login) must not be treated as a finished session, or it sleeps the full
# 90-180 min throttle break. Confirmed live 2026-10-27: first session ran at
# 04:10:27, the feeder queued 1000 jobs at 04:10:32, the loop slept 109 min.
NO_WORK = -1
NO_LOGIN = -2
_IDLE_RETRY_S = 60
_NO_LOGIN_RETRY_S = 300
_ERROR_BREAK_MIN_MINUTES = 3
_ERROR_BREAK_MAX_MINUTES = 8
_BATCH_DELAY_MIN_S = 20
_BATCH_DELAY_MAX_S = 40
_KEYWORDS_PER_BATCH_MIN = 3
_KEYWORDS_PER_BATCH_MAX = 5
_DOMAINS_PER_SESSION_BATCH = 200
# Outer safety net around lookup_keywords() (2026-08-27, root cause of a
# multi-hour silent stall on the VPS, see scrape.py's own
# _SAVE_DOWNLOAD_TIMEOUT_S comment for the specific unbounded await that
# caused it -- now fixed there directly). Kept here too as defense in
# depth: any FUTURE unbounded await added inside lookup_keywords() would
# otherwise reproduce the exact same failure mode -- the task never
# raises/completes, so _keyword_farm_is_alive()'s `not task.done()` check
# keeps reporting healthy and StallWatchdog's death-detection never fires.
# Generous relative to the individual UI-step timeouts inside
# lookup_keywords() (each bounded at <=30s) so this only trips on a
# genuine hang, not normal slow-page variance.
_LOOKUP_TIMEOUT_S = 120.0

# How often the outer loop checks stop_event/RAM pressure while idling
# between a domain-lookup batch and the next one, and during the
# inter-session break -- short enough that POST /discover-auto/stop's own
# stop_event.set() (see main.py's _stop_continuous_crawl) is noticed
# promptly rather than only at the next batch/session boundary.
_TICK_S = 5.0


class KeywordFarmAutoState:
    """Snapshot surface for GET /discover-auto/status, same shape/purpose as
    LadyToolsAutoState/SimilarWebSyncState elsewhere in this package."""

    def __init__(self) -> None:
        self.running = False
        self.disabled = False
        self.sessionsRun = 0
        self.domainsLookedUp = 0
        self.lastError: str | None = None

    def snapshot(self) -> dict:
        return {
            "running": self.running,
            "disabled": self.disabled,
            "sessionsRun": self.sessionsRun,
            "domainsLookedUp": self.domainsLookedUp,
            "lastError": self.lastError,
        }


async def flush_outbox() -> int:
    """Drains the outbox into MySQL in ONE transaction per call (up to
    FLUSH_BATCH rows). Never raises -- on failure (almost always MySQL being
    unreachable) the rows stay queued locally and the next flush retries."""
    batch = store.outbox_peek(config.FLUSH_BATCH)
    if not batch:
        return 0
    try:
        await db.upsert_many([params for _, params in batch])
    except Exception as err:  # noqa: BLE001
        logger.warning("[loop] outbox flush failed, %d row(s) stay queued: %s", len(batch), err)
        return 0
    store.outbox_done([i for i, _ in batch])
    return len(batch)


async def maybe_flush() -> None:
    if store.outbox_size() >= config.FLUSH_MIN_ROWS or store.outbox_oldest_age() >= config.FLUSH_MAX_AGE_S:
        await flush_outbox()


async def _close_browser_best_effort(reason: str) -> None:
    try:
        await browser.close_context()
    except Exception as err:  # noqa: BLE001 -- must not take down the pressure-handling tick
        logger.warning("[keyword_farm_auto] failed to close browser under %s: %s", reason, err)


async def _wait_ticks(stop_event: asyncio.Event, total_seconds: float) -> None:
    """Sleeps up to total_seconds in _TICK_S increments, returning early the
    moment stop_event fires -- used for both the inter-batch delay and the
    inter-session break so a stop request doesn't have to wait out a
    multi-hour break to actually take effect."""
    remaining = total_seconds
    while remaining > 0 and not stop_event.is_set():
        step = min(_TICK_S, remaining)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=step)
        except asyncio.TimeoutError:
            pass
        remaining -= step


async def _run_session(state: KeywordFarmAutoState, stop_event: asyncio.Event, guard: RamGuard, session_minutes: float) -> int:
    """One farming session, same shape as scripts/run_keyword_farm.py's own
    _run_session -- batches domains into random groups of 3-5, looks each
    batch up, saves results, and stops early on stop_event, a RAM emergency,
    or the session's own time budget running out."""
    if not browser.has_saved_login():
        logger.warning("[keyword_farm_auto] No saved Google login for this profile -- skipping session (POST /auth/start-login, then log in via noVNC).")
        return NO_LOGIN

    await flush_outbox()
    domains = await db.get_next_domains(_DOMAINS_PER_SESSION_BATCH)  # local SQLite queue
    if not domains:
        return NO_WORK
    unfinished = set(domains)  # claimed but not yet resolved; handed back on exit

    context = await browser.get_context()
    page = await context.new_page()

    deadline = asyncio.get_event_loop().time() + session_minutes * 60
    looked_up = 0

    batches: list[dict[str, str]] = []
    current: dict[str, str] = {}
    current_target = random.randint(_KEYWORDS_PER_BATCH_MIN, _KEYWORDS_PER_BATCH_MAX)
    for domain in domains:
        keyword = domain_to_keyword(domain)
        if not keyword:
            jobqueue.finish(domain, False, error="no keyword derivable from domain")
            unfinished.discard(domain)
            continue
        if keyword.lower() in current:
            continue
        current[keyword.lower()] = domain
        if len(current) >= current_target:
            batches.append(current)
            current = {}
            current_target = random.randint(_KEYWORDS_PER_BATCH_MIN, _KEYWORDS_PER_BATCH_MAX)
    if current:
        batches.append(current)

    try:
        await ensure_discover_page(page)

        for keyword_to_domain in batches:
            if stop_event.is_set():
                break
            if asyncio.get_event_loop().time() >= deadline:
                logger.info("[keyword_farm_auto] Time budget reached (%.1f min) -- ending session.", session_minutes)
                break
            if guard.pressure in ("pause_crawl", "stop_search", "emergency"):
                # Matches every other worker pool's own pressure gate
                # (continuous_crawl.py/tier2_worker.py/tier3_worker.py/
                # lady_tools_auto.py all stop starting new work at
                # pause_crawl, not just stop_search/emergency) -- this loop
                # used to only react at the two higher tiers, which meant it
                # kept opening new batches even after every other pool in
                # the system had already stopped taking on new work at the
                # lower pause_crawl threshold.
                logger.info("[keyword_farm_auto] RAM pressure %r -- ending session early.", guard.pressure)
                break

            keywords = list(keyword_to_domain.keys())
            try:
                rows = await asyncio.wait_for(lookup_keywords(page, keywords), timeout=_LOOKUP_TIMEOUT_S)
            except asyncio.TimeoutError:
                state.lastError = f"lookup_keywords timed out after {_LOOKUP_TIMEOUT_S}s for keywords={keywords!r}"
                logger.error("[keyword_farm_auto] %s -- ending session early (page state is unknown after a hang, safer to restart fresh)", state.lastError)
                break
            except KeywordPlannerAccountError as err:
                state.lastError = str(err)
                logger.error("[keyword_farm_auto] Account not usable, aborting session early: %s", err)
                break
            except Exception as err:  # noqa: BLE001 -- one bad batch must not kill the whole session
                state.lastError = str(err)
                logger.warning("[keyword_farm_auto] Lookup failed for batch=%r: %s", keywords, err)
                await _wait_ticks(stop_event, random.uniform(_BATCH_DELAY_MIN_S, _BATCH_DELAY_MAX_S))
                continue

            params = db.build_params(keyword_to_domain, rows) if rows else []
            found = {p[0] for p in params}
            for d in keyword_to_domain.values():
                # A domain with no row = Keyword Planner has no data for it:
                # closed as failed locally, re-queued only after REQUEUE_AFTER_S.
                jobqueue.finish(d, d in found, error=None if d in found else "no Keyword Planner data")
                unfinished.discard(d)
            if params:
                store.outbox_add(params)
                await maybe_flush()
                looked_up += len(rows)
                state.domainsLookedUp += len(rows)
                for row in rows:
                    domain = keyword_to_domain.get(row["keyword"].strip().lower())
                    logger.info(
                        "[keyword_farm_auto] %s (keyword=%r): avg_monthly_searches=%s",
                        domain, row["keyword"], row.get("avg_monthly_searches"),
                    )

            await _wait_ticks(stop_event, random.uniform(_BATCH_DELAY_MIN_S, _BATCH_DELAY_MAX_S))
    finally:
        jobqueue.release(list(unfinished))  # untouched domains go back to the queue
        try:
            await page.close()
        except Exception:  # noqa: BLE001
            pass

    return looked_up


async def run_keyword_farm_auto_loop(state: KeywordFarmAutoState, stop_event: asyncio.Event) -> None:
    """Self-rescheduling session/break loop -- launched by
    src/api/main.py's _start_continuous_crawl alongside lady_tools_auto/
    similarweb, only when the caller opts in via DiscoverAutoRequest.
    enableKeywordFarm (default off: this automates the Google Ads UI, a
    Terms-of-Service risk explicitly accepted per-session, not something to
    silently start on every crawl -- see CLAUDE.md's session notes and
    scripts/run_keyword_farm.py's own docstring)."""
    guard = RamGuard()
    state.running = True
    try:
        while not stop_event.is_set():
            transition = guard.tick()
            if transition["changed"]:
                logger.info("[keyword_farm_auto] RAM %s%% -- pressure %s -> %s", transition["pct"], transition["prevPressure"], transition["newPressure"])
                if transition["newPressure"] in ("stop_search", "emergency"):
                    # Same reasoning as lady_tools_auto.py's own Lady Gemini
                    # handling: this farm's persistent Chrome profile holds
                    # RAM across calls, not just mid-fetch, so pausing new
                    # work alone (the loop's own guard.pressure check inside
                    # _run_session) doesn't release what's already held.
                    # Closing outright is safe -- the next session's own
                    # browser.get_context() lazily relaunches it.
                    await _close_browser_best_effort(transition["newPressure"])

            if guard.pressure in ("pause_crawl", "stop_search", "emergency"):
                # Same gate as _run_session's own per-batch check -- don't
                # start a brand-new session (which immediately launches a
                # real Chrome browser) while the system is already under
                # enough RAM pressure that every other pool has stopped
                # taking on new work.
                await _wait_ticks(stop_event, _TICK_S)
                continue

            session_minutes = random.uniform(_SESSION_MIN_MINUTES, _SESSION_MAX_MINUTES)
            logger.info("[keyword_farm_auto] Starting session (~%.1f min budget)", session_minutes)
            session_failed = False
            try:
                looked_up = await _run_session(state, stop_event, guard, session_minutes)
                if looked_up in (NO_WORK, NO_LOGIN):
                    wait_s = _IDLE_RETRY_S if looked_up == NO_WORK else _NO_LOGIN_RETRY_S
                    if looked_up == NO_WORK:
                        logger.info("[keyword_farm_auto] Nothing queued yet -- checking again in %ds.", wait_s)
                    await _wait_ticks(stop_event, wait_s)
                    continue
                state.sessionsRun += 1
                logger.info("[keyword_farm_auto] Session done -- %d domain(s) looked up.", looked_up)
            except Exception as err:  # noqa: BLE001 -- one bad session must not kill the whole loop
                session_failed = True
                state.lastError = str(err)
                logger.warning("[keyword_farm_auto] Session raised: %s", err)

            if stop_event.is_set():
                break

            if session_failed:
                # Short retry instead of the full throttle break -- see
                # _ERROR_BREAK_MIN_MINUTES's own comment for why.
                break_minutes = random.uniform(_ERROR_BREAK_MIN_MINUTES, _ERROR_BREAK_MAX_MINUTES)
            else:
                break_minutes = random.uniform(_BREAK_MIN_MINUTES, _BREAK_MAX_MINUTES)
            logger.info("[keyword_farm_auto] Sleeping ~%.1f min before next session.", break_minutes)
            await _wait_ticks(stop_event, break_minutes * 60)
    finally:
        state.running = False
        await _close_browser_best_effort("loop shutdown")
