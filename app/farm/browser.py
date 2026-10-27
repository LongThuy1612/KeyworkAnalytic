"""Persistent, logged-in Chromium profile for the Keyword Planner UI farm.

Deliberately its OWN profile dir, separate from src/lady_gemini's -- that
profile is a Google *consumer* account signed into Gemini; this one is
signed into Google Ads (the account behind the "Long NT Manager" MCC).
Mixing them into one profile would tie both services' fates together for no
benefit (a block on one account would look like it might implicate the
other, when they're unrelated logins). Otherwise the same lifecycle shape as
src/lady_gemini/browser.py: launch_persistent_context, headed (headless=
False) so the operator can RDP in for the one-time manual Google login, no
proxy (matches the operator's explicit instruction for the whole Lady
Gemini/Ads automation work -- see CLAUDE.md's Lady Gemini session notes for
why proxy is off the table here).

Originally standalone-only (scripts/run_keyword_farm.py), now ALSO driven
by the always-on server process via src/orchestrator/keyword_farm_auto.py
(opt-in per session through DiscoverAutoRequest.enableKeywordFarm) -- the
two are never run at once against the same profile in practice (the
operator runs one or the other), so the SingletonLock contention risk this
docstring used to warn about hasn't materialized, but get_context()'s own
re-entrancy check below still assumes single-caller use, not concurrent
callers racing each other.
"""

import asyncio
import logging
import os

from playwright.async_api import BrowserContext, async_playwright
from app.gl_flags import software_gl_args

logger = logging.getLogger("keyword_farm")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROFILE_DIR = os.environ.get(
    "KEYWORD_FARM_PROFILE_DIR", os.path.join(_REPO_ROOT, "data", "profile")
)
DOWNLOADS_DIR = os.environ.get(
    "KEYWORD_FARM_DOWNLOADS_DIR", os.path.join(_REPO_ROOT, "data", "downloads")
)

_playwright_manager = None
_browser_context: BrowserContext | None = None

# Bounds the best-effort close() of a dead context in get_context() below --
# same reasoning as browser_manager.py's own CLOSE_TIMEOUT_S: closing a
# context whose underlying process already died can itself hang without
# this, defeating the whole point of the liveness check.
CLOSE_TIMEOUT_S = 5.0


def has_saved_login() -> bool:
    """Same detection approach as src/lady_gemini/browser.py's
    is_anonymous_mode -- checks for Chrome's own session cookie DB rather
    than a flag this module sets itself, so it stays correct regardless of
    how the login actually happened (this module's own start_login() below,
    or a profile dir copied in some other way).

    Checks BOTH Default/Cookies and Default/Network/Cookies (2026-08-24 fix):
    confirmed live on the VPS's real Google Chrome build that Cookies lands
    directly under Default/, not Default/Network/ -- Default/Network/ never
    even gets created. A real login was completed and verified live (Cookies
    file present, 36KB, correct mtime) but this function still reported
    hasSavedLogin=false because it only checked the Network/ subpath, which
    a different Chrome build/version may still use. Checking both paths
    makes this correct across builds instead of assuming one fixed layout."""
    return os.path.exists(os.path.join(PROFILE_DIR, "Default", "Cookies")) or os.path.exists(
        os.path.join(PROFILE_DIR, "Default", "Network", "Cookies")
    )


async def start_login() -> None:
    """Opens a real, visible Chrome window on ads.google.com for the
    operator to log into by hand over RDP (same manual-login pattern as
    Lady Gemini's /auth/start-login) -- this module never handles a Google
    password itself. Leaves the browser open; the operator closes it once
    logged in, then the next get_context() call launches fresh against the
    now-authenticated profile dir."""
    context = await get_context()
    page = await context.new_page()
    await page.goto("https://ads.google.com/aw/keywordplanner/home")
    logger.info("[LOGIN] Chrome window opened on ads.google.com -- log in by hand over RDP, then leave this open until the session is confirmed saved.")


async def get_context() -> BrowserContext:
    """Launches the persistent profile if not already running; returns the
    existing one otherwise (mirrors src/lady_gemini/browser.py's init_
    browser() re-entrancy check, minus the lock -- this module is driven by
    one single-threaded script, not concurrent server request handlers, so
    the extra asyncio.Lock lady_gemini needs doesn't apply here).

    Checks the cached context's own browser.is_connected() before reusing
    it -- confirmed live (2026-08-27) as the root cause of keyword_farm_auto
    silently going quiet for hours on the VPS with StallWatchdog never
    noticing: when the underlying Chrome process dies mid-session (RAM
    pressure/OS kill -- same failure mode src/browser/browser_manager.py's
    own get_domain_context() already guards against, see that function's
    own docstring), the NEXT call here used to return the same dead
    _browser_context object unconditionally. context.new_page() against a
    context whose process is already gone then hangs indefinitely (no
    exception, no timeout) rather than failing fast -- keyword_farm_auto's
    own asyncio.Task never raises and never completes, so
    _keyword_farm_is_alive()'s `not task.done()` check keeps reporting it
    as healthy forever, and StallWatchdog's death-detection (which only
    fires on should_run and not is_alive) never triggers a restart. A dead
    context is discarded (best-effort close, bounded by CLOSE_TIMEOUT_S so
    the close itself can't also hang) and a fresh one launched in its
    place, same pattern as browser_manager.py's own fix."""
    global _playwright_manager, _browser_context

    if _browser_context is not None:
        browser = _browser_context.browser
        if browser is not None and browser.is_connected():
            return _browser_context
        logger.warning("[BROWSER] Cached context's browser is disconnected -- discarding and relaunching.")
        try:
            await asyncio.wait_for(_browser_context.close(), timeout=CLOSE_TIMEOUT_S)
        except Exception:  # noqa: BLE001 -- best-effort; the process is likely already gone
            pass
        _browser_context = None

    os.makedirs(PROFILE_DIR, exist_ok=True)
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)

    for lock_file in ("SingletonLock", "SingletonCookie"):
        path = os.path.join(PROFILE_DIR, lock_file)
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:  # noqa: BLE001
                pass

    _playwright_manager = await async_playwright().start()
    _browser_context = await _playwright_manager.chromium.launch_persistent_context(
        user_data_dir=PROFILE_DIR,
        headless=False,
        accept_downloads=True,
        # Explicit, not left to Chrome's own per-profile download setting --
        # confirmed live (2026-08-15) that omitting this left a real
        # "Download keyword ideas" click's resulting file unfindable in
        # either the OS user's normal Downloads folder or the profile dir
        # itself (channel="chrome" real Chrome, not Playwright's bundled
        # Chromium, appears to resolve its download location differently
        # than expected when this isn't pinned down). scrape.py's own
        # download.save_as() still explicitly places the file too, but
        # pinning this here removes one more place Chrome could silently
        # disagree about where the file landed.
        downloads_path=DOWNLOADS_DIR,
        ignore_default_args=["--enable-automation"],
        viewport=None,
        channel="chrome",
        args=[
            "--no-first-run",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--start-maximized",
            *software_gl_args(),
        ],
    )
    logger.info("[SYSTEM] Keyword farm Chromium profile started (%s).", PROFILE_DIR)
    return _browser_context


async def close_context() -> None:
    global _playwright_manager, _browser_context
    if _browser_context is not None:
        try:
            await _browser_context.close()
        except Exception:  # noqa: BLE001
            pass
        _browser_context = None
    if _playwright_manager is not None:
        try:
            await _playwright_manager.stop()
        except Exception:  # noqa: BLE001
            pass
        _playwright_manager = None
    logger.info("[SYSTEM] Keyword farm browser closed.")
