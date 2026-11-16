"""Drives the Google Ads Keyword Planner "Discover new keywords" UI to
export a full CSV per keyword, rather than scraping the results table's DOM
directly. This is the operator's own discovery (2026-08-15): the page has a
"Download keyword ideas" button that produces a UTF-16 TSV export (see
src/keyword_farm/csv_parser.py) containing every column the table shows
PLUS the seed keyword's and every idea's 12-month searches breakdown, which
is not otherwise recoverable from the DOM (the on-page sparkline/trend chart
has no simple text/attribute form to scrape). Using the built-in export
button is also a strictly gentler UI interaction than scraping a live table
(one click, one file write, vs. reading potentially hundreds of table rows
each time) -- lower automation "footprint" per keyword, which matters given
the accepted Terms-of-Service risk this whole farm already carries (see
CLAUDE.md and run_keyword_farm.py's own docstring).

Flow per keyword: type into the search box -> wait for results -> click
Download -> wait for the file to land in the downloads dir -> parse it ->
delete it (the operator's own "download and get data then delete the file"
instruction -- nothing about Google Ads needs that file to persist on disk
once its contents are in KeywordSignal).
"""

import asyncio
import logging
import os
import re
import time

from playwright.async_api import Page

from app.farm.csv_parser import parse_csv, read_export_file

logger = logging.getLogger("keyword_farm")

KEYWORD_PLANNER_URL = "https://ads.google.com/aw/keywordplanner/home"
OVERVIEW_URL = "https://ads.google.com/aw/overview"
_RESULTS_TIMEOUT_MS = 30_000
_DOWNLOAD_TIMEOUT_MS = 30_000
_ACCOUNT_PICKER_TIMEOUT_MS = 5_000
# Confirmed live (2026-08-27): download.save_as()/download.path() neither
# accept their own `timeout` kwarg -- the _DOWNLOAD_TIMEOUT_MS passed to
# page.expect_download() above only bounds waiting for the download EVENT
# to fire, not the file-write that follows once it has. When Chrome's
# actual download stalls after the event already fired (root cause of a
# multi-hour silent stall on the VPS, 2026-08-27 -- browser history showed
# the last-opened page was the results screen, i.e. past the click that
# starts the download, and the process never advanced or raised), these
# two calls could hang indefinitely with nothing else in this function
# bounding them. Wrapped in asyncio.wait_for() below using this timeout.
_SAVE_DOWNLOAD_TIMEOUT_S = 30.0
# Confirmed live via the operator's own screen recording (2026-08-15): the
# account-switcher dropdown takes a moment to actually render its full
# Recent/All-accounts list after the header switcher is clicked (the click
# itself succeeds instantly, but Playwright's auto-waiting only guarantees
# the click TARGET was actionable, not that content opening as a RESULT of
# the click has finished rendering) -- querying for dropdown contents
# before this delay elapses can miss the list entirely.
_DROPDOWN_RENDER_DELAY_MS = 800
# Delay between individual UI steps within lookup_keywords's own chip-input
# flow (click field, type, press Enter, etc). Raised from an earlier
# 300-400ms per operator instruction (2026-08-17): slow the automation down
# further to look less bot-like, on top of the per-batch delay already
# applied by run_keyword_farm.py between whole batches.
_INTER_STEP_DELAY_MS = 700

_NO_ACCOUNT_MARKER = "no results"

_MCC_ACCOUNT_NAME = os.environ.get("KEYWORD_FARM_MCC_NAME", "Long NT Manager")
# The specific client account (under the MCC) to select via the header
# switcher. Confirmed live via the operator's own screen recording
# (2026-08-15) that 264-579-6190 DOES work (contrary to an earlier,
# incorrect conclusion drawn from automation-only attempts) -- Keyword
# Planner loaded cleanly with "Select an active account: 264-579-6190" and
# no "No results" error. The earlier failures were a NAVIGATION bug, not an
# account problem: see _dismiss_account_picker's own docstring.
_CLIENT_ACCOUNT_ID = os.environ.get("KEYWORD_FARM_CLIENT_ACCOUNT_ID", "264-579-6190")


class KeywordPlannerAccountError(Exception):
    """Raised when the selected Ads account isn't usable by Keyword Planner
    yet (billing missing, still Draft) -- distinct from a normal per-keyword
    scrape failure so run_keyword_farm.py can stop the whole session instead
    of burning through the throttle budget retrying every keyword."""


async def _select_client_account(page: Page) -> None:
    """Selects _CLIENT_ACCOUNT_ID via the header account-switcher dropdown
    (the "Long NT Manager 919-156-0582 ▾" control next to the Google Ads
    logo) -- the exact flow confirmed via the operator's own screen
    recording (2026-08-15):
      1. Click the header switcher once. The dropdown (Recent list + "All
         Long NT Manager accounts" section) renders after a short delay --
         no second click needed, just a wait (_DROPDOWN_RENDER_DELAY_MS).
      2. Click the row matching _CLIENT_ACCOUNT_ID.
    A no-op if the header switcher isn't present (e.g. already on a page
    without it) or already shows _CLIENT_ACCOUNT_ID."""
    # Confirmed live (2026-08-17) that a page-wide get_by_text(
    # _CLIENT_ACCOUNT_ID) check here caused a false "already selected"
    # short-circuit -- the account id text can be present elsewhere on the
    # page (e.g. a "Recent" list item still in the DOM from a prior
    # picker interaction) even while the HEADER itself still shows the
    # MCC. Scoping the check to the header switcher control itself (the
    # same element clicked below) avoids that false positive.
    header_switcher = page.get_by_text(_MCC_ACCOUNT_NAME, exact=False).first
    if not await header_switcher.count():
        # No MCC name in the header at all -- either already on
        # _CLIENT_ACCOUNT_ID, or some other page state entirely; either
        # way there's no switcher to click.
        return
    if _CLIENT_ACCOUNT_ID in (await header_switcher.locator("xpath=..").inner_text()):
        return

    await header_switcher.click()
    await page.wait_for_timeout(_DROPDOWN_RENDER_DELAY_MS)

    client_entry = page.get_by_text(_CLIENT_ACCOUNT_ID, exact=False).first
    try:
        await client_entry.wait_for(state="visible", timeout=_RESULTS_TIMEOUT_MS)
    except Exception:
        debug_path = os.path.join(
            os.environ.get("TEMP", "."), f"keyword_farm_client_picker_debug_{int(time.time())}.png"
        )
        try:
            await page.screenshot(path=debug_path, full_page=True)
            logger.error("[SCRAPE] Client account %r not found in header switcher -- screenshot saved to %s", _CLIENT_ACCOUNT_ID, debug_path)
        except Exception:  # noqa: BLE001
            pass
        raise
    await client_entry.click()
    try:
        await client_entry.wait_for(state="hidden", timeout=_RESULTS_TIMEOUT_MS)
    except Exception:  # noqa: BLE001
        pass


async def _open_keyword_planner_via_sidebar(page: Page) -> None:
    """Navigates to Keyword Planner via Tools -> Planning -> Keyword
    Planner in the left sidebar, rather than page.goto(KEYWORD_PLANNER_URL)
    directly. Confirmed via the operator's own screen recording (2026-08-15)
    that this distinction matters: a direct goto() re-triggers the full
    "Select a Google Ads account" picker dialog from scratch every time
    (Google Ads doesn't remember the last choice across page loads for that
    specific entry point), while sidebar navigation preserves whatever
    client account is already active in the header -- this was the actual
    root cause of every earlier "No results" / wrong-account failure, not
    an issue with any particular client account.

    Confirmed live (2026-08-17) that this sidebar is NOT a flyout that only
    appears after clicking "Tools" -- once already on any Tools-family page
    (e.g. the previous batch's Keyword Planner results, or "Asset studio"
    reached via a previous mis-click), the Tools sidebar is already
    expanded and "Tools" itself shows as the active section. Clicking
    "Tools" again in that state toggled/collapsed it instead of opening it,
    and "Planning" stayed collapsed (its own toggle arrow never flipped),
    so "Keyword Planner" never appeared in the DOM and the caller timed
    out. Fix: only click "Tools" when the sidebar isn't already showing a
    "Planning" section, and always click "Planning" itself (not gated on
    is_visible(), which was true even while its own children stayed
    collapsed) to make sure it's expanded rather than assuming one click
    from a cold start always suffices."""
    planning = page.get_by_text("Planning", exact=True).first
    if not await planning.count():
        tools_icon = page.get_by_text("Tools", exact=True).first
        await tools_icon.click()
        await page.wait_for_timeout(_DROPDOWN_RENDER_DELAY_MS)
        planning = page.get_by_text("Planning", exact=True).first

    keyword_planner_link = page.get_by_text("Keyword Planner", exact=True).first
    for _ in range(3):
        if await keyword_planner_link.count() and await keyword_planner_link.is_visible():
            break
        if await planning.count():
            await planning.click()
            await page.wait_for_timeout(_DROPDOWN_RENDER_DELAY_MS)
        keyword_planner_link = page.get_by_text("Keyword Planner", exact=True).first
    await keyword_planner_link.click()
    await page.wait_for_selector(
        "text=/Discover new keywords|Select an active account/i", timeout=_RESULTS_TIMEOUT_MS
    )


async def _dismiss_account_picker(page: Page, _depth: int = 0) -> None:
    """Gets `page` onto a usable Keyword Planner screen for
    _CLIENT_ACCOUNT_ID, following the exact flow confirmed via the
    operator's own screen recording (2026-08-15):
      1. If a "Select a Google Ads account" dialog is showing (first-ever
         navigation in the profile), click the MCC entry to drill into it.
      2. Select _CLIENT_ACCOUNT_ID via the header switcher
         (_select_client_account).
      3. If that lands on Google Ads' own "Create your first campaign"
         wizard for the account (happens even for accounts that work fine
         once past this screen -- it's that account's own configured
         "home", unrelated to billing), click the "Google Ads" logo to back
         out to the MCC Overview, THEN navigate to Keyword Planner via the
         sidebar (Tools -> Planning -> Keyword Planner) rather than
         page.goto() -- see _open_keyword_planner_via_sidebar's own
         docstring for why that distinction is the actual fix.
    Capped at 3 recursive attempts to avoid looping forever if the account
    genuinely never settles."""
    picker_heading = page.get_by_text("Select a Google Ads account", exact=False)
    try:
        await picker_heading.wait_for(state="visible", timeout=_ACCOUNT_PICKER_TIMEOUT_MS)
        mcc_entry = page.get_by_text(_MCC_ACCOUNT_NAME, exact=False).first
        await mcc_entry.click()
        await page.wait_for_selector(
            "text=/Keyword Planner|Discover new keywords|Overview/i", timeout=_RESULTS_TIMEOUT_MS
        )
    except Exception:  # noqa: BLE001 -- step 1 dialog wasn't showing, move on
        pass

    await _select_client_account(page)

    first_campaign_wizard = page.get_by_text("Create your first campaign", exact=False)
    try:
        await first_campaign_wizard.wait_for(state="visible", timeout=_ACCOUNT_PICKER_TIMEOUT_MS)
        landed_on_wizard = True
    except Exception:  # noqa: BLE001
        landed_on_wizard = False

    if landed_on_wizard:
        if _depth >= 3:
            debug_path = os.path.join(
                os.environ.get("TEMP", "."), f"keyword_farm_wizard_loop_debug_{int(time.time())}.png"
            )
            try:
                await page.screenshot(path=debug_path, full_page=True)
                logger.error("[SCRAPE] Still on 'Create your first campaign' after %d attempts -- screenshot saved to %s", _depth, debug_path)
            except Exception:  # noqa: BLE001
                pass
            raise KeywordPlannerAccountError(
                f"Client account {_CLIENT_ACCOUNT_ID!r} keeps landing on the "
                "'Create your first campaign' wizard instead of Keyword Planner."
            )
        logo = page.get_by_text("Google Ads", exact=False).first
        await logo.click()
        await page.wait_for_selector("text=/Overview/i", timeout=_RESULTS_TIMEOUT_MS)
        await _open_keyword_planner_via_sidebar(page)
        await _dismiss_account_picker(page, _depth=_depth + 1)
        return

    # Not on the wizard -- but selecting the client account only lands on
    # that account's OWN Overview page (confirmed live, 2026-08-15), not
    # Keyword Planner itself. Navigate there explicitly via the sidebar
    # (same as the wizard-recovery path above) rather than assuming
    # _select_client_account's click already put us on the right screen.
    if "keywordplanner" not in page.url:
        await _open_keyword_planner_via_sidebar(page)

    try:
        await page.wait_for_selector(
            "text=/Discover new keywords|Keywords you provided|Avg\\. monthly searches|Select an active account/i",
            timeout=_RESULTS_TIMEOUT_MS,
        )
    except Exception:
        debug_path = os.path.join(
            os.environ.get("TEMP", "."), f"keyword_farm_post_client_click_debug_{int(time.time())}.png"
        )
        try:
            await page.screenshot(path=debug_path, full_page=True)
            logger.error("[SCRAPE] Discover-keywords screen never appeared after selecting client account -- screenshot saved to %s (url=%s)", debug_path, page.url)
        except Exception:  # noqa: BLE001
            pass
        raise


async def ensure_discover_page(page: Page) -> None:
    """Gets `page` onto Keyword Planner's "Discover new keywords" screen for
    _CLIENT_ACCOUNT_ID. Starts from the MCC Overview page (NOT
    keywordplanner/home directly) -- confirmed via the operator's own
    screen recording (2026-08-15) that starting from Overview and letting
    _dismiss_account_picker drive account selection + sidebar navigation
    from there is what actually works; a direct goto(KEYWORD_PLANNER_URL)
    re-triggers Google Ads' account picker from scratch every time. Raises
    KeywordPlannerAccountError if the account picker shows no usable
    account, or if the account can't get past its own onboarding wizard
    (see _dismiss_account_picker's own docstring)."""
    if "ads.google.com" not in page.url:
        await page.goto(OVERVIEW_URL, wait_until="domcontentloaded")

    await _dismiss_account_picker(page)

    no_results = page.get_by_text(_NO_ACCOUNT_MARKER, exact=False)
    try:
        await no_results.wait_for(state="visible", timeout=3_000)
    except Exception:  # noqa: BLE001 -- timeout just means the marker wasn't there, which is the good case
        return

    debug_path = os.path.join(
        os.environ.get("TEMP", "."), f"keyword_farm_no_results_debug_{int(time.time())}.png"
    )
    try:
        await page.screenshot(path=debug_path, full_page=True)
        logger.error("[SCRAPE] 'No results' marker matched -- screenshot saved to %s (url=%s)", debug_path, page.url)
    except Exception:  # noqa: BLE001
        pass

    raise KeywordPlannerAccountError(
        "Keyword Planner shows 'No results' for the account picker -- "
        "the selected Ads account has no usable billing setup yet."
    )


async def _set_worldwide_english(page: Page) -> None:
    """Sets the "Discover new keywords" dialog's location/language chips to
    "All locations" / "English" instead of the account's own default
    (Vietnam / Vietnamese here, since the MCC/client accounts were set up
    for a Vietnam-based operation -- see CLAUDE.md). Confirmed via the
    operator's own screen recording (2026-08-15) that this setting persists
    for the rest of the browsing session once set (a later "bun cha" query
    in the same session still showed "All locations"/"English" without
    re-setting it) -- so this only needs to run once per session, on the
    FIRST call into the dialog, not before every keyword. Flow observed:
      Language: click the "Vietnamese (default)" chip -> type "en" into
        the language search box that opens -> click "English".
      Location: click the "Vietnam" chip -> a Location dialog opens with
        "Vietnam country" listed under "Locations (1)" -> click that row's
        own X (remove) button -> click Save. An empty location list is
        what Keyword Planner treats as "no location restriction" (there is
        no explicit "Worldwide" option in this UI).
    Best-effort: logs and continues rather than raising if either chip
    isn't found, since a wrong location/language skews volume numbers but
    doesn't make the lookup itself fail.
    """
    try:
        language_chip = page.get_by_text("Vietnamese (default)", exact=False).first
        if await language_chip.count():
            await language_chip.click()
            await page.wait_for_timeout(400)
            # Confirmed live (2026-08-15) that this dropdown's search box
            # placeholder text doesn't reliably match a "search languages"
            # regex (exact wording/case not confirmed, and Playwright's
            # get_by_placeholder timed out against it) -- falling back to
            # the visible, focused text input inside the now-open dropdown
            # instead of depending on placeholder text at all.
            lang_search = page.get_by_placeholder(re.compile("search languages", re.IGNORECASE)).first
            if not await lang_search.count():
                lang_search = page.locator("input[type='text']:visible").last
            await lang_search.click()
            await lang_search.fill("en")
            await page.wait_for_timeout(400)
            english_option = page.get_by_text("English", exact=True).first
            await english_option.click()
            await page.wait_for_timeout(400)
    except Exception as err:  # noqa: BLE001 -- best-effort, wrong language shouldn't fail the whole lookup
        logger.warning("[SCRAPE] Could not switch language to English: %s", err)

    try:
        location_chip = page.get_by_text("Vietnam", exact=False).first
        if await location_chip.count():
            await location_chip.click()
            await page.wait_for_timeout(400)
            # Confirmed live (2026-08-17, operator observation): the dialog
            # has TWO remove (x/circle) icons -- one on the "Locations (1)"
            # header row and one on the "Vietnam country" row itself -- and
            # clicking EITHER ONE clears the whole location list. Several
            # page.mouse.click() attempts at computed absolute coordinates
            # (measured directly off debug screenshots, so the target pixel
            # itself was correct) still didn't register -- most likely
            # page.mouse.click() landed on a different stacked
            # element/overlay at that pixel rather than the actual icon
            # (raw mouse clicks bypass Playwright's own actionability
            # checks, unlike locator.click()). Switching to Playwright's
            # own CSS-based locators for the visible circle-x icons within
            # the dialog instead of guessing pixel coordinates at all.
            dialog_panel = page.locator("text=Location").locator(
                "xpath=ancestor::*[contains(@class,'pane') or contains(@class,'dialog') or contains(@class,'modal')][1]"
            )
            remove_icons = dialog_panel.locator("mat-icon, svg, [class*='remove' i], [class*='close' i]")
            for _ in range(5):
                if not await page.get_by_text("Vietnam country", exact=False).count():
                    break
                icon_count = await remove_icons.count()
                if icon_count == 0:
                    break
                clicked = False
                for i in range(icon_count):
                    try:
                        await remove_icons.nth(i).click(timeout=2_000)
                        clicked = True
                        break
                    except Exception:  # noqa: BLE001 -- try the next candidate icon
                        continue
                if not clicked:
                    break
                await page.wait_for_timeout(400)

            debug_path = os.path.join(
                os.environ.get("TEMP", "."), f"keyword_farm_location_removed_debug_{int(time.time())}.png"
            )
            if await page.get_by_text("Vietnam country", exact=False).count():
                try:
                    await page.screenshot(path=debug_path, full_page=True)
                    logger.warning("[SCRAPE] 'Vietnam country' still present after remove attempts -- screenshot saved to %s", debug_path)
                except Exception:  # noqa: BLE001
                    pass

            # Confirmed live (2026-08-17): a PAGE-WIDE get_by_text("Save")
            # resolved to a "not visible" element -- some other, unrelated
            # "Save" text elsewhere in the DOM matched before the actual
            # dialog's own Save button. Scoping to dialog_panel (defined
            # above for the remove icons) targets the right one.
            save_button = dialog_panel.get_by_text("Save", exact=True).first
            if await save_button.count():
                await save_button.click(timeout=5_000)
            await page.wait_for_timeout(400)
            # If the dialog is still open (Save was disabled/no-op because
            # the location wasn't actually removed), Cancel out rather than
            # leaving a modal overlay blocking every subsequent click on
            # the page for the rest of the session -- confirmed live that
            # this overlay intercepts pointer events on the keyword search
            # box, silently breaking every later lookup_keyword() call.
            # Checking is_visible() rather than count() here -- confirmed
            # live that the dialog's own placeholder text stays present but
            # hidden in the DOM (Angular tends to keep closed dialogs
            # mounted-but-hidden rather than removing them) even after a
            # successful Save, so a plain count() check false-positives
            # into an unnecessary 30s Cancel-button timeout.
            location_input = page.get_by_placeholder("Enter a location to include", exact=False).first
            if await location_input.count() and await location_input.is_visible():
                cancel_button = dialog_panel.get_by_text("Cancel", exact=True).first
                if await cancel_button.count():
                    await cancel_button.click(timeout=5_000)
                    await page.wait_for_timeout(400)
    except Exception as err:  # noqa: BLE001
        logger.warning("[SCRAPE] Could not clear location to All locations: %s", err)


# Keyword Planner's own hard limit, confirmed live (2026-08-17): submitting
# more than 7 keywords in one "Discover new keywords" query silently drops
# the excess (every keyword after the 7th then fails downstream since its
# chip was never actually created). run_keyword_farm.py's own batches are
# sized 3-5, well under this, but this cap stays as a hard safety net for
# any other caller.
_MAX_KEYWORDS_PER_BATCH = 7


async def _return_to_keyword_planner_home(page: Page) -> None:
    """Navigates back to Keyword Planner's own two-tile home screen
    ("Discover new keywords" / "Get search volume and forecasts") via the
    "Keyword Planner" sidebar link, so every batch -- including the first --
    starts from the same known-good screen. Operator decision (2026-08-17,
    end of session): the alternative ("State 2", reusing the combined
    search box left over from the previous batch's results page) proved
    unreliable across many fix attempts (see git history of this file for
    the full trail -- viewport, duplicate-DOM-node, and hidden-element
    issues all traced back to that leftover results-page state). Going back
    to a clean home screen and always driving the same chip-input dialog
    flow ("State 1" in the old naming) avoids that whole class of bug,
    trading a bit of extra navigation time for reliability across batches.

    Reuses _open_keyword_planner_via_sidebar (Tools -> Planning -> Keyword
    Planner) rather than a direct link/goto -- the same distinction that
    matters for the very first navigation into Keyword Planner (see that
    helper's own docstring) also applies here: Keyword Planner results
    pages don't reliably expose a plain "Keyword Planner" breadcrumb/sidebar
    text to click directly, so going through the same Tools menu path this
    module already trusts avoids introducing a second, less-proven
    navigation route."""
    await _open_keyword_planner_via_sidebar(page)
    await page.wait_for_timeout(_INTER_STEP_DELAY_MS)


async def lookup_keywords(page: Page, keywords: list[str]) -> list[dict] | None:
    """Looks up up to _MAX_KEYWORDS_PER_BATCH `keywords` in one Keyword
    Planner query, and reads back ONLY the seed rows (one per input
    keyword) -- "Keyword ideas" suggestion rows are downloaded as part of
    the same CSV export but dropped during parsing (see
    csv_parser.parse_csv's own docstring; explicit operator instruction:
    batch multiple keywords per UI interaction instead of 1, keep only the
    seeds).

    Every batch -- including the first -- starts by returning to Keyword
    Planner's own home screen (_return_to_keyword_planner_home) and driving
    the "Discover new keywords" chip-input dialog from scratch. An earlier
    version tried to reuse a second, "already have results" screen state
    for batch 2+ instead of navigating back; that state turned out to be
    unreliable (duplicate hidden/visible DOM nodes sharing the same
    placeholder, see git history) and was dropped per operator decision
    (2026-08-17) in favor of this simpler, single-code-path flow. Delays
    between steps are intentionally generous (_INTER_STEP_DELAY_MS) per
    explicit operator instruction to slow the automation down and look less
    bot-like, on top of the per-batch delay already applied by the caller.
    """
    keywords = keywords[:_MAX_KEYWORDS_PER_BATCH]

    try:
        await _return_to_keyword_planner_home(page)

        discover_dialog = page.get_by_text("Discover new keywords", exact=False).first
        await discover_dialog.click()
        await page.wait_for_timeout(_INTER_STEP_DELAY_MS)
        await page.wait_for_selector(
            "text=/Start with keywords|Try .meal delivery./i", timeout=_ACCOUNT_PICKER_TIMEOUT_MS
        )
        await page.wait_for_timeout(_INTER_STEP_DELAY_MS)

        # Switching location/language only needs to happen once per
        # session, on the first-ever query (it persists across later
        # queries, see _set_worldwide_english's own docstring).
        if await page.get_by_text("Vietnamese (default)", exact=False).count():
            await _set_worldwide_english(page)
            await page.wait_for_timeout(_INTER_STEP_DELAY_MS)

        for keyword in keywords:
            target_input = page.get_by_placeholder(
                re.compile('meal delivery|leather boots|products.*services', re.IGNORECASE)
            ).first
            if not await target_input.count():
                target_input = page.get_by_placeholder(re.compile("add another keyword", re.IGNORECASE)).first
            await target_input.click(timeout=10_000)
            await page.wait_for_timeout(_INTER_STEP_DELAY_MS)
            await target_input.fill(keyword)
            await page.wait_for_timeout(_INTER_STEP_DELAY_MS)
            await target_input.press("Enter")  # commits the typed text into a keyword chip, does NOT submit the form
            await page.wait_for_timeout(_INTER_STEP_DELAY_MS)

        get_results = page.get_by_role("button", name=re.compile("get results", re.IGNORECASE))
        await get_results.click(timeout=10_000)
    except Exception:
        debug_path = os.path.join(
            os.environ.get("TEMP", "."), f"keyword_farm_lookup_input_debug_{int(time.time())}.png"
        )
        try:
            await page.screenshot(path=debug_path, full_page=True)
            logger.error("[SCRAPE] Could not find/fill the keyword search input for keywords=%r -- screenshot saved to %s (url=%s)", keywords, debug_path, page.url)
        except Exception:  # noqa: BLE001
            pass
        raise

    try:
        await page.wait_for_selector("text=/Keywords you provided|Avg\\. monthly searches/i", timeout=_RESULTS_TIMEOUT_MS)
    except Exception:  # noqa: BLE001
        logger.warning("[SCRAPE] No results table appeared for keywords=%r within timeout", keywords)
        return None

    download_button = page.get_by_text("Download keyword ideas", exact=False).first
    if not await download_button.count():
        logger.warning("[SCRAPE] Download button not found for keywords=%r", keywords)
        return None

    # Confirmed live (2026-08-15, operator screenshot): "Download keyword
    # ideas" doesn't download directly -- it opens a small format-choice
    # menu ("Keyword ideas" / ".csv" / "Google Sheets"). Only clicking
    # ".csv" in that menu actually triggers the download event.
    await download_button.click()
    await page.wait_for_timeout(_DROPDOWN_RENDER_DELAY_MS)
    csv_option = page.get_by_text(".csv", exact=False).first
    await csv_option.wait_for(state="visible", timeout=_ACCOUNT_PICKER_TIMEOUT_MS)
    async with page.expect_download(timeout=_DOWNLOAD_TIMEOUT_MS) as download_info:
        await csv_option.click()
    download = await download_info.value

    # Confirmed live (2026-08-17): the search-box-not-visible failures on
    # every batch after the first happened immediately after a download,
    # and debug screenshots showed the results page rendered normally with
    # nothing obviously covering the search box -- but the "Download
    # keyword ideas" format-choice dropdown's own closing animation/focus
    # return may still be settling right when the NEXT batch's
    # scroll_into_view_if_needed() + click ran. Pressing Escape and giving
    # the page a beat here, right after the click that triggers the
    # download, is cheap insurance against that dropdown (or its focus
    # trap) still being considered "in the way" when this function is
    # called again for the next batch.
    await page.keyboard.press("Escape")
    await page.wait_for_timeout(500)

    tmp_path = os.path.join(
        os.environ.get("TEMP", "/tmp"), f"keyword_farm_{int(time.time() * 1000)}.csv"
    )
    try:
        await asyncio.wait_for(download.save_as(tmp_path), timeout=_SAVE_DOWNLOAD_TIMEOUT_S)
    except asyncio.TimeoutError as err:
        raise RuntimeError(f"download.save_as() timed out after {_SAVE_DOWNLOAD_TIMEOUT_S}s for keywords={keywords!r} -- Chrome download likely stalled") from err

    # Confirmed live (2026-08-18): save_as() COPIES from Playwright's own
    # download artifact -- it does not move or delete it. Because browser.py
    # configures downloads_path=DOWNLOADS_DIR on the persistent context,
    # Chrome independently persists that original artifact into
    # .keyword_farm_downloads/ regardless of save_as()'s destination, and it
    # was never being cleaned up (108 leftover CSVs found accumulated there
    # since 2026-08-15). download.path() is the real path to that original
    # artifact -- delete it too, not just tmp_path.
    try:
        original_path = await asyncio.wait_for(download.path(), timeout=_SAVE_DOWNLOAD_TIMEOUT_S)
    except asyncio.TimeoutError:
        original_path = None  # save_as() above already succeeded -- losing the cleanup path isn't fatal, just a leftover file

    try:
        raw = read_export_file(tmp_path)
        rows = parse_csv(raw, keywords)
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        if original_path:
            try:
                os.remove(original_path)
            except OSError:
                pass

    if not rows:
        logger.warning("[SCRAPE] Downloaded CSV parsed to zero seed rows for keywords=%r", keywords)
        return None

    return rows
