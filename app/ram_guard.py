"""Port of the _ramMonitor tick in crawl-affiliate.service.js's
_runContinuousCrawl  -- a 4-tier RAM pressure ladder (idle_drain / pause_crawl
/ stop_search / emergency) with hysteresis, plus best-effort CPU high-usage
logging.

NOT ported here (no Python equivalent exists yet to act on): BrowserPool
drainIdle()/dynamic resize calls under each pressure tier  -- those depend on
Tier 2 (Playwright) infrastructure that doesn't exist in this codebase yet.
This module only tracks and exposes the pressure STATE; wiring pressure
changes to actual throttling (pausing crawl workers, reducing concurrency)
is the orchestrator's job once a multi-worker orchestrator exists  -- see
RamGuard.pressure's docstring for the contract callers should follow.
"""

import asyncio
import gc
import os
import time

import psutil

RAM_IDLE_DRAIN = int(os.environ.get("RAM_IDLE_DRAIN_PCT", "82"))
RAM_PAUSE_CRAWL = int(os.environ.get("RAM_PAUSE_PCT", "87"))
RAM_EMERGENCY = int(os.environ.get("RAM_EMERGENCY_PCT", "95"))
RAM_HYSTERESIS = int(os.environ.get("RAM_HYSTERESIS_PCT", "5"))
CPU_HIGH_PCT = int(os.environ.get("CPU_HIGH_PCT", "90"))

_ENTRY_THRESHOLD = {
    "emergency": RAM_EMERGENCY,
    "stop_search": None,  # computed per-instance  -- see RamGuard.__init__ (depends on ram_max)
    "pause_crawl": RAM_PAUSE_CRAWL,
    "idle_drain": RAM_IDLE_DRAIN,
}


class RamGuard:
    """Tracks RAM/CPU pressure state. Call `tick()` in a loop (see run_forever
    below) or once per orchestrator iteration; read `.pressure` to decide
    whether to throttle/pause crawl work.

    pressure values, in ascending severity: None | 'idle_drain' | 'pause_crawl'
    | 'stop_search' | 'emergency'. Same semantics as the Node original:
    - 'idle_drain': informational only, no throttling expected.
    - 'pause_crawl'/'stop_search': orchestrator should stop starting new
      crawl work (existing in-flight work may finish).
    - 'emergency': orchestrator should abort in-flight work immediately.
    """

    def __init__(self, ram_max: int | None = None):
        # ram_max (from a caller-configurable run, mirrors discoverAuto's
        # ramMax param) overrides RAM_STOP_SEARCH's default  -- clamped to stay
        # strictly between RAM_PAUSE_CRAWL and RAM_EMERGENCY so the 4
        # thresholds stay correctly ordered regardless of what's passed.
        #
        # Default (no ram_max given -- the common case, e.g. a plain
        # POST /discover-auto with no ramMax) is the MIDPOINT of (RAM_PAUSE_
        # CRAWL, RAM_EMERGENCY), not RAM_PAUSE_CRAWL + 1. The old +1 default
        # put stop_search only 1 point above pause_crawl (e.g. 87 -> 88 at
        # this codebase's actual .env values) -- tick()'s hysteresis (see its
        # own comment) only guards against flip-flopping back DOWN once a
        # tier is entered, not against oscillating between adjacent tiers
        # while RAM hovers in that same narrow 1-point band on the way up,
        # so a session with no explicit ramMax could bounce pause_crawl <->
        # stop_search on nearly every tick whenever RAM sat near 87-88%.
        # A midpoint default gives both bands roughly the room the original
        # 4-tier ladder design intended (idle_drain/pause_crawl/emergency are
        # all several points apart) without changing behavior for any caller
        # that already passes its own ram_max.
        effective_ram_max = ram_max if ram_max is not None else (RAM_PAUSE_CRAWL + RAM_EMERGENCY) // 2
        self.ram_stop_search = min(max(effective_ram_max, RAM_PAUSE_CRAWL + 1), RAM_EMERGENCY - 1)
        self.pressure: str | None = None
        self.ram_percent = 0
        self.cpu_percent = 0
        self._cpu_high_since: float = 0
        # This process's own RSS -- system-wide ram_percent above can't tell
        # "this Python process is leaking" apart from "something else on the
        # box is using RAM"; tracked separately so that distinction is
        # actually diagnosable (see heartbeat/log surfacing at call sites)
        # instead of guessed at.
        self._proc = psutil.Process()
        self.process_rss_mb = 0
        self._baseline_rss_mb: float | None = None
        self.process_rss_growth_mb = 0.0

    def _entry_threshold_for(self, pressure: str | None) -> int:
        if pressure == "emergency":
            return RAM_EMERGENCY
        if pressure == "stop_search":
            return self.ram_stop_search
        if pressure == "pause_crawl":
            return RAM_PAUSE_CRAWL
        if pressure == "idle_drain":
            return RAM_IDLE_DRAIN
        return 0

    def tick(self) -> dict:
        """Sample RAM/CPU once, update self.pressure, return a dict describing
        any transition (for logging by the caller)."""
        pct = round(psutil.virtual_memory().percent)
        self.ram_percent = pct
        self.cpu_percent = round(psutil.cpu_percent(interval=None))

        try:
            rss_mb = self._proc.memory_info().rss / (1024 * 1024)
        except Exception:  # noqa: BLE001 -- process introspection failing must not kill the guard loop
            rss_mb = self.process_rss_mb
        self.process_rss_mb = round(rss_mb)
        if self._baseline_rss_mb is None:
            self._baseline_rss_mb = rss_mb
        self.process_rss_growth_mb = round(rss_mb - self._baseline_rss_mb)

        now = time.monotonic()
        if self.cpu_percent >= CPU_HIGH_PCT:
            if self._cpu_high_since == 0:
                self._cpu_high_since = now
        else:
            self._cpu_high_since = 0

        prev_pressure = self.pressure
        transition: dict = {"pct": pct, "prevPressure": prev_pressure, "newPressure": prev_pressure, "changed": False}

        if pct >= RAM_EMERGENCY:
            if prev_pressure != "emergency":
                self.pressure = "emergency"
                transition.update(newPressure="emergency", changed=True)
        elif pct >= self.ram_stop_search:
            if prev_pressure not in ("stop_search", "emergency"):
                self.pressure = "stop_search"
                transition.update(newPressure="stop_search", changed=True)
        elif pct >= RAM_PAUSE_CRAWL:
            if prev_pressure not in ("pause_crawl", "stop_search", "emergency"):
                self.pressure = "pause_crawl"
                transition.update(newPressure="pause_crawl", changed=True)
        elif pct >= RAM_IDLE_DRAIN:
            if prev_pressure != "idle_drain":
                self.pressure = "idle_drain"
                transition.update(newPressure="idle_drain", changed=True)
        else:
            # Hysteresis: only clear pressure once RAM drops enough below the
            # entry threshold that triggered the current tier  -- prevents
            # rapid flip-flop when RAM hovers near a boundary.
            entry_threshold = self._entry_threshold_for(prev_pressure)
            if prev_pressure and pct < entry_threshold - RAM_HYSTERESIS:
                self.pressure = None
                transition.update(newPressure=None, changed=True)

        # Port of the Node original's own ladder: EVERY tier (including
        # idle_drain, the earliest/mildest one) proactively calls drainIdle()
        # + global.gc() the moment it's entered, not just at emergency --
        # Python previously did neither at any tier (only emergency's
        # cancel+restart), which is exactly why "pause_crawl" alone never
        # made RAM go back down: pausing only stops taking NEW work, it does
        # nothing about memory already held by finished-but-uncollected
        # objects (old HTML strings, response buffers, ONNX tensors). No
        # drainIdle() equivalent here -- this port's BrowserPool already
        # closes each domain's Chromium process right after use (no
        # idle-reuse to drain in the first place, see browser_pool.py) -- but
        # gc.collect() is a direct, low-risk match for Node's action and was
        # completely missing. Only fires on an INCREASE in pressure (not the
        # hysteresis clear-down above), matching Node's own placement.
        if transition["changed"] and transition["newPressure"] is not None:
            collected = gc.collect()
            print(f"[ram_guard] pressure -> {transition['newPressure']} ({pct}%) -- gc.collect() freed {collected} object(s)")

        return transition

    def cpu_high_duration_s(self) -> float:
        if self._cpu_high_since == 0:
            return 0
        return time.monotonic() - self._cpu_high_since

    def poll_interval_s(self) -> float:
        """Longer poll interval under high pressure  -- OS needs time to
        reclaim pages, no point sampling faster than that."""
        if self.pressure in ("emergency", "stop_search"):
            return 15.0
        if self.pressure:
            return 8.0
        return 10.0


async def run_forever(guard: RamGuard, on_transition=None, stop_event: asyncio.Event | None = None) -> None:
    """Self-rescheduling loop  -- same accumulate-then-trigger pattern used
    elsewhere in this codebase (see BEWeb's crawl-affiliate.service.js tick
    comments) rather than a fixed-interval scheduler, since the poll
    interval itself changes with pressure (see poll_interval_s)."""
    while not (stop_event and stop_event.is_set()):
        transition = guard.tick()
        if transition["changed"] and on_transition:
            on_transition(transition)

        cpu_high_s = guard.cpu_high_duration_s()
        if cpu_high_s > 30 and on_transition and int(cpu_high_s) % 60 < guard.poll_interval_s():
            print(f"[ram_guard] CPU {guard.cpu_percent}% >= {CPU_HIGH_PCT}% for {round(cpu_high_s)}s  -- consider reducing worker concurrency")

        try:
            if stop_event:
                await asyncio.wait_for(stop_event.wait(), timeout=guard.poll_interval_s())
            else:
                await asyncio.sleep(guard.poll_interval_s())
        except asyncio.TimeoutError:
            pass

