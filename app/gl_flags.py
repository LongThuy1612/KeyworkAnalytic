"""Chrome flags for machines without a real GPU (the Linux VPS runs headed
Chrome on Xvfb :99).

Confirmed live 2026-09-29: on that server, Chrome (branded 151 and Playwright's
bundled Chromium alike) can't create GPU command buffers ("ContextResult::
kTransientFailure: Failed to send GpuControl.CreateCommandBuffer", "WebGL
blocklisted") and the heavy Google pages (Gemini, Ads Keyword Planner) died with
"Aw, Snap! SIGTRAP". Since Chrome 137 the automatic software-GL fallback is
opt-in; launching with `--use-angle=swiftshader --enable-unsafe-swiftshader`
made a hand-opened Gemini tab (real Lady Gemini profile) load and stay up.

Only applied on Linux (Windows/macOS have a real GPU path) and only to the
persistent Google-facing browsers. BROWSER_SOFTWARE_GL=false turns it off.
Deliberately NOT applied to the anti-bot crawl browsers (Tier 2/3, search):
exposing a "SwiftShader" WebGL renderer could change their Cloudflare/bot
fingerprint and that hasn't been tested.
"""

import os
import sys


def software_gl_args() -> list[str]:
    if not sys.platform.startswith("linux"):
        return []
    if os.environ.get("BROWSER_SOFTWARE_GL", "true").strip().lower() != "true":
        return []
    return ["--use-angle=swiftshader", "--enable-unsafe-swiftshader"]
