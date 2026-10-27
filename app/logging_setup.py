"""Console + rotating file logging, same shape as notebooklm-docker's app.py
(50MB files, anything older than 5 days is cleaned by log_cleanup_task)."""

import asyncio
import glob
import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler

LOG_DIR = os.environ.get("LOG_DIR", "logs")
LOG_FILE = os.path.join(LOG_DIR, "service.log")
KEEP_DAYS = int(os.environ.get("LOG_KEEP_DAYS", "5"))


def install() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    rotating = RotatingFileHandler(LOG_FILE, maxBytes=50 * 1024 * 1024, backupCount=20, encoding="utf-8")
    rotating.setFormatter(fmt)
    root = logging.getLogger()
    root.handlers[:] = [console, rotating]
    root.setLevel(logging.INFO)
    for noisy in ("httpx", "httpcore", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


async def log_cleanup_task() -> None:
    while True:
        try:
            cutoff = time.time() - KEEP_DAYS * 86400
            for path in glob.glob(LOG_FILE + "*"):
                if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                    os.remove(path)
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(21600)
