"""Service limits, in the spirit of notebooklm-docker's config.py (page cap,
idle timeout, queue timeout) applied to the job queue."""

import os

# Reject POST /analyze with 503 QUEUE_FULL when this many jobs are already
# waiting (notebooklm-docker rejects a request that waited QUEUE_TIMEOUT for a
# slot; for a durable queue the equivalent back-pressure is its depth).
MAX_QUEUE_PENDING = int(os.getenv("MAX_QUEUE_PENDING", 20000))

# A claimed job whose worker never reports back (crash, hung tab, proxy dead
# and the domain pushed back) is returned to the queue after this many seconds.
JOB_LEASE_S = int(os.getenv("JOB_LEASE_S", 7200))

# A job is abandoned (status=failed) after this many lease expiries.
JOB_MAX_ATTEMPTS = int(os.getenv("JOB_MAX_ATTEMPTS", 3))

# Crawl-DB read budget (see app/feeder.py). The feeder tick itself is local
# (SQLite only); the database is queried only when a pull is due: the tool's queue is
# under FEED_LOW_WATER AND FEED_MIN_INTERVAL_S has passed since its last pull.
# Automatic candidate feeding from affiliate_programs (every crawled domain) is OFF by default: Keyword Planner and
# lookups are scarce, so they run only for the domains handed in through POST /keywords
# (the clean dataset). Set FEED_AUTO=true to bring the old behaviour back.
# The website's own database (PostgreSQL, read-only). When set, both lookups follow the programmes published on the
# site -- the clean dataset -- and the automatic feeding is switched on for them (see app/sitedb.py).
SITE_DB_URL = os.getenv("SITE_DB_URL", "").strip()
FEED_AUTO = os.getenv("FEED_AUTO", "true" if SITE_DB_URL else "false").lower() == "true"

FEED_LOW_WATER = int(os.getenv("FEED_LOW_WATER", 200))
FEED_BATCH = int(os.getenv("FEED_BATCH", 1000))
FEED_INTERVAL_S = int(os.getenv("FEED_INTERVAL_S", 30))
FEED_MIN_INTERVAL_S = int(os.getenv("FEED_MIN_INTERVAL_S", 600))
FEED_EXHAUSTED_WAIT_S = int(os.getenv("FEED_EXHAUSTED_WAIT_S", 3600))
FEED_CURSOR_RESET_S = int(os.getenv("FEED_CURSOR_RESET_S", 86400))
FEED_RETRY_INTERVAL_S = int(os.getenv("FEED_RETRY_INTERVAL_S", 21600))

# Auto-fed jobs already finished are only re-queued after this long (the database's
# own retry tiers decide whether the domain is still a candidate).
REQUEUE_AFTER_S = int(os.getenv("REQUEUE_AFTER_S", 1209600))

# Outbox -> the database in one transaction: when FLUSH_MIN_ROWS rows are waiting or the
# oldest has waited FLUSH_MAX_AGE_S (lookups here are sparse: ~10 rows a minute).
FLUSH_MIN_ROWS = int(os.getenv("FLUSH_MIN_ROWS", 20))
FLUSH_MAX_AGE_S = int(os.getenv("FLUSH_MAX_AGE_S", 300))
FLUSH_BATCH = int(os.getenv("FLUSH_BATCH", 200))

# A domain is looked up again after this many days (every lookup overwrites its row in "KeywordSignal").
KEYWORD_RECHECK_DAYS = int(os.getenv("KEYWORD_RECHECK_DAYS", 30))
