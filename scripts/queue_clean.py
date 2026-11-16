"""Hand a list of domains (the clean dataset) to a running KeyworkAnalytic as Keyword Planner jobs.

    python scripts/queue_clean.py clean_domains.txt [--keywords http://localhost:8101] [--priority 1]

Accepts a text file (one domain per line) or a JSON list (strings, or objects with a "domain" key).
"""

import argparse
import json

import httpx


def load(path: str) -> list[str]:
    raw = open(path, encoding="utf-8").read()
    try:
        data = json.loads(raw)
        items = [d["domain"] if isinstance(d, dict) else d for d in data]
    except ValueError:
        items = [line.strip() for line in raw.splitlines() if line.strip()]
    return list(dict.fromkeys(str(d).lower() for d in items))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--keywords", default="http://localhost:8101")
    ap.add_argument("--priority", type=int, default=1)
    a = ap.parse_args()
    domains = load(a.file)
    print(f"{len(domains)} domain(s)")
    with httpx.Client(timeout=60) as c:
        for i in range(0, len(domains), 2000):
            chunk = domains[i:i + 2000]
            r = c.post(f"{a.keywords}/keywords", json={"domains": chunk, "priority": a.priority})
            print("keywords:", r.status_code, r.text[:120])


main()
