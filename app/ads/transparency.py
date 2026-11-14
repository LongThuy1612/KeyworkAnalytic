"""Google Ads Transparency Center, read over plain HTTP (no browser, no login).

The public page loads its data from one RPC, SearchService/SearchCreatives, filtered by the advertised domain. Each
creative carries the advertiser's account id (AR...) and legal name; a page has 40 creatives and a continuation token.
That is enough for the one question this module answers, and nothing more detailed is kept:

    Does anyone other than the website's own advertiser run Google ads that point to this domain?

A third party paying for traffic to a merchant (an affiliate, a partner agency, a reseller) is a good sign for
a publisher deciding whether the programme is worth promoting.

`region` is a required block of the request (without it the RPC answers an empty body): {"1":1,"2":0,"3":2704} is what the
web page itself sends.
"""

import asyncio
import json
import random
import re
from urllib.parse import urlencode

import httpx

URL = "https://adstransparency.google.com/anji/_/rpc/SearchService/SearchCreatives?authuser="
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
    "x-same-domain": "1",
    "Origin": "https://adstransparency.google.com",
    "Referer": "https://adstransparency.google.com/?region=anywhere",
    "Accept": "*/*",
}
PAGE_SIZE = 40
_SECOND_LEVEL = {"co", "com", "org", "net", "ac", "gov", "edu"}


class RateLimited(Exception):
    """Google answered 429/403: stop for a while, this says nothing about the domain."""


class TransparencyError(Exception):
    pass


def registrable(domain: str) -> str:
    h = domain.lower().strip().removeprefix("www.")
    p = h.split(".")
    return ".".join(p[-3:]) if len(p) >= 3 and p[-2] in _SECOND_LEVEL and len(p[-1]) == 2 else ".".join(p[-2:])


def _alnum(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def brand_of(domain: str) -> str:
    return _alnum(registrable(domain).split(".")[0])


async def _search(client: httpx.AsyncClient, domain: str, token: str | None = None) -> dict:
    req: dict = {"2": PAGE_SIZE, "3": {"12": {"1": domain, "2": True}}, "7": {"1": 1, "2": 0, "3": 2704}}
    if token:
        req["4"] = token
    try:
        r = await client.post(URL, headers=HEADERS, content=urlencode({"f.req": json.dumps(req, separators=(",", ":"))}), timeout=30)
    except httpx.HTTPError as e:
        raise TransparencyError(f"transport: {type(e).__name__}") from e
    if r.status_code in (403, 429) or 300 <= r.status_code < 400:
        # Google's rate limit is a redirect to google.com/sorry (a CAPTCHA page), not a 429. It lifts after a while.
        raise RateLimited(f"{r.status_code} {r.headers.get('location', '')[:60]}".strip())
    if r.status_code != 200:
        raise TransparencyError(f"http {r.status_code}")
    if len(r.content) <= 2:
        return {}                                   # no ads point to this domain
    try:
        return r.json()
    except ValueError as e:
        raise TransparencyError("not json") from e


def _as_int(v) -> int | None:
    """The estimated total-ads range comes back as numbers or numeric strings depending on the size."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    return int(v) if isinstance(v, str) and v.isdigit() else None


def analyze(domain: str, creatives: list[dict], est: tuple[int | None, int | None], pages: int) -> dict:
    """creatives: the raw creative objects ("1" id, "12" advertiser name, "14" domain). Advertisers are grouped by
    normalised NAME (one company often has several accounts)."""
    brand = brand_of(domain)
    by_name: dict[str, dict] = {}
    for c in creatives:
        name = (c.get("12") or "").strip()
        key = _alnum(name) or str(c.get("1"))
        e = by_name.setdefault(key, {"name": name or "(unknown)", "ads": 0})
        e["ads"] += 1
    owner_match = 0
    owners: set[str] = set()
    if len(brand) >= 3:
        def _is_owner(key: str, entry: dict) -> bool:
            if len(brand) >= 4:
                return brand in key or (len(key) >= 4 and key in brand)
            # a 3-letter brand ("kit") would match any name that merely contains it: require a whole word
            return brand in {_alnum(w) for w in re.split(r"\W+", entry["name"])}
        owners = {k for k, v in by_name.items() if _is_owner(k, v)}
        owner_match = 1 if owners else 0
    if not owners and by_name:
        top_key, top = max(by_name.items(), key=lambda x: x[1]["ads"])
        total = sum(v["ads"] for v in by_name.values())
        if len(by_name) == 1 or top["ads"] / total >= 0.6:
            owners, owner_match = {top_key}, 2      # the advertiser that dominates is taken to be the website itself
    others = sorted(((k, v) for k, v in by_name.items() if k not in owners), key=lambda x: -x[1]["ads"])
    return {
        "has_ads": bool(creatives),
        "advertiser_count": len(by_name),
        "other_advertiser_count": len(others),
        "has_other_advertisers": bool(others) and owner_match > 0,
        "owner_match": owner_match,
        "sampled_ads": len(creatives),
        "sampled_pages": pages,
        "ads_range_low": est[0],
        "ads_range_high": est[1],
        "other_names": [v["name"][:60] for _, v in others[:3]],
    }


async def check_domain(client: httpx.AsyncClient, domain: str, max_pages: int = 3, delay_s: float = 0.7) -> dict:
    """Up to `max_pages` pages (120 creatives). Stops early on a short page. Raises RateLimited / TransparencyError."""
    d = registrable(domain)
    creatives: list[dict] = []
    est: tuple[int | None, int | None] = (None, None)
    token = None
    pages = 0
    for _ in range(max_pages):
        data = await _search(client, d, token)
        pages += 1
        got = data.get("1") or []
        creatives += got
        if pages == 1:
            est = (_as_int(data.get("4")), _as_int(data.get("5")))
        token = data.get("2")
        if len(got) < PAGE_SIZE or not token:
            break
        await asyncio.sleep(delay_s * random.uniform(0.8, 1.4))
    return analyze(d, creatives, est, pages)
