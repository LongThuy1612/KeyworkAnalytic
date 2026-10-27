"""Derives a brand-name search keyword from a bare domain -- e.g.
"n8n.io" -> "n8n", "get-response.com" -> "get response". Intentionally
simple (strip TLD, de-hyphenate) rather than trying to guess a "real" brand
name -- Keyword Planner still returns a usable volume figure for a mangled
guess (worst case it's low/zero, which is itself informative), and the
alternative (an LLM call per domain to guess the brand name) would burn far
more of this farm's deliberately-scarce throughput budget than it's worth
for a first pass.
"""

import re

AFFILIATE_SUBDOMAINS = ["affiliates", "affiliate", "partners", "partner", "refer", "promoters", "ambassador", "ambassadors"]

_TLD_RE = re.compile(
    r"\.(com|net|org|io|co|app|dev|ai|shop|store|xyz|online|site|biz|info|"
    r"cc|me|tv|so|to|ly|gg|id|ke|ng|za|in|vn|sg|my|ph|th|jp|kr|cn|hk|tw|"
    r"us|uk|ca|de|fr|es|it|nl|se|no|dk|fi|pl|ru|br|mx|au|nz|"
    # Added 2026-08-17 -- "investors.club" was confirmed live to keep its
    # ".club" TLD verbatim in the derived keyword (turning into "investors
    # club" only via the "." -> " " fallback below, not a clean brand-only
    # strip) because "club" wasn't in this list yet. Rounding out with a
    # few other common gTLDs seen among crawled domains for the same reason.
    r"club|life|world|company|agency|services|solutions|tools|tech|digital|"
    r"media|network|group|team|works)$",
    re.IGNORECASE,
)


# Confirmed live (2026-08-17): domains like "affiliates.eldorado.com" or
# "affiliates.helpdesk247.com" derived a keyword of "affiliates eldorado" /
# "affiliates helpdesk247" -- the "affiliates"/"affiliate"/"partner(s)"/etc
# label is this crawler's own generic affiliate-program subdomain prefix
# (see AFFILIATE_SUBDOMAINS in src/crawler/constants.py, the list this
# module's own crawl already uses to probe such subdomains), not part of
# the brand name, and searching it as a keyword skews volume toward
# "affiliate program" boilerplate rather than the actual brand. Reusing
# that same list here (rather than a new one) keeps "what counts as a
# generic affiliate-subdomain label" defined in exactly one place.
_LEADING_SUBDOMAIN_RE = re.compile(
    r"^(" + "|".join(re.escape(s) for s in AFFILIATE_SUBDOMAINS) + r")\.",
    re.IGNORECASE,
)


def domain_to_keyword(domain: str) -> str:
    bare = domain.strip().lower()
    bare = re.sub(r"^www\.", "", bare)
    bare = _LEADING_SUBDOMAIN_RE.sub("", bare)
    # Confirmed live (2026-08-17): a SINGLE .sub() pass only strips one TLD
    # label, leaving multi-part TLDs (e.g. "dollarbreak.co.ke") untouched
    # after only ".ke" would need to strip too -- "co" isn't in the list on
    # its own right-anchored pass since ".ke" still follows it at that
    # point. Looping until no further TLD-looking suffix matches handles
    # both multi-part ccTLDs (.co.ke, .co.uk) and, incidentally, any
    # (currently impossible) domain with more than one matching suffix.
    while True:
        new_bare = _TLD_RE.sub("", bare)
        if new_bare == bare:
            break
        bare = new_bare
    # Any remaining TLD-looking suffix not in the list above is left as-is
    # rather than guessed at further -- Keyword Planner tolerates a
    # slightly-off seed keyword fine.
    #
    # Confirmed live (2026-08-17): domains with a subdomain (e.g.
    # "chat.socialintents.com", "affiliates.phantombuster.com") were left
    # with a literal "." between the subdomain and brand after TLD
    # stripping (e.g. "chat.socialintents") -- that doesn't match how
    # Keyword Planner echoes the seed keyword back in its CSV export
    # (csv_parser.parse_csv matches by exact string), so every such
    # domain's row silently failed to match and got dropped. Treating any
    # remaining "." the same as "-"/"_" (replace with a space) turns it
    # into "chat socialintents", which matches.
    return bare.replace("-", " ").replace("_", " ").replace(".", " ").strip()
