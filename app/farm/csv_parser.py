"""Parses a Keyword Planner "Download keyword ideas" CSV export -- the
format confirmed live from a real download (2026-08-15, "n8n" query):

    Keyword Stats <download timestamp>
    "<date range>"
    Keyword\tCurrency\tAvg. monthly searches\tThree month change\tYoY change\t
    Competition\tCompetition (indexed value)\tTop of page bid (low range)\t
    Top of page bid (high range)\tAd impression share\tOrganic impression share\t
    Organic average position\tIn account?\tIn plan?\t
    Searches: Aug 2025\tSearches: Sep 2025\t...\tSearches: Jul 2026
    n8n\tVND\t1830000\t-18%\t-33%\tMedium\t36\t1046\t73710\t\t\t\t\t\t2240000\t...
    n8n automation\tVND\t27100\t0%\t22%\tLow\t33\t3576\t83476\t\t\t\t\t\t22200\t...

Tab-separated despite the .csv extension (Google's own export uses TSV under
that name). The FIRST data row is always the seed keyword itself (Google
lists it under "Keywords you provided" in the UI, and it's always first in
the export too -- confirmed against the operator's own screenshot, which
showed the identical ordering); every row after that is a "Keyword ideas"
suggestion. There is no explicit section marker in the CSV distinguishing
the two (unlike the UI's own "Keywords you provided" / "Keyword ideas"
sub-headers) -- row 0 = seed is the only signal, so parse_csv() takes the
seed keyword's exact text as an extra safety check rather than trusting
position alone.
"""

import csv
import io
import json


def read_export_file(path: str) -> str:
    """Google's own "Download keyword ideas" export is UTF-16 LE with a BOM
    (confirmed live: first bytes are \\xff\\xfe) despite the .csv extension
    implying plain text -- read it with the right codec rather than letting
    a default utf-8 read blow up on the first non-ASCII byte."""
    with open(path, "rb") as f:
        raw = f.read()
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16-le")
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be")
    return raw.decode("utf-8-sig")


def parse_csv(raw_text: str, seed_keywords: list[str]) -> list[dict]:
    """Returns one dict per SEED row only -- "Keyword ideas" suggestion rows
    are dropped entirely (operator's explicit instruction: only the seed
    keywords the batch was run with matter, not Google's own related-term
    suggestions). Matches rows by exact (case-insensitive) text against
    `seed_keywords` rather than position, since a multi-keyword batch (see
    run_keyword_farm.py's own batching) puts N seed rows under "Keywords you
    provided" followed by many more idea rows for ALL of them combined --
    there's no longer a simple "row 0 is the seed" rule once more than one
    seed keyword is submitted per query.

    Each returned dict is shaped for db.save_keyword_rows():
        {"keyword": str, "is_seed": True, "avg_monthly_searches": int|None,
         "three_month_change": str|None, "yoy_change": str|None,
         "competition": str|None, "competition_index": int|None,
         "low_top_of_page_bid": int|None, "high_top_of_page_bid": int|None,
         "monthly_trend_json": str|None}
    (is_seed is always True here -- non-seed rows are never returned.)
    Skips the two header lines above the tab-separated table (title line,
    quoted date-range line) before handing off to csv.DictReader.
    """
    seed_set = {k.strip().lower() for k in seed_keywords}

    lines = raw_text.splitlines()
    # First line is "Keyword Stats <timestamp>", second is a quoted date
    # range -- neither is part of the tab-separated table. The real header
    # row is the first line containing a tab character.
    header_idx = next((i for i, line in enumerate(lines) if "\t" in line), None)
    if header_idx is None:
        return []

    reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])), delimiter="\t")
    month_columns = [f for f in (reader.fieldnames or []) if f.startswith("Searches:")]

    results = []
    for row in reader:
        keyword = (row.get("Keyword") or "").strip()
        if not keyword or keyword.lower() not in seed_set:
            continue

        monthly_trend = {}
        for col in month_columns:
            value = _parse_int(row.get(col))
            if value is not None:
                # "Searches: Aug 2025" -> "2025-08"
                month_label = col.replace("Searches:", "").strip()
                monthly_trend[_month_label_to_key(month_label)] = value

        results.append(
            {
                "keyword": keyword,
                "is_seed": True,
                "avg_monthly_searches": _parse_int(row.get("Avg. monthly searches")),
                "three_month_change": (row.get("Three month change") or "").strip() or None,
                "yoy_change": (row.get("YoY change") or "").strip() or None,
                "competition": (row.get("Competition") or "").strip() or None,
                "competition_index": _parse_int(row.get("Competition (indexed value)")),
                "low_top_of_page_bid": _parse_int(row.get("Top of page bid (low range)")),
                "high_top_of_page_bid": _parse_int(row.get("Top of page bid (high range)")),
                "monthly_trend_json": json.dumps(monthly_trend) if monthly_trend else None,
            }
        )
    return results


_MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
    "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


def _month_label_to_key(label: str) -> str:
    """"Aug 2025" -> "2025-08". Falls back to the raw label (unlikely to
    collide with a real key) if the format doesn't match what Google's own
    export has consistently used."""
    parts = label.split()
    if len(parts) == 2 and parts[0][:3] in _MONTHS:
        return f"{parts[1]}-{_MONTHS[parts[0][:3]]}"
    return label


def _parse_int(text: str | None) -> int | None:
    if not text:
        return None
    digits = text.strip().replace(",", "")
    if not digits or not digits.lstrip("-").isdigit():
        return None
    return int(digits)
