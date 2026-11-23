"""Stock listings from public SEC EDGAR files.

Which companies sold shares to the public for the first time, on what date, under which ticker,
and, once known, when they filed to leave their exchange. Built only from public US government
files (EDGAR quarterly form indexes, company submissions, prospectus covers), so it can be cached
and shared freely.

Why it exists: today's ticker list holds only the companies that are still listed. A study of new
listings built on it silently drops every company that was later delisted, acquired or went
bankrupt. This table keeps them, with the ticker printed on their own prospectus.

Point in time: a row becomes visible on its final prospectus date (form 424B4 or 424B1, filed 0 to
2 business days after the first trade), and a delisting date only once that filing is public.
The first trade date itself is the first real daily bar from the strategy's price source.

Docs: ``docs/SEC_FUNDAMENTALS.md`` (Listings) and ``docsrc/fundamentals.rst``.
"""

from __future__ import annotations

import gzip
import html
import json
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable

LISTING_FORMS = ("424B4", "424B1")
DELISTING_FORMS = ("25", "25-NSE", "15-12B", "15-12G", "15-15D")
PERIODIC_FORMS = ("10-K", "10-Q", "20-F", "40-F", "10-K405", "10-KSB")
BLANK_CHECK_SIC = "6770"
TABLE_VERSION = 1
TABLE_RELATIVE_PATH = ("listings", "v1", "listings.json.gz")
TABLE_REMOTE_RELATIVE = "sec/listings/v1/listings.json.gz"
FORM_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/form.gz"
PROSPECTUS_HEAD_BYTES = 400_000

_INDEX_LINE = re.compile(r"^(\S+)\s+(.+?)\s+(\d{3,10})\s+(\d{4}-\d{2}-\d{2})\s+(edgar/\S+)\s*$")
_TICKER_PATTERN = re.compile(
    r"(?:under\s+the\s+(?:trading\s+|ticker\s+)?symbols?|ticker\s+symbol)\s*[\"'“‘]\s*"
    r"([A-Z][A-Z0-9]{0,5}(?:[.\-][A-Z]{1,2})?)\s*\.?\s*[\"'”’]",
)


def parse_form_index(text: str) -> list[dict[str, Any]]:
    """Final-prospectus rows of an EDGAR ``form.idx``: form, company, cik, filed, path."""
    rows = []
    for line in text.splitlines():
        if not line.startswith(LISTING_FORMS):
            continue
        match = _INDEX_LINE.match(line.rstrip())
        if not match or match.group(1) not in LISTING_FORMS:
            continue
        rows.append(
            {
                "form": match.group(1),
                "company": match.group(2).strip(),
                "cik": int(match.group(3)),
                "filed": match.group(4),
                "path": match.group(5),
            }
        )
    return rows


def ticker_from_prospectus(text: str) -> str | None:
    """Ticker printed on a prospectus cover ("under the symbol ..."), or None.

    Units and warrants are often listed first ("ABCDU"); the shortest symbol named is the shares.
    """
    plain = html.unescape(re.sub(r"<[^>]+>", " ", text or ""))
    plain = re.sub(r"\s+", " ", plain)
    found = [m.group(1) for m in _TICKER_PATTERN.finditer(plain)]
    if not found:
        return None
    return min(found[:6], key=lambda symbol: (len(symbol), found.index(symbol)))


def _filings(submissions: dict[str, Any]) -> list[tuple[str, str]]:
    recent = (submissions.get("filings") or {}).get("recent") or {}
    return list(zip(recent.get("form") or [], recent.get("filingDate") or []))


def classify_listing(offering: dict[str, Any], submissions: dict[str, Any]) -> dict[str, Any] | None:
    """A listing row for a company's first final prospectus, or None for a follow-on offering.

    A company that filed periodic reports (10-K, 10-Q, 20-F) before this prospectus was already
    public. Blank-check companies (SIC 6770) are kept and labeled, never silently mixed in.
    """
    filed = offering["filed"]
    filings = _filings(submissions)
    if any(form in PERIODIC_FORMS and when < filed for form, when in filings):
        return None
    sic = str(submissions.get("sic") or "")
    name = submissions.get("name") or offering.get("company") or ""
    kind = "blank_check" if sic == BLANK_CHECK_SIC else "operating_company"
    delisting = sorted(when for form, when in filings if form in DELISTING_FORMS and when >= filed)
    return {
        "cik": int(offering["cik"]),
        "name": name,
        "ticker": None,
        "prospectus_date": filed,
        "form": offering["form"],
        "kind": kind,
        "sic": sic or None,
        "delisting_filed": delisting[0] if delisting else None,
        "prospectus_url": f"https://www.sec.gov/Archives/{offering['path']}",
    }


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


class ListingsTable:
    """The listings rows plus point-in-time reads."""

    def __init__(self, rows: Iterable[dict[str, Any]], *, built_at: str | None = None):
        self.rows = sorted(rows, key=lambda r: (r["prospectus_date"], r["cik"]))
        self.built_at = built_at

    def between(
        self,
        start: Any,
        end: Any,
        *,
        as_of: Any,
        include_blank_checks: bool = False,
    ) -> list[dict[str, Any]]:
        """Listings with a prospectus date in [start, end] and on or before ``as_of``.

        A delisting filed after ``as_of`` is not known yet and reads as None.
        """
        lo, hi, cut = _as_date(start), _as_date(end), _as_date(as_of)
        out = []
        for row in self.rows:
            when = _as_date(row["prospectus_date"])
            if when < lo or when > hi or when > cut:
                continue
            if row.get("kind") == "blank_check" and not include_blank_checks:
                continue
            visible = dict(row)
            if visible.get("delisting_filed") and _as_date(visible["delisting_filed"]) > cut:
                visible["delisting_filed"] = None
            out.append(visible)
        return out

    def for_symbol(self, symbol: str, *, as_of: Any) -> dict[str, Any] | None:
        """The latest listing printed under ``symbol`` that is public at ``as_of``."""
        wanted = str(symbol).strip().upper()
        matches = [r for r in self.between(date(1900, 1, 1), as_of, as_of=as_of, include_blank_checks=True)
                   if (r.get("ticker") or "").upper() == wanted]
        return matches[-1] if matches else None

    def save(self, path: str | os.PathLike[str]) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}")
        with gzip.open(tmp, "wt", encoding="utf-8") as handle:
            json.dump({"version": TABLE_VERSION, "built_at": self.built_at, "rows": self.rows}, handle)
        os.replace(tmp, path)

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "ListingsTable":
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("version") != TABLE_VERSION:
            raise ValueError(f"listings table version {payload.get('version')!r}, expected {TABLE_VERSION}")
        return cls(payload.get("rows") or [], built_at=payload.get("built_at"))


def build_listings(
    sec: Any,
    years: Iterable[int],
    *,
    fetch_text: Callable[[str], str],
    fetch_head: Callable[[str], str],
    fetch_submissions: Callable[[int], dict[str, Any]],
    progress: Callable[[str], None] | None = None,
    max_workers: int = 8,
) -> ListingsTable:
    """Build the table from SEC files. The fetchers are injected so a warmer can rate-limit,
    cache and resume them; ``sec`` is only used for its clock in the ``built_at`` stamp."""
    first: dict[int, dict[str, Any]] = {}
    for year in years:
        for quarter in (1, 2, 3, 4):
            try:
                text = fetch_text(FORM_INDEX_URL.format(year=year, quarter=quarter))
            except Exception as exc:  # a future quarter has no index yet
                if progress:
                    progress(f"form index {year} Q{quarter} unavailable: {exc}")
                continue
            for row in parse_form_index(text):
                known = first.get(row["cik"])
                if known is None or row["filed"] < known["filed"]:
                    first[row["cik"]] = row
    def one(offering: dict[str, Any]) -> dict[str, Any] | None:
        try:
            row = classify_listing(offering, fetch_submissions(offering["cik"]))
        except Exception as exc:
            if progress:
                progress(f"submissions for CIK {offering['cik']} unavailable: {exc}")
            return None
        if row is None:
            return None
        try:
            row["ticker"] = ticker_from_prospectus(fetch_head(row["prospectus_url"]))
        except Exception:
            row["ticker"] = None
        return row

    from concurrent.futures import ThreadPoolExecutor

    offerings = sorted(first.values(), key=lambda r: r["filed"])
    rows = []
    # The fetchers share the SEC rate limit; threads only overlap network latency.
    with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as pool:
        for index, row in enumerate(pool.map(one, offerings)):
            if row is not None:
                rows.append(row)
            if progress and index % 250 == 0:
                progress(f"{index} of {len(offerings)} offerings checked, {len(rows)} listings")
    built_at = datetime.now().astimezone().isoformat()
    return ListingsTable(rows, built_at=built_at)
