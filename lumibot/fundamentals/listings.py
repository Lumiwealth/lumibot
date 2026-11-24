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
from typing import Any, Callable, Iterable, NamedTuple
from zoneinfo import ZoneInfo

from .sec import _NEW_YORK, _parse_dt, _same_tz, _strip_html

LISTING_FORMS = ("424B4", "424B1")
DELISTING_FORMS = ("25", "25-NSE", "15-12B", "15-12G", "15-15D")
PERIODIC_FORMS = ("10-K", "10-Q", "20-F", "40-F", "10-K405", "10-KSB")
BLANK_CHECK_SIC = "6770"
# Blank-check companies sometimes file under another SIC code; their names give them away.
_BLANK_CHECK_NAME = re.compile(r"\b(acquisition|merger)\s+(corp|corporation|co|company|inc|ltd|limited|holdings)\b", re.I)
DEFAULT_KINDS = ("operating_company",)
_ALL_KINDS = ("operating_company", "blank_check", "other")
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


_BLANK_CHECK_COVER = re.compile(r"\bblank[\s-]+check\s+compan(?:y|ies)\b", re.I)


def prospectus_is_blank_check(text: str) -> bool:
    """True when a prospectus describes the issuer as a blank check company (a SPAC)."""
    plain = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", text or "")))
    return bool(_BLANK_CHECK_COVER.search(plain))


class _Filing(NamedTuple):
    form: str
    day: str
    accepted: str | None
    accession: str | None
    document: str | None


def _filings(submissions: dict[str, Any]) -> list[_Filing]:
    recent = (submissions.get("filings") or {}).get("recent") or {}
    rows = []
    for file in (recent, *submissions.get("historical_filings", ())):
        columns = [file.get(name) or [] for name in ("acceptanceDateTime", "accessionNumber", "primaryDocument")]
        for index, (form, day) in enumerate(zip(file.get("form") or [], file.get("filingDate") or [])):
            values = [column[index] if index < len(column) else None for column in columns]
            rows.append(_Filing(form, day, *values))
    return rows


_EXCHANGE = re.compile(r"\b(?:new york stock exchange|nyse(?: american| arca)?|nasdaq|cboe bzx|iex|ltse)\b")


def _exchange(text: str) -> str | None:
    match = _EXCHANGE.search(text)
    if not match:
        return None
    return "nyse" if match[0] == "new york stock exchange" else match[0]


def _document_url(cik: int, filing: _Filing) -> str | None:
    accession = (filing.accession or "").replace("-", "")
    document = (filing.document or "").rsplit("/", 1)[-1]
    if re.fullmatch(r"[0-9]{18}", accession) and re.fullmatch(r"[a-zA-Z0-9_.-]+\.(?:htm[l]?|xml|txt)", document):
        return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{document}"
    return None


def _confirmed_transfer(cik, removal, registrations, fetch_head):
    # Form 8-A can register a different class or record a different event. Read
    # the actual filing and require an explicit move for the same class and exchange.
    if not fetch_head or not registrations or removal.form not in ("25", "25-NSE"):
        return False
    entered = date.fromisoformat(removal.day)
    witnesses = []
    for registration in registrations:
        days = (entered - date.fromisoformat(registration.day)).days
        if not 0 <= days <= 30:
            continue
        if days == 0:
            left, right = _parse_dt(registration.accepted), _parse_dt(removal.accepted)
            if left is None or right is None or _same_tz(left, right) > right:
                continue
        url = _document_url(cik, registration)
        if url:
            witnesses.append(url)
    notice = _document_url(cik, removal)
    if not notice or not witnesses:
        return False

    raw = fetch_head(notice)
    xml_class = re.search(r"<descriptionClassSecurity>([^<]+)</descriptionClassSecurity>", raw, re.I)
    if xml_class:
        xml_exchange = re.search(r"<exchange>.*?<entityName>([^<]+)</entityName>", raw, re.I | re.S)
        title = _strip_html(xml_class[1])
        origin = _exchange(xml_exchange[1].lower()) if xml_exchange else None
    else:
        plain = _strip_html(raw)
        description = re.search(r"([^\n]+)\s*\(description of class of securities\)", plain, re.I)
        issuer = re.search(r"([^\n]+)\s*\(exact name of issuer\b", plain, re.I)
        title = description[1] if description else ""
        origin = _exchange(issuer[1].lower()) if issuer else None
    security = re.split(r",?\s+par value\b", " ".join(title.lower().split()), maxsplit=1)[0]
    if not security or not origin:
        return False

    for url in witnesses:
        text = " ".join(_strip_html(fetch_head(url)).lower().split())
        pattern = (
            rf"\btransfer (?:of )?(?:the |its )?listing\b.{{0,180}}?\b{re.escape(security)}\b"
            r".{0,120}?\bfrom ([^.]{1,100}?) to ([^.]{1,100})"
        )
        for match in re.finditer(pattern, text):
            old, new = _exchange(match[1]), _exchange(match[2])
            if old == origin and new and new != origin:
                return True
    return False


def _public_by(filing_date: date, acceptance: str | None, date_cutoff: date, instant_cutoff: datetime | None):
    if instant_cutoff is not None and acceptance:
        published = _parse_dt(acceptance)
        if published is not None:
            return _same_tz(published, instant_cutoff) <= instant_cutoff
    return filing_date <= date_cutoff


def classify_listing(
    offering: dict[str, Any], submissions: dict[str, Any], *, fetch_head: Callable[[str], str] | None = None,
) -> dict[str, Any] | None:
    """A listing row for a company's first final prospectus, or None for a follow-on offering.

    A company that filed periodic reports (10-K, 10-Q, 20-F) before this prospectus was already
    public. Blank-check companies (SIC 6770) are kept and labeled, never silently mixed in.
    """
    filed = offering["filed"]
    filings = _filings(submissions)
    if any(filing.form in PERIODIC_FORMS and filing.day < filed for filing in filings):
        return None
    sic = str(submissions.get("sic") or "")
    name = submissions.get("name") or offering.get("company") or ""
    # A blank check renames itself after its merger, so check the name it filed the prospectus
    # under and its former names too, not only today's name.
    names = [name, offering.get("company") or ""] + [
        str(former.get("name") or "") for former in submissions.get("formerNames") or [] if isinstance(former, dict)
    ]
    if sic == BLANK_CHECK_SIC or any(_BLANK_CHECK_NAME.search(n) for n in names):
        kind = "blank_check"
    elif not sic:
        kind = "other"  # funds, trusts and finance subsidiaries file without an industry code
    else:
        kind = "operating_company"
    registrations = [filing for filing in filings if filing.form == "8-A12B"]
    removals = sorted(
        (filing for filing in filings if filing.form in DELISTING_FORMS and filing.day >= filed),
        key=lambda notice: (notice.day, notice.accepted or ""),
    )
    delisting = next(
        (notice for notice in removals if not _confirmed_transfer(offering["cik"], notice, registrations, fetch_head)),
        None,
    )
    prospectuses = [filing for filing in filings if (filing.form, filing.day) == (offering["form"], filed)]
    accession = Path(offering["path"]).stem.replace("-", "")
    exact = [filing for filing in prospectuses if (filing.accession or "").replace("-", "") == accession]
    acceptances = [filing.accepted for filing in (exact or prospectuses) if filing.accepted]

    return {
        "cik": int(offering["cik"]),
        "name": name,
        "ticker": None,
        "prospectus_date": filed,
        "prospectus_accepted_at": max(acceptances, default=None),
        "form": offering["form"],
        "kind": kind,
        "sic": sic or None,
        "delisting_filed": delisting.day if delisting else None,
        "delisting_accepted_at": delisting.accepted if delisting else None,
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
        kinds: Iterable[str] = DEFAULT_KINDS,
    ) -> list[dict[str, Any]]:
        """Listings with a prospectus date in [start, end] and on or before ``as_of``.

        A delisting filed after ``as_of`` is not known yet and reads as None.
        """
        lo, hi, cut = _as_date(start), _as_date(end), _as_date(as_of)
        instant = _parse_dt(as_of) if isinstance(as_of, datetime) or len(str(as_of)) > 10 else None
        if instant is not None and instant.tzinfo is None:
            instant = instant.replace(tzinfo=_NEW_YORK)
        wanted = {kinds} if isinstance(kinds, str) else set(kinds)
        out = []
        for row in self.rows:
            when = _as_date(row["prospectus_date"])
            if when < lo or when > hi or not _public_by(when, row.get("prospectus_accepted_at"), cut, instant):
                continue
            if row.get("kind") not in wanted:
                continue
            visible = dict(row)
            visible.pop("prospectus_accepted_at", None)
            delisting_accepted = visible.pop("delisting_accepted_at", None)
            if visible.get("delisting_filed") and not _public_by(
                _as_date(visible["delisting_filed"]), delisting_accepted, cut, instant,
            ):
                visible["delisting_filed"] = None
            out.append(visible)
        return out

    def for_symbol(self, symbol: str, *, as_of: Any) -> dict[str, Any] | None:
        """The latest listing printed under ``symbol`` that is public at ``as_of``."""
        wanted = str(symbol).strip().upper()
        matches = [r for r in self.between(date(1900, 1, 1), as_of, as_of=as_of, kinds=_ALL_KINDS)
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
    today = datetime.now(ZoneInfo("America/New_York")).date()
    for year in years:
        for quarter in (1, 2, 3, 4):
            # The running index contains filings through the previous business day.
            if date(year, 3 * quarter - 2, 1) >= today:
                continue
            try:
                text = fetch_text(FORM_INDEX_URL.format(year=year, quarter=quarter))
            except Exception as exc:
                raise RuntimeError(f"Cannot rebuild SEC listings: form index {year} Q{quarter} unavailable") from exc
            for row in parse_form_index(text):
                known = first.get(row["cik"])
                if known is None or row["filed"] < known["filed"]:
                    first[row["cik"]] = row
    def one(offering: dict[str, Any]) -> dict[str, Any] | None:
        try:
            submissions = fetch_submissions(offering["cik"])
        except Exception as exc:
            raise RuntimeError(f"Cannot rebuild SEC listings: submissions for CIK {offering['cik']} unavailable") from exc
        row = classify_listing(offering, submissions, fetch_head=fetch_head)
        if row is None:
            return None
        try:
            head = fetch_head(row["prospectus_url"])
        except Exception:
            return row
        row["ticker"] = ticker_from_prospectus(head)
        if row["kind"] == "operating_company" and prospectus_is_blank_check(head):
            row["kind"] = "blank_check"
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
