"""Official House Clerk periodic transaction report download and parser.

The yearly index is the public ZIP at disclosures-clerk.house.gov. Each PTR
PDF is parsed from that filing. ReportDate is the digital signature date.
TransactionDate is never treated as the moment the row became public.
"""

from __future__ import annotations

import io
import re
import zipfile
from datetime import date, datetime, time, timezone
from typing import Any

import httpx

_USER_AGENT = "Lumiwealth research botspot.trade"
_INDEX_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.ZIP"
_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"
_TICKER = re.compile(r"^[A-Z][A-Z0-9]{0,9}(?:[.-][A-Z0-9]{1,4})?$")
_ASSET_CODE = re.compile(r"\((?P<ticker>[A-Z][A-Z0-9.]{0,9})\)\s*\[(?P<code>ST|OP|AB|PS|OT|GS|OL|HN|CT|DC|FN|BA|RP|CS|MF|ET|PS)\]")
_ANCHOR = re.compile(
    r"(?P<tx>P|S(?:\s*\(partial\))?|E)\s+(?P<tx_date>\d{2}/\d{2}/\d{4})\s+(?P<note_date>\d{2}/\d{2}/\d{4})"
)
_AMOUNT = re.compile(r"\$[0-9][0-9,]*(?:\s*-\s*\$[0-9][0-9,]*)?")
_SIGNED = re.compile(r"Digitally Signed:\s*(?P<name>.*?)\s*,\s*(?P<signed>\d{2}/\d{2}/\d{4})")
_FILING_ID = re.compile(r"Filing ID #(?P<doc_id>\d+)")
_MEMBER = re.compile(r"Name:\s*(?P<name>Hon\.[^\n]+)")
_STRIKE = re.compile(r"strike price of \$([0-9][0-9,]*(?:\.[0-9]+)?)")
_EXPIRATION = re.compile(r"expiration date of\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})")
_CALL_PUT = re.compile(r"\b(call|put)\b", re.IGNORECASE)
_FURNITURE = (
    "P T R",
    "Clerk of the House",
    "Legislative Resource Center",
    "ID Owner Asset",
    "Date Notification",
    "Amount Cap.",
    "Gains >",
    "Filing ID #",
    "For the complete list of asset type",
    "I CERTIFY",
    "my knowledge and belief",
    "STOCK Act",
    "fd.house.gov",
)


def house_index_url(year: int) -> str:
    return _INDEX_URL.format(year=int(year))


def house_pdf_url(year: int, doc_id: str) -> str:
    return _PDF_URL.format(year=int(year), doc_id=str(doc_id))


def _client() -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": _USER_AGENT},
        follow_redirects=True,
        timeout=60.0,
    )


def download_house_index(year: int) -> str:
    """Download the yearly House financial-disclosure index and return its text."""
    with _client() as client:
        response = client.get(house_index_url(year))
        response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".txt")]
        if not names:
            raise ValueError(f"House index ZIP for {year} did not contain a text file.")
        return archive.read(names[0]).decode("utf-8", errors="replace")


def parse_house_index(index_text: str) -> list[dict[str, str]]:
    """Parse the Clerk index. FilingType P is a periodic transaction report."""
    rows = []
    for line in index_text.splitlines():
        parts = [part.strip() for part in line.split("\t")]
        if len(parts) < 9 or parts[0] in {"Prefix", ""}:
            continue
        prefix, last, first, suffix, filing_type, state_district, year, filing_date, doc_id = parts[:9]
        if filing_type != "P" or not doc_id.isdigit():
            continue
        rows.append(
            {
                "prefix": prefix,
                "last": last,
                "first": first,
                "suffix": suffix,
                "filing_type": filing_type,
                "state_district": state_district,
                "year": year,
                "filing_date": filing_date,
                "doc_id": doc_id,
            }
        )
    return rows


def download_house_pdf(year: int, doc_id: str) -> bytes:
    with _client() as client:
        response = client.get(house_pdf_url(year, doc_id))
        response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise ValueError(f"House document {doc_id} was not a PDF.")
    return response.content


def pdf_bytes_to_text(pdf_bytes: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(pdf_bytes))
    return "\n".join((page.extract_text() or "") for page in reader.pages).replace("\x00", "")


def reflow_ptr_text(text: str) -> str:
    """Join a wrapped House PTR extract into one line per transaction.

    This only removes page furniture and line wraps. It does not decide
    buy, sell, size, or whether a row is public.
    """
    cleaned = text.replace("\x00", "")
    if "Filing ID #" not in cleaned and "Digitally Signed:" not in cleaned:
        return cleaned

    records: list[str] = []
    current: list[str] = []

    def flush() -> None:
        if current:
            records.append(" ".join(current))
            current.clear()

    for raw_line in text.replace("\x00", "").splitlines():
        line = " ".join(raw_line.split())
        if not line:
            continue
        if line in {"F I", "T", "Type", "Date", "$200?", "I P O", "C S", "Yes No"}:
            continue
        if line.startswith(
            (
                "Name:",
                "Status:",
                "State/District:",
                "Digitally Signed:",
                "P T R",
                "Clerk of the House",
                "Filing ID",
                "ID Owner",
                "* For the",
            )
        ):
            continue
        if line.startswith("SP ") or line.startswith("JT "):
            flush()
            current.append(line)
            continue
        if current:
            current.append(line)
    flush()
    return "\n".join(records)


def _clean_text(text: str) -> str:
    kept = []
    for raw_line in text.replace("\x00", "").splitlines():
        line = " ".join(raw_line.split())
        if not line:
            continue
        if line in {"F I", "T", "Type", "Date", "$200?", "I P O", "C S", "Yes No"}:
            continue
        if line.startswith(("Name:", "Status:", "State/District:", "Digitally Signed:")):
            continue
        if any(marker in line for marker in _FURNITURE):
            continue
        if line.startswith("F S:") or line.startswith("S O:") or line.startswith("L:"):
            continue
        if line.startswith("I V D"):
            continue
        kept.append(line)
    return " ".join(kept)


def _iso_date(value: str) -> str:
    parsed = datetime.strptime(value, "%m/%d/%Y").date()
    return parsed.isoformat()


def _expiration(value: str) -> str | None:
    parts = value.split("/")
    if len(parts) != 3:
        return None
    month, day, year = (int(part) for part in parts)
    if year < 100:
        year += 2000
    return date(year, month, day).isoformat()


def _side(transaction: str, description: str) -> str | None:
    lowered = description.lower()
    if transaction == "E" or "spinoff" in lowered or "gift" in lowered or "donor-advised" in lowered or "contribution" in lowered:
        return None
    if transaction.startswith("S"):
        return "sell"
    if transaction == "P":
        return "buy"
    return None


def _skip_reason(asset_name: str, asset_code: str | None, description: str, transaction: str) -> str | None:
    lowered_name = asset_name.lower()
    lowered = description.lower()
    if "llc" in lowered_name or "llc" in lowered:
        return "private_llc"
    if asset_code == "PS" or "private stock" in lowered or "private llc" in lowered:
        return "private_security"
    if asset_code == "OT" or "money market" in lowered_name or "treasury fund" in lowered or "mutual fund" in lowered:
        return "fund"
    if transaction == "E" or "spinoff" in lowered:
        return "spinoff"
    if "gift" in lowered or "donor-advised" in lowered or "contribution" in lowered:
        return "gift"
    return None


def _option_fields(description: str) -> dict[str, Any]:
    kind = _CALL_PUT.search(description)
    strike = _STRIKE.search(description)
    expiration = _EXPIRATION.search(description)
    return {
        "option_type": kind.group(1).lower() if kind else None,
        "strike": float(strike.group(1).replace(",", "")) if strike else None,
        "expiration": _expiration(expiration.group(1)) if expiration else None,
    }


def parse_house_ptr_text(text: str, *, source_url: str | None = None) -> list[dict[str, Any]]:
    """Parse one House PTR into trade rows and explicitly skipped rows.

    Skipped rows stay in the result with ``skipped_reason`` so a dry run can
    prove a private LLC, gift, spinoff, or money-market fund was seen and
    dropped. Trade rows have ``skipped_reason`` None.
    """
    signed = _SIGNED.search(text.replace("\x00", ""))
    filing_id = _FILING_ID.search(text)
    member = _MEMBER.search(text.replace("\x00", ""))
    if signed is None:
        raise ValueError("House PTR is missing the digital signature date.")
    report_date = _iso_date(signed.group("signed"))
    politician = " ".join((member.group("name") if member else signed.group("name")).split())
    doc_id = filing_id.group("doc_id") if filing_id else None
    cleaned = _clean_text(text)
    matches = list(_ANCHOR.finditer(cleaned))
    rows = []
    for index, match in enumerate(matches):
        previous = matches[index - 1].end() if index else 0
        nxt = matches[index + 1].start() if index + 1 < len(matches) else len(cleaned)
        header = cleaned[previous:match.start()]
        body = cleaned[match.end():nxt]
        description = ""
        amount = ""
        if "D:" in body:
            before_description, after_description = body.split("D:", 1)
            amount_match = _AMOUNT.search(before_description)
            amount = amount_match.group(0) if amount_match else ""
            description = after_description.strip()
            # The next asset name sits in this body after the description when
            # the following anchor has not started yet. Cut it off at the next
            # owner marker or ticker block that belongs to the following row.
            description = re.split(r"\s+(?:SP|JT|DC|JT)\s+[A-Z]", description, maxsplit=1)[0].strip()
        else:
            amount_match = _AMOUNT.search(header + " " + body)
            amount = amount_match.group(0) if amount_match else ""
        asset_blob = f"{header} {body.split('D:', 1)[0]}"
        asset_match = None
        for candidate in _ASSET_CODE.finditer(asset_blob):
            asset_match = candidate
        ticker = asset_match.group("ticker") if asset_match else None
        asset_code = asset_match.group("code") if asset_match else None
        asset_name = asset_blob[: asset_match.start()] if asset_match else asset_blob
        asset_name = re.sub(r"^(?:SP|JT|DC)\s+", "", " ".join(asset_name.split())).strip(" -")
        # Wrapped rows put the ticker after the amount. Search the description
        # side of the header too.
        if asset_match is None:
            fallback = _ASSET_CODE.search(header + body)
            if fallback:
                ticker = fallback.group("ticker")
                asset_code = fallback.group("code")
                asset_name = re.sub(r"^(?:SP|JT|DC)\s+", "", header[: fallback.start()] if fallback.start() else header)
                asset_name = " ".join(asset_name.split()).strip(" -")
        transaction = match.group("tx")
        description = " ".join(description.split())
        description = re.split(r"\s+Digitally Signed:", description, maxsplit=1)[0].strip()
        option = _option_fields(description)
        header_asset = None
        for candidate in _ASSET_CODE.finditer(header):
            header_asset = candidate
        if header_asset is not None:
            asset_match = header_asset
            ticker = asset_match.group("ticker")
            asset_code = asset_match.group("code")
            prefix = header[: asset_match.start()]
            owners = list(re.finditer(r"\b(?:SP|JT|DC)\s+", prefix))
            asset_name = prefix[owners[-1].end():] if owners else prefix
        elif asset_match is not None:
            pre_description = body.split("D:", 1)[0]
            owner_cut = re.split(r"\s+(?:SP|JT|DC)\s+", pre_description, maxsplit=1)[0]
            wrapped = None
            for candidate in _ASSET_CODE.finditer(owner_cut):
                wrapped = candidate
            if wrapped is not None:
                asset_match = wrapped
                ticker = asset_match.group("ticker")
                asset_code = asset_match.group("code")
                asset_name = f"{header} {owner_cut[: wrapped.start()]}"
        asset_name = re.sub(r"^(?:SP|JT|DC)\s+", "", " ".join(asset_name.split())).strip(" -")
        reason = _skip_reason(asset_name, asset_code, description, transaction.split()[0])
        side = None if reason else _side(transaction, description)
        if ticker and not _TICKER.fullmatch(ticker):
            reason = reason or "invalid_ticker"
        rows.append(
            {
                "Politician": politician,
                "Ticker": ticker,
                "Transaction": "Sale" if transaction.startswith("S") else "Purchase" if transaction == "P" else transaction,
                "TransactionDate": _iso_date(match.group("tx_date")),
                "ReportDate": report_date,
                "Amount": " ".join(amount.split()),
                "description": description,
                "asset_name": " ".join(asset_name.split()),
                "asset_code": asset_code,
                "owner": "SP" if header.strip().startswith("SP") else "JT" if "JT" in header[:8] else None,
                "doc_id": doc_id,
                "side": side,
                "option_type": option["option_type"],
                "strike": option["strike"],
                "expiration": option["expiration"],
                "skipped_reason": reason,
                "source": "house_clerk_ptr",
                "source_url": source_url or (house_pdf_url(int(report_date[:4]), doc_id) if doc_id else None),
                "data_rights": "official_public_filing",
            }
        )
    return rows


def tradeable_rows(rows: list[dict[str, Any]], *, asset_mode: str) -> list[dict[str, Any]]:
    """Stock mode needs a ticker and a buy or sell. Option mode also needs call
    or put, strike, and expiration. Rows missing any of those are omitted.
    """
    mode = asset_mode.lower().strip()
    if mode not in {"stock", "option"}:
        raise ValueError("asset_mode must be 'stock' or 'option'.")
    chosen = []
    for row in rows:
        if row.get("skipped_reason"):
            continue
        ticker = row.get("Ticker")
        side = row.get("side")
        if not ticker or not side or not _TICKER.fullmatch(str(ticker)):
            continue
        code = row.get("asset_code")
        if mode == "stock":
            if code == "OP":
                continue
            if code not in {"ST", "AB"}:
                continue
            chosen.append(row)
            continue
        if code != "OP":
            continue
        if row.get("option_type") not in {"call", "put"} or row.get("strike") is None or not row.get("expiration"):
            continue
        chosen.append(row)
    return chosen


def load_house_filings(
    year: int,
    *,
    last_names: list[str] | None = None,
    doc_ids: list[str] | None = None,
    asset_mode: str = "stock",
) -> list[dict[str, Any]]:
    """Download the yearly index and the matching PTR PDFs, then return trade rows."""
    index_rows = parse_house_index(download_house_index(year))
    wanted_names = {name.lower() for name in last_names} if last_names else None
    wanted_ids = {str(doc_id) for doc_id in doc_ids} if doc_ids else None
    selected = []
    for row in index_rows:
        if wanted_ids is not None and row["doc_id"] not in wanted_ids:
            continue
        if wanted_names is not None and row["last"].lower() not in wanted_names:
            continue
        if wanted_ids is None and wanted_names is None:
            continue
        selected.append(row)
    parsed: list[dict[str, Any]] = []
    for row in selected:
        pdf = download_house_pdf(int(row["year"] or year), row["doc_id"])
        text = pdf_bytes_to_text(pdf)
        parsed.extend(tradeable_rows(parse_house_ptr_text(text, source_url=house_pdf_url(int(row["year"] or year), row["doc_id"])), asset_mode=asset_mode))
    return parsed


def _public_datetime(value: Any) -> datetime | None:
    """Parse a House index date. MM/DD/YYYY and ISO are both public dates."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def public_house_filings(
    year: int,
    *,
    last_names: list[str],
    as_of: Any,
    asset_mode: str = "stock",
) -> dict[str, Any]:
    """Download only House PTR filings already public at as_of.

    The yearly index date is the gate. A later filing is counted and skipped
    before any PDF download, so its document id and trades never enter the result.
    """
    from lumibot.components.disclosure_signals import visible_congress_disclosures

    ceiling = _public_datetime(as_of)
    if ceiling is None:
        raise ValueError("as_of must be a valid datetime.")
    wanted = {str(name).strip().lower() for name in last_names if str(name).strip()}
    index_rows = parse_house_index(download_house_index(year))
    kept = []
    omitted_future_count = 0
    for row in index_rows:
        if row["last"].strip().lower() not in wanted:
            continue
        published = _public_datetime(row["filing_date"])
        if published is None or published > ceiling:
            omitted_future_count += 1
            continue
        kept.append(row)
    parsed: list[dict[str, Any]] = []
    for row in kept:
        filing_year = int(row["year"] or year)
        pdf = download_house_pdf(filing_year, row["doc_id"])
        text = pdf_bytes_to_text(pdf)
        source_url = house_pdf_url(filing_year, row["doc_id"])
        parsed.extend(tradeable_rows(parse_house_ptr_text(text, source_url=source_url), asset_mode=asset_mode))
    filings = visible_congress_disclosures(parsed, as_of=ceiling)
    return {
        "ok": True,
        "as_of": ceiling.isoformat(),
        "filings": filings,
        "count": len(filings),
        "omitted_future_count": omitted_future_count,
    }


def format_dry_run(rows: list[dict[str, Any]]) -> str:
    lines = []
    for row in rows:
        if row.get("skipped_reason"):
            continue
        option = ""
        if row.get("option_type"):
            option = f" {row['option_type']} strike {row['strike']} exp {row['expiration']}"
        lines.append(
            f"{row.get('doc_id')} {row.get('ReportDate')} {row.get('Politician')} "
            f"{row.get('Ticker')} {row.get('asset_code')} {row.get('side')} "
            f"tx {row.get('TransactionDate')} {row.get('Amount')}{option} | {row.get('description')}"
        )
    return "\n".join(lines) + ("\n" if lines else "")
