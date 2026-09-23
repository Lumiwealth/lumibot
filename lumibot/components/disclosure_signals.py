from __future__ import annotations

import hashlib
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

_OPEN_MARKET_CODES = {"P", "S"}
_TRANSACTION_KINDS = {
    "P": "open_market_purchase",
    "S": "open_market_sale",
    "A": "grant_or_award",
    "D": "disposition_to_issuer",
    "F": "tax_or_exercise_payment",
    "G": "gift",
    "M": "option_exercise",
}
_TICKER = re.compile(r"^[A-Z][A-Z0-9]{0,9}(?:[.-][A-Z0-9]{1,4})?$")


def _datetime(value: Any) -> datetime | None:
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
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _amount_range(value: Any) -> tuple[float | None, float | None]:
    numbers = re.findall(r"[0-9][0-9,]*(?:\.[0-9]+)?", str(value or ""))
    parsed = []
    for number in numbers[:2]:
        try:
            parsed.append(float(Decimal(number.replace(",", ""))))
        except InvalidOperation:
            continue
    if not parsed:
        return None, None
    if len(parsed) == 1:
        return parsed[0], parsed[0]
    return min(parsed), max(parsed)


def _stable_id(parts: Iterable[Any]) -> str:
    canonical = "|".join(str(part or "").strip() for part in parts)
    return hashlib.sha256(canonical.encode()).hexdigest()[:24]


def normalize_congress_disclosure(record: dict[str, Any]) -> dict[str, Any] | None:
    ticker = str(record.get("Ticker") or record.get("ticker") or "").strip().upper()
    politician = str(
        record.get("Politician") or record.get("Representative") or record.get("Name") or record.get("politician") or ""
    ).strip()
    transaction_date = str(record.get("TransactionDate") or record.get("transaction_date") or "").strip()
    report_date = str(
        record.get("ReportDate") or record.get("DisclosureDate") or record.get("published_at") or ""
    ).strip()
    published = _datetime(report_date)
    if not _TICKER.fullmatch(ticker) or not published:
        return None
    amount_min, amount_max = _amount_range(record.get("Amount") or record.get("amount"))
    transaction = str(record.get("Transaction") or record.get("transaction") or "").strip()
    disclosure_id = str(record.get("id") or record.get("DisclosureID") or "").strip() or _stable_id(
        (politician, ticker, transaction, transaction_date, published.isoformat(), amount_min, amount_max)
    )
    fetched = _datetime(record.get("fetched_at")) or datetime.now(timezone.utc)
    return {
        "id": disclosure_id,
        "politician": politician,
        "ticker": ticker,
        "transaction": transaction,
        "transaction_date": transaction_date or None,
        "published_at": published.isoformat(),
        "fetched_at": fetched.isoformat(),
        "amount_min": amount_min,
        "amount_max": amount_max,
        "amendment": bool(record.get("Amendment") or record.get("amendment")),
        "source": record.get("source") or "congress_disclosure",
        "source_url": record.get("SourceUrl") or record.get("source_url"),
        "data_rights": record.get("data_rights") or "official_public_filing",
        "asset_code": record.get("asset_code"),
        "description": record.get("description"),
        "doc_id": record.get("doc_id") or record.get("DocID"),
        "side": record.get("side"),
        "option_type": record.get("option_type"),
        "strike": record.get("strike"),
        "expiration": record.get("expiration"),
    }


def visible_congress_disclosures(records: Iterable[dict[str, Any]], *, as_of: Any) -> list[dict[str, Any]]:
    ceiling = _datetime(as_of)
    if ceiling is None:
        raise ValueError("as_of must be a valid datetime.")
    visible_by_id = {}
    for record in records:
        normalized = normalize_congress_disclosure(record)
        if normalized is None:
            continue
        published = _datetime(normalized["published_at"])
        if published is not None and published <= ceiling:
            existing = visible_by_id.get(normalized["id"])
            if existing is None or (normalized["published_at"], normalized["amendment"]) > (
                existing["published_at"],
                existing["amendment"],
            ):
                visible_by_id[normalized["id"]] = normalized
    return sorted(visible_by_id.values(), key=lambda row: (row["published_at"], row["id"]))


def _local_name(element: ET.Element) -> str:
    return element.tag.rsplit("}", 1)[-1]


def _descendant_text(element: ET.Element, *path: str) -> str | None:
    current = element
    for name in path:
        current = next((child for child in current if _local_name(child) == name), None)
        if current is None:
            return None
    text = "".join(current.itertext()).strip()
    return text or None


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_form4_xml(
    xml_text: str,
    *,
    accession_number: str,
    acceptance_datetime: Any,
    source_url: str | None = None,
    fetched_at: Any | None = None,
) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ValueError(f"Invalid Form 4 XML: {exc}") from exc
    published = _datetime(acceptance_datetime)
    if published is None:
        raise ValueError("acceptance_datetime must be a valid datetime.")
    fetched = _datetime(fetched_at) or datetime.now(timezone.utc)
    document_type = _descendant_text(root, "documentType") or "4"
    ticker = (_descendant_text(root, "issuer", "issuerTradingSymbol") or "").upper()
    issuer_cik = _descendant_text(root, "issuer", "issuerCik")
    owner_name = _descendant_text(root, "reportingOwner", "reportingOwnerId", "rptOwnerName")
    owner_cik = _descendant_text(root, "reportingOwner", "reportingOwnerId", "rptOwnerCik")
    period_of_report = _descendant_text(root, "periodOfReport")
    automatic_plan = str(_descendant_text(root, "aff10b5One") or "").strip().lower() in {"1", "true", "yes"}
    rows = []
    for element in root.iter():
        name = _local_name(element)
        if name not in {"nonDerivativeTransaction", "derivativeTransaction"}:
            continue
        code = (_descendant_text(element, "transactionCoding", "transactionCode") or "").upper()
        ownership_code = (
            _descendant_text(element, "ownershipNature", "directOrIndirectOwnership", "value") or ""
        ).upper()
        acquired_disposed = (
            _descendant_text(element, "transactionAmounts", "transactionAcquiredDisposedCode", "value") or ""
        ).upper()
        transaction_date = _descendant_text(element, "transactionDate", "value")
        security_title = _descendant_text(element, "securityTitle", "value")
        shares = _float(_descendant_text(element, "transactionAmounts", "transactionShares", "value"))
        price = _float(_descendant_text(element, "transactionAmounts", "transactionPricePerShare", "value"))
        derivative = name == "derivativeTransaction"
        transaction_key = _stable_id(
            (owner_cik, ticker, derivative, code, transaction_date, security_title, shares, price, acquired_disposed)
        )
        transaction_id = _stable_id(
            (accession_number, owner_cik, ticker, derivative, code, transaction_date, security_title, shares, price)
        )
        rows.append(
            {
                "id": transaction_id,
                "accession_number": accession_number,
                "document_type": document_type,
                "amendment": document_type.upper().endswith("/A"),
                "ticker": ticker,
                "issuer_cik": issuer_cik,
                "owner_name": owner_name,
                "owner_cik": owner_cik,
                "period_of_report": period_of_report,
                "transaction_date": transaction_date,
                "published_at": published.isoformat(),
                "fetched_at": fetched.isoformat(),
                "source": "sec_edgar_form4",
                "security_title": security_title,
                "transaction_code": code,
                "transaction_kind": _TRANSACTION_KINDS.get(code, "other"),
                "open_market": code in _OPEN_MARKET_CODES,
                "automatic_plan": automatic_plan,
                "acquired_disposed": acquired_disposed,
                "shares": shares,
                "price_per_share": price,
                "transaction_value": shares * price if shares is not None and price is not None else None,
                "derivative": derivative,
                "ownership": {"D": "direct", "I": "indirect"}.get(ownership_code, "unknown"),
                "source_url": source_url,
                "transaction_key": transaction_key,
            }
        )
    return rows


def visible_insider_transactions(records: Iterable[dict[str, Any]], *, as_of: Any) -> list[dict[str, Any]]:
    ceiling = _datetime(as_of)
    if ceiling is None:
        raise ValueError("as_of must be a valid datetime.")
    visible_by_transaction: dict[str, dict[str, Any]] = {}
    for record in records:
        transaction_id = str(record.get("id") or "").strip() or _stable_id(
            (
                record.get("accession_number"),
                record.get("ticker"),
                record.get("owner_cik"),
                record.get("transaction_code"),
                record.get("transaction_date"),
            )
        )
        published = _datetime(record.get("published_at") or record.get("acceptance_datetime"))
        if published is None or published > ceiling:
            continue
        transaction_key = str(record.get("transaction_key") or transaction_id)
        candidate = {
            **record,
            "id": transaction_id,
            "published_at": published.isoformat(),
            "source": record.get("source") or "sec_edgar_form4",
            "fetched_at": (_datetime(record.get("fetched_at")) or published).isoformat(),
        }
        existing = visible_by_transaction.get(transaction_key)
        if existing is None or (candidate["published_at"], bool(candidate.get("amendment"))) > (
            existing["published_at"],
            bool(existing.get("amendment")),
        ):
            visible_by_transaction[transaction_key] = candidate
    return sorted(visible_by_transaction.values(), key=lambda row: (row["published_at"], row["id"]))
