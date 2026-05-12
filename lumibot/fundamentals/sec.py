import json
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests


SEC_DATA_BASE_URL = "https://data.sec.gov"
SEC_ARCHIVES_BASE_URL = "https://www.sec.gov/Archives/edgar/data/"
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
DEFAULT_SEC_USER_AGENT = "LumiBot open-source trading framework support@lumiwealth.com"


INCOME_STATEMENT_TAGS = {
    "revenue": ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet"],
    "cost_of_revenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
    "gross_profit": ["GrossProfit"],
    "operating_income": ["OperatingIncomeLoss"],
    "net_income": ["NetIncomeLoss"],
    "eps_basic": ["EarningsPerShareBasic"],
    "eps_diluted": ["EarningsPerShareDiluted"],
}

BALANCE_SHEET_TAGS = {
    "assets": ["Assets"],
    "current_assets": ["AssetsCurrent"],
    "cash": ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"],
    "liabilities": ["Liabilities"],
    "current_liabilities": ["LiabilitiesCurrent"],
    "debt": ["LongTermDebtAndFinanceLeaseObligationsCurrent", "LongTermDebtCurrent", "LongTermDebt"],
    "equity": ["StockholdersEquity"],
    "shares_outstanding": ["EntityCommonStockSharesOutstanding", "CommonStocksIncludingAdditionalPaidInCapital"],
}

CASH_FLOW_TAGS = {
    "operating_cash_flow": ["NetCashProvidedByUsedInOperatingActivities"],
    "capex": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    "free_cash_flow_components": ["NetCashProvidedByUsedInOperatingActivities", "PaymentsToAcquirePropertyPlantAndEquipment"],
    "investing_cash_flow": ["NetCashProvidedByUsedInInvestingActivities"],
    "financing_cash_flow": ["NetCashProvidedByUsedInFinancingActivities"],
    "dividends_paid": ["PaymentsOfDividends", "PaymentsOfDividendsCommonStock"],
    "buybacks": ["PaymentsForRepurchaseOfCommonStock"],
}

_DEFAULT_COMPANY_FACTS_LIMIT = 80
_PRIORITY_COMPANY_FACT_TAGS = tuple(
    dict.fromkeys(
        [
            *(tag for tags in INCOME_STATEMENT_TAGS.values() for tag in tags),
            *(tag for tags in BALANCE_SHEET_TAGS.values() for tag in tags),
            *(tag for tags in CASH_FLOW_TAGS.values() for tag in tags),
            "ResearchAndDevelopmentExpense",
            "SellingGeneralAndAdministrativeExpense",
            "OperatingExpenses",
            "InterestExpenseNonOperating",
            "IncomeTaxExpenseBenefit",
            "InventoryNet",
            "AccountsReceivableNetCurrent",
            "AccountsPayableCurrent",
            "LongTermDebtAndFinanceLeaseObligationsNoncurrent",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
        ]
    )
)


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return None


def _as_of_datetime(value: Any) -> datetime:
    parsed = _parse_dt(value)
    if parsed is not None:
        return parsed
    return datetime.now(timezone.utc)


def _same_tz(value: datetime, reference: datetime) -> datetime:
    if value.tzinfo is None and reference.tzinfo is not None:
        return value.replace(tzinfo=reference.tzinfo)
    if value.tzinfo is not None and reference.tzinfo is None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if value.tzinfo is not None and reference.tzinfo is not None:
        return value.astimezone(reference.tzinfo)
    return value


def _strip_html(text: str) -> str:
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;?", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


class SECFundamentals:
    """Direct SEC EDGAR client with mandatory local caching and point-in-time helpers."""

    def __init__(
        self,
        strategy: Any | None = None,
        *,
        cache_dir: str | os.PathLike[str] | None = None,
        user_agent: str | None = None,
        min_request_interval_seconds: float = 0.2,
    ) -> None:
        self.strategy = strategy
        self.cache_dir = Path(
            cache_dir
            or os.environ.get("LUMIBOT_SEC_CACHE_DIR")
            or Path.home() / ".lumibot" / "cache" / "sec"
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.user_agent = user_agent or os.environ.get("LUMIBOT_SEC_USER_AGENT") or DEFAULT_SEC_USER_AGENT
        self.min_request_interval_seconds = max(float(min_request_interval_seconds), 0.0)
        self._last_request_at = 0.0

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
        }

    def _archive_headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
        }

    def _cache_path(self, *parts: str) -> Path:
        safe = [re.sub(r"[^A-Za-z0-9_.=-]+", "_", str(part)).strip("_") for part in parts]
        return self.cache_dir.joinpath(*safe)

    def _get_json(self, url: str, cache_path: Path) -> dict[str, Any]:
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))
        self._rate_limit()
        response = requests.get(url, headers=self._headers(), timeout=30)
        response.raise_for_status()
        payload = response.json()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return payload

    def _get_text(self, url: str, cache_path: Path) -> str:
        if cache_path.exists():
            return cache_path.read_text(encoding="utf-8", errors="replace")
        self._rate_limit()
        response = requests.get(url, headers=self._archive_headers(), timeout=30)
        response.raise_for_status()
        text = response.text
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(text, encoding="utf-8", errors="replace")
        return text

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.min_request_interval_seconds:
            time.sleep(self.min_request_interval_seconds - elapsed)
        self._last_request_at = time.monotonic()

    def _strategy_as_of(self) -> datetime:
        if self.strategy is not None and hasattr(self.strategy, "get_datetime"):
            try:
                return _as_of_datetime(self.strategy.get_datetime())
            except Exception:
                pass
        return datetime.now(timezone.utc)

    def ticker_to_cik(self, symbol: str) -> str:
        symbol_upper = str(symbol).upper().strip()
        payload = self._get_json(SEC_COMPANY_TICKERS_URL, self._cache_path("company_tickers.json"))
        for entry in payload.values():
            if str(entry.get("ticker", "")).upper() == symbol_upper:
                return f"{int(entry['cik_str']):010d}"
        raise ValueError(f"No SEC CIK found for ticker {symbol!r}.")

    def get_submissions(self, symbol: str) -> dict[str, Any]:
        cik = self.ticker_to_cik(symbol)
        url = f"{SEC_DATA_BASE_URL}/submissions/CIK{cik}.json"
        return self._get_json(url, self._cache_path("submissions", f"CIK{cik}.json"))

    def get_company_facts(
        self,
        symbol: str,
        *,
        as_of: Any | None = None,
        raw: bool = False,
        max_facts: int | None = _DEFAULT_COMPANY_FACTS_LIMIT,
    ) -> dict[str, Any]:
        cik = self.ticker_to_cik(symbol)
        url = f"{SEC_DATA_BASE_URL}/api/xbrl/companyfacts/CIK{cik}.json"
        payload = self._get_json(url, self._cache_path("companyfacts", f"CIK{cik}.json"))
        if raw:
            return payload
        facts = payload.get("facts", {}).get("us-gaap", {})
        as_of_dt = _as_of_datetime(as_of) if as_of is not None else self._strategy_as_of()
        compact: dict[str, Any] = {"symbol": symbol.upper(), "cik": cik, "as_of": as_of_dt.isoformat(), "facts": {}}
        ordered_tags = [tag for tag in _PRIORITY_COMPANY_FACT_TAGS if tag in facts]
        priority_seen = set(ordered_tags)
        ordered_tags.extend(tag for tag in sorted(facts) if tag not in priority_seen)
        limit = None if max_facts is None else max(int(max_facts), 1)
        stopped_for_limit = False
        for tag in ordered_tags:
            if limit is not None and len(compact["facts"]) >= limit:
                stopped_for_limit = True
                break
            tag_payload = facts[tag]
            units = tag_payload.get("units", {})
            latest = self._latest_fact_from_units(units, as_of_dt)
            if latest is not None:
                compact["facts"][tag] = latest
        compact["fact_count"] = len(compact["facts"])
        compact["truncated"] = stopped_for_limit
        compact["max_facts"] = max_facts
        return compact

    def get_income_statement(self, symbol: str, *, as_of: Any | None = None, raw: bool = False) -> dict[str, Any]:
        return self._normalized_statement(symbol, INCOME_STATEMENT_TAGS, as_of=as_of, raw=raw, statement="income_statement")

    def get_balance_sheet(self, symbol: str, *, as_of: Any | None = None, raw: bool = False) -> dict[str, Any]:
        return self._normalized_statement(symbol, BALANCE_SHEET_TAGS, as_of=as_of, raw=raw, statement="balance_sheet")

    def get_cash_flow(self, symbol: str, *, as_of: Any | None = None, raw: bool = False) -> dict[str, Any]:
        statement = self._normalized_statement(symbol, CASH_FLOW_TAGS, as_of=as_of, raw=raw, statement="cash_flow")
        rows = statement.get("values", {})
        ocf = rows.get("operating_cash_flow", {}).get("value") if isinstance(rows.get("operating_cash_flow"), dict) else None
        capex = rows.get("capex", {}).get("value") if isinstance(rows.get("capex"), dict) else None
        if ocf is not None and capex is not None:
            try:
                rows["free_cash_flow"] = {"value": float(ocf) - abs(float(capex)), "derived": True}
            except Exception:
                pass
        return statement

    def _normalized_statement(
        self,
        symbol: str,
        tag_map: dict[str, list[str]],
        *,
        as_of: Any | None,
        raw: bool,
        statement: str,
    ) -> dict[str, Any]:
        raw_facts = self.get_company_facts(symbol, raw=True)
        if raw:
            return raw_facts
        as_of_dt = _as_of_datetime(as_of) if as_of is not None else self._strategy_as_of()
        facts = raw_facts.get("facts", {}).get("us-gaap", {})
        values: dict[str, Any] = {}
        for field, tags in tag_map.items():
            for tag in tags:
                tag_payload = facts.get(tag)
                if not tag_payload:
                    continue
                latest = self._latest_fact_from_units(tag_payload.get("units", {}), as_of_dt)
                if latest is not None:
                    values[field] = {"tag": tag, **latest}
                    break
        return {
            "symbol": symbol.upper(),
            "cik": self.ticker_to_cik(symbol),
            "statement": statement,
            "as_of": as_of_dt.isoformat(),
            "values": values,
        }

    def _latest_fact_from_units(self, units: dict[str, Any], as_of: datetime) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        for unit, facts in units.items():
            if not isinstance(facts, list):
                continue
            for fact in facts:
                if not isinstance(fact, dict):
                    continue
                filed = _parse_dt(fact.get("filed") or fact.get("acceptanceDateTime"))
                if filed is None:
                    continue
                filed = _same_tz(filed, as_of)
                if filed <= as_of:
                    candidates.append({**fact, "unit": unit})
        if not candidates:
            return None
        candidates.sort(key=lambda row: (str(row.get("filed") or ""), str(row.get("end") or "")), reverse=True)
        chosen = candidates[0]
        return {
            "value": chosen.get("val"),
            "unit": chosen.get("unit"),
            "filed": chosen.get("filed"),
            "form": chosen.get("form"),
            "fy": chosen.get("fy"),
            "fp": chosen.get("fp"),
            "frame": chosen.get("frame"),
            "start": chosen.get("start"),
            "end": chosen.get("end"),
            "accession_number": chosen.get("accn"),
        }

    def get_filings(
        self,
        symbol: str,
        *,
        form: str | None = None,
        as_of: Any | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        submissions = self.get_submissions(symbol)
        as_of_dt = _as_of_datetime(as_of) if as_of is not None else self._strategy_as_of()
        recent = submissions.get("filings", {}).get("recent", {})
        rows = []
        forms = recent.get("form", [])
        accession_numbers = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        report_dates = recent.get("reportDate", [])
        acceptances = recent.get("acceptanceDateTime", [])
        primary_docs = recent.get("primaryDocument", [])
        descriptions = recent.get("primaryDocDescription", [])
        for idx, filing_form in enumerate(forms):
            if form and str(filing_form).upper() != str(form).upper():
                continue
            filed_raw = acceptances[idx] if idx < len(acceptances) and acceptances[idx] else filing_dates[idx]
            filed_dt = _parse_dt(filed_raw)
            if filed_dt is None:
                continue
            filed_dt = _same_tz(filed_dt, as_of_dt)
            if filed_dt > as_of_dt:
                continue
            accession = accession_numbers[idx]
            primary_document = primary_docs[idx] if idx < len(primary_docs) else ""
            rows.append(
                {
                    "symbol": symbol.upper(),
                    "cik": submissions.get("cik"),
                    "form": filing_form,
                    "accession_number": accession,
                    "filing_date": filing_dates[idx] if idx < len(filing_dates) else None,
                    "report_date": report_dates[idx] if idx < len(report_dates) else None,
                    "acceptance_datetime": acceptances[idx] if idx < len(acceptances) else None,
                    "primary_document": primary_document,
                    "description": descriptions[idx] if idx < len(descriptions) else None,
                    "document_url": self._filing_url(submissions.get("cik"), accession, primary_document),
                }
            )
            if len(rows) >= max(int(limit), 1):
                break
        return {"symbol": symbol.upper(), "as_of": as_of_dt.isoformat(), "filings": rows}

    def search_filing(
        self,
        symbol: str,
        *,
        accession_number: str,
        query: str,
        primary_document: str | None = None,
        max_results: int = 5,
        context_chars: int = 600,
    ) -> dict[str, Any]:
        text_result = self.get_filing_document(
            symbol,
            accession_number=accession_number,
            primary_document=primary_document,
            as_text=True,
        )
        text = text_result["text"]
        terms = [term for term in re.split(r"\s+", query.strip()) if term]
        matches: list[dict[str, Any]] = []
        lowered = text.lower()
        for term in terms:
            for match in re.finditer(re.escape(term.lower()), lowered):
                start = max(match.start() - context_chars, 0)
                end = min(match.end() + context_chars, len(text))
                matches.append({"term": term, "start": start, "end": end, "context": text[start:end].strip()})
                if len(matches) >= max_results:
                    break
            if len(matches) >= max_results:
                break
        return {
            "symbol": symbol.upper(),
            "accession_number": accession_number,
            "query": query,
            "match_count": len(matches),
            "matches": matches,
            "document_url": text_result["document_url"],
        }

    def get_filing_document(
        self,
        symbol: str,
        *,
        accession_number: str,
        primary_document: str | None = None,
        as_text: bool = True,
        max_chars: int | None = 20000,
    ) -> dict[str, Any]:
        cik = self.ticker_to_cik(symbol)
        if not primary_document:
            filings = self.get_filings(symbol, limit=1000)
            for filing in filings["filings"]:
                if filing["accession_number"] == accession_number:
                    primary_document = filing.get("primary_document")
                    break
        if not primary_document:
            raise ValueError("primary_document is required when accession_number is not in recent submissions.")
        url = self._filing_url(cik, accession_number, primary_document)
        raw = self._get_text(url, self._cache_path("filings", cik, accession_number, primary_document))
        text = _strip_html(raw) if as_text else raw
        original_length = len(text)
        truncated = False
        if max_chars is not None and original_length > int(max_chars):
            text = text[: int(max_chars)]
            truncated = True
        return {
            "symbol": symbol.upper(),
            "cik": cik,
            "accession_number": accession_number,
            "primary_document": primary_document,
            "document_url": url,
            "as_text": as_text,
            "text": text,
            "original_length": original_length,
            "truncated": truncated,
            "max_chars": max_chars,
        }

    def _filing_url(self, cik: Any, accession_number: str, primary_document: str) -> str:
        cik_int = str(int(str(cik).lstrip("0") or "0"))
        accession_clean = str(accession_number).replace("-", "")
        return urljoin(SEC_ARCHIVES_BASE_URL, f"{cik_int}/{accession_clean}/{primary_document}")
