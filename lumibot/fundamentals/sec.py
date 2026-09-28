import copy
import html
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
DEFAULT_MUTABLE_CACHE_TTL_SECONDS = 300.0


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
    text = re.sub(r"(?is)<script\b.*?</script\b[^>]*>|<style\b.*?</style\b[^>]*>", " ", text)
    text = re.sub(r"(?is)<ix:hidden.*?</ix:hidden>", " ", text)
    text = re.sub(r"(?is)</?(?:p|div|br|tr|table|section|article|h[1-6])\b[^>]*>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


_FILING_SECTION_RE = re.compile(
    r"(?im)(?:^|\n)\s*(item\s+(?:1a|1b|1|2|3|4|5|6|7a|7|8|9a|9b|9|10|11|12|13|14|15)\.?\s+[^\n]{0,180})"
)


def _filing_section_id(heading: str) -> str:
    match = re.search(r"(?i)item\s+(1a|1b|1|2|3|4|5|6|7a|7|8|9a|9b|9|10|11|12|13|14|15)", heading)
    if not match:
        return ""
    return f"item_{match.group(1).lower()}"


def _section_alias_candidates(section: str) -> list[str]:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(section or "").strip().lower()).strip("_")
    aliases = {
        "risk": ["item_1a"],
        "risk_factors": ["item_1a"],
        "business": ["item_1"],
        "mda": ["item_7", "item_2"],
        "md_a": ["item_7", "item_2"],
        "management_discussion": ["item_7", "item_2"],
        "results_of_operations": ["item_7", "item_2"],
        "liquidity": ["item_7", "item_2"],
        "market_risk": ["item_7a", "item_3"],
        "financial_statements": ["item_8", "item_1"],
        "controls": ["item_9a", "item_4"],
    }
    if normalized.startswith("item_"):
        return [normalized]
    if normalized.startswith("item"):
        return [f"item_{normalized[4:]}"]
    return aliases.get(normalized, [normalized])


class SECTickerNotFoundError(ValueError):
    """The SEC ticker map has no CIK for this symbol, so EDGAR has no filings for it."""


class SECFundamentals:
    """Direct SEC EDGAR client with mandatory local caching and point-in-time helpers."""

    def __init__(
        self,
        strategy: Any | None = None,
        *,
        cache_dir: str | os.PathLike[str] | None = None,
        user_agent: str | None = None,
        min_request_interval_seconds: float = 0.2,
        cache_mode: str = "auto",
        mutable_cache_ttl_seconds: float = DEFAULT_MUTABLE_CACHE_TTL_SECONDS,
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
        normalized_cache_mode = str(cache_mode).strip().lower()
        if normalized_cache_mode not in {"auto", "live", "backtest"}:
            raise ValueError("cache_mode must be one of: auto, live, backtest")
        self.cache_mode = normalized_cache_mode
        self.mutable_cache_ttl_seconds = max(float(mutable_cache_ttl_seconds), 0.0)
        self._last_request_at = 0.0

    @staticmethod
    def _cache_meta_path(cache_path: Path) -> Path:
        return cache_path.with_name(f"{cache_path.name}.meta.json")

    def _cache_meta(self, cache_path: Path) -> dict[str, Any]:
        meta_path = self._cache_meta_path(cache_path)
        if not meta_path.exists():
            return {}
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _response_header(headers: Any, name: str) -> str | None:
        if not headers:
            return None
        for key, value in headers.items():
            if str(key).lower() == name.lower() and value:
                return str(value)
        return None

    def _record_cache_fetch(self, cache_path: Path, response_headers: Any = None) -> str:
        fetched_at = datetime.now(timezone.utc).isoformat()
        previous = self._cache_meta(cache_path)
        etag = self._response_header(response_headers, "etag") or previous.get("etag")
        last_modified = self._response_header(response_headers, "last-modified") or previous.get("last_modified")
        metadata = {"fetched_at": fetched_at}
        if etag:
            metadata["etag"] = etag
        if last_modified:
            metadata["last_modified"] = last_modified
        meta_path = self._cache_meta_path(cache_path)
        meta_path.write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")
        return fetched_at

    def _cache_fetched_at(self, cache_path: Path) -> str:
        value = self._cache_meta(cache_path).get("fetched_at")
        if value:
            return str(value)
        timestamp = cache_path.stat().st_mtime if cache_path.exists() else time.time()
        return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()

    def _revalidation_headers(self, cache_path: Path) -> dict[str, str]:
        metadata = self._cache_meta(cache_path)
        headers: dict[str, str] = {}
        if metadata.get("etag"):
            headers["If-None-Match"] = str(metadata["etag"])
        if metadata.get("last_modified"):
            headers["If-Modified-Since"] = str(metadata["last_modified"])
        return headers

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

    def _resolved_cache_mode(self) -> str:
        if self.cache_mode != "auto":
            return self.cache_mode
        if self.strategy is not None and bool(getattr(self.strategy, "is_backtesting", False)):
            return "backtest"
        return "live"

    def _cache_is_usable(self, cache_path: Path, *, mutable: bool) -> bool:
        if not cache_path.exists():
            return False
        if not mutable or self._resolved_cache_mode() == "backtest":
            return True
        age_seconds = max(time.time() - cache_path.stat().st_mtime, 0.0)
        return age_seconds <= self.mutable_cache_ttl_seconds

    def _get_json(self, url: str, cache_path: Path, *, mutable: bool = False) -> dict[str, Any]:
        if self._cache_is_usable(cache_path, mutable=mutable):
            return json.loads(cache_path.read_text(encoding="utf-8"))
        self._rate_limit()
        headers = self._headers()
        if mutable and cache_path.exists():
            headers.update(self._revalidation_headers(cache_path))
        response = requests.get(url, headers=headers, timeout=30)
        if getattr(response, "status_code", None) == 304 and cache_path.exists():
            os.utime(cache_path, None)
            self._record_cache_fetch(cache_path, getattr(response, "headers", None))
            return json.loads(cache_path.read_text(encoding="utf-8"))
        response.raise_for_status()
        payload = response.json()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        self._record_cache_fetch(cache_path, getattr(response, "headers", None))
        return payload

    def _get_text(self, url: str, cache_path: Path, *, mutable: bool = False) -> str:
        if self._cache_is_usable(cache_path, mutable=mutable):
            return cache_path.read_text(encoding="utf-8", errors="replace")
        self._rate_limit()
        headers = self._archive_headers()
        if mutable and cache_path.exists():
            headers.update(self._revalidation_headers(cache_path))
        response = requests.get(url, headers=headers, timeout=30)
        if getattr(response, "status_code", None) == 304 and cache_path.exists():
            os.utime(cache_path, None)
            self._record_cache_fetch(cache_path, getattr(response, "headers", None))
            return cache_path.read_text(encoding="utf-8", errors="replace")
        response.raise_for_status()
        text = response.text
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(text, encoding="utf-8", errors="replace")
        self._record_cache_fetch(cache_path, getattr(response, "headers", None))
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

    def _resolve_as_of(self, as_of: Any | None) -> datetime:
        strategy_as_of = self._strategy_as_of()
        if as_of is None:
            return strategy_as_of
        requested = _as_of_datetime(as_of)
        if self.strategy is not None and bool(getattr(self.strategy, "is_backtesting", False)):
            # A caller-supplied date must never reveal filings after the backtest clock.
            return min(_same_tz(requested, strategy_as_of), strategy_as_of)
        return requested

    def ticker_to_cik(self, symbol: str) -> str:
        symbol_upper = str(symbol).upper().strip()
        payload = self._get_json(
            SEC_COMPANY_TICKERS_URL,
            self._cache_path("company_tickers.json"),
            mutable=True,
        )
        for entry in payload.values():
            if str(entry.get("ticker", "")).upper() == symbol_upper:
                return f"{int(entry['cik_str']):010d}"
        raise SECTickerNotFoundError(f"No SEC CIK found for ticker {symbol!r}.")

    def _get_submissions_payload(self, symbol: str) -> dict[str, Any]:
        cik = self.ticker_to_cik(symbol)
        url = f"{SEC_DATA_BASE_URL}/submissions/CIK{cik}.json"
        cache_path = self._cache_path("submissions", f"CIK{cik}.json")
        payload = self._get_json(
            url,
            cache_path,
            mutable=True,
        )
        return {
            **payload,
            "id": f"sec-submissions-{cik}",
            "source": "sec_edgar_submissions",
            "source_url": url,
            "fetched_at": self._cache_fetched_at(cache_path),
        }

    def get_submissions(self, symbol: str, *, as_of: Any | None = None) -> dict[str, Any]:
        as_of_dt = self._resolve_as_of(as_of)
        return self._filter_submissions_as_of(self._get_submissions_payload(symbol), as_of_dt)

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
        cache_path = self._cache_path("companyfacts", f"CIK{cik}.json")
        payload = self._get_json(
            url,
            cache_path,
            mutable=True,
        )
        as_of_dt = self._resolve_as_of(as_of)
        provenance = {
            "id": f"sec-companyfacts-{cik}",
            "source": "sec_edgar_companyfacts",
            "source_url": url,
            "fetched_at": self._cache_fetched_at(cache_path),
        }
        if raw:
            filtered = self._filter_company_facts_as_of(payload, as_of_dt)
            filtered.update(provenance)
            filtered["published_at"] = self._latest_company_fact_publication(filtered)
            return filtered
        facts = payload.get("facts", {}).get("us-gaap", {})
        compact: dict[str, Any] = {
            "symbol": symbol.upper(),
            "cik": cik,
            "as_of": as_of_dt.isoformat(),
            **provenance,
            "facts": {},
        }
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
        compact["published_at"] = self._latest_company_fact_publication(
            self._filter_company_facts_as_of(payload, as_of_dt)
        )
        return compact

    @staticmethod
    def _latest_company_fact_publication(payload: dict[str, Any]) -> str | None:
        latest: datetime | None = None
        facts = payload.get("facts") or {}
        for taxonomy in facts.values() if isinstance(facts, dict) else ():
            for fact in taxonomy.values() if isinstance(taxonomy, dict) else ():
                units = fact.get("units") if isinstance(fact, dict) else None
                for rows in units.values() if isinstance(units, dict) else ():
                    for row in rows if isinstance(rows, list) else ():
                        published = _parse_dt(row.get("acceptanceDateTime") or row.get("filed")) if isinstance(row, dict) else None
                        if published is not None and (latest is None or _same_tz(published, latest) > latest):
                            latest = published
        return latest.isoformat() if latest is not None else None

    def _filter_company_facts_as_of(self, payload: dict[str, Any], as_of: datetime) -> dict[str, Any]:
        filtered = copy.deepcopy(payload)
        facts = filtered.get("facts", {})
        if isinstance(facts, dict):
            for taxonomy in facts.values():
                if not isinstance(taxonomy, dict):
                    continue
                for fact in taxonomy.values():
                    units = fact.get("units") if isinstance(fact, dict) else None
                    if not isinstance(units, dict):
                        continue
                    for unit, rows in list(units.items()):
                        if not isinstance(rows, list):
                            continue
                        visible_rows = []
                        for row in rows:
                            if not isinstance(row, dict):
                                continue
                            published = _parse_dt(row.get("filed") or row.get("acceptanceDateTime"))
                            if published is None or _same_tz(published, as_of) <= as_of:
                                visible_rows.append(row)
                        units[unit] = visible_rows
        filtered["as_of"] = as_of.isoformat()
        return filtered

    def _filter_submissions_as_of(self, payload: dict[str, Any], as_of: datetime) -> dict[str, Any]:
        filtered = copy.deepcopy(payload)
        recent = filtered.get("filings", {}).get("recent", {})
        if not isinstance(recent, dict):
            filtered["as_of"] = as_of.isoformat()
            filtered["published_at"] = None
            return filtered
        forms = recent.get("form")
        if not isinstance(forms, list):
            filtered["as_of"] = as_of.isoformat()
            filtered["published_at"] = None
            return filtered
        acceptances = recent.get("acceptanceDateTime", [])
        filing_dates = recent.get("filingDate", [])
        keep_indexes = []
        for index in range(len(forms)):
            raw = acceptances[index] if index < len(acceptances) and acceptances[index] else None
            if not raw and index < len(filing_dates):
                raw = filing_dates[index]
            published = _parse_dt(raw)
            if published is not None and _same_tz(published, as_of) <= as_of:
                keep_indexes.append(index)
        for key, values in list(recent.items()):
            if isinstance(values, list):
                recent[key] = [values[index] for index in keep_indexes if index < len(values)]
        filtered["as_of"] = as_of.isoformat()
        recent_acceptances = recent.get("acceptanceDateTime", [])
        recent_filing_dates = recent.get("filingDate", [])
        published_values = [value for value in recent_acceptances if value] or [
            value for value in recent_filing_dates if value
        ]
        filtered["published_at"] = max(published_values, default=None)
        return filtered

    def get_income_statement(self, symbol: str, *, as_of: Any | None = None, raw: bool = False) -> dict[str, Any]:
        return self._normalized_statement(
            symbol, INCOME_STATEMENT_TAGS, as_of=as_of, raw=raw, statement="income_statement"
        )

    def get_balance_sheet(self, symbol: str, *, as_of: Any | None = None, raw: bool = False) -> dict[str, Any]:
        return self._normalized_statement(symbol, BALANCE_SHEET_TAGS, as_of=as_of, raw=raw, statement="balance_sheet")

    def get_cash_flow(self, symbol: str, *, as_of: Any | None = None, raw: bool = False) -> dict[str, Any]:
        statement = self._normalized_statement(symbol, CASH_FLOW_TAGS, as_of=as_of, raw=raw, statement="cash_flow")
        rows = statement.get("values", {})
        ocf = (
            rows.get("operating_cash_flow", {}).get("value")
            if isinstance(rows.get("operating_cash_flow"), dict)
            else None
        )
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
        raw_facts = self.get_company_facts(symbol, as_of=as_of, raw=True)
        if raw:
            return raw_facts
        as_of_dt = self._resolve_as_of(as_of)
        facts = raw_facts.get("facts", {}).get("us-gaap", {})
        field_candidates: dict[str, list[dict[str, Any]]] = {}
        for field, tags in tag_map.items():
            candidates = self._fact_candidates_for_tags(facts, tags, as_of_dt)
            if candidates:
                field_candidates[field] = candidates

        anchor = self._statement_anchor(field_candidates)
        values: dict[str, Any] = {}
        warnings: list[dict[str, Any]] = []
        for field, candidates in field_candidates.items():
            chosen = self._best_matching_statement_fact(candidates, anchor)
            if chosen is not None:
                values[field] = chosen
                continue
            latest = candidates[0]
            warnings.append(
                {
                    "field": field,
                    "message": (
                        "Latest fact did not match the statement period anchor and was omitted "
                        "to avoid mixing facts from different SEC filings or periods."
                    ),
                    "latest_candidate": {
                        key: latest.get(key)
                        for key in ("tag", "filed", "form", "fy", "fp", "start", "end", "accession_number")
                    },
                    "anchor": {
                        key: anchor.get(key) if anchor else None
                        for key in ("tag", "filed", "form", "fy", "fp", "start", "end", "accession_number")
                    },
                }
            )
        return {
            "symbol": symbol.upper(),
            "cik": self.ticker_to_cik(symbol),
            "statement": statement,
            "as_of": as_of_dt.isoformat(),
            "id": raw_facts.get("id"),
            "source": raw_facts.get("source"),
            "source_url": raw_facts.get("source_url"),
            "published_at": raw_facts.get("published_at"),
            "fetched_at": raw_facts.get("fetched_at"),
            "values": values,
            "anchor": {
                key: anchor.get(key)
                for key in ("tag", "filed", "form", "fy", "fp", "start", "end", "accession_number")
            }
            if anchor
            else None,
            "warnings": warnings,
        }

    def _fact_candidates_for_tags(
        self,
        facts: dict[str, Any],
        tags: list[str],
        as_of: datetime,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for tag in tags:
            tag_payload = facts.get(tag)
            if not tag_payload:
                continue
            units = tag_payload.get("units", {})
            for unit, unit_facts in units.items():
                if not isinstance(unit_facts, list):
                    continue
                for fact in unit_facts:
                    if not isinstance(fact, dict):
                        continue
                    filed = _parse_dt(fact.get("filed") or fact.get("acceptanceDateTime"))
                    if filed is None:
                        continue
                    filed = _same_tz(filed, as_of)
                    if filed > as_of:
                        continue
                    candidates.append(
                        {
                            "tag": tag,
                            "value": fact.get("val"),
                            "unit": unit,
                            "filed": fact.get("filed"),
                            "form": fact.get("form"),
                            "fy": fact.get("fy"),
                            "fp": fact.get("fp"),
                            "frame": fact.get("frame"),
                            "start": fact.get("start"),
                            "end": fact.get("end"),
                            "accession_number": fact.get("accn"),
                        }
                    )
        candidates.sort(key=lambda row: self._statement_candidate_sort_key(row, tags), reverse=True)
        return candidates

    def _statement_candidate_sort_key(self, row: dict[str, Any], tags: list[str] | None = None) -> tuple[Any, ...]:
        form_priority = {
            "10-K": 5,
            "20-F": 5,
            "40-F": 5,
            "10-Q": 4,
            "8-K": 2,
        }.get(str(row.get("form") or "").upper(), 1)
        tag_priority = 0
        if tags:
            try:
                tag_priority = len(tags) - tags.index(str(row.get("tag") or ""))
            except ValueError:
                tag_priority = 0
        return (
            str(row.get("filed") or ""),
            str(row.get("end") or ""),
            str(row.get("start") or ""),
            form_priority,
            tag_priority,
        )

    def _statement_anchor(self, field_candidates: dict[str, list[dict[str, Any]]]) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        for rows in field_candidates.values():
            candidates.extend(rows)
        if not candidates:
            return None
        return sorted(candidates, key=lambda row: self._statement_candidate_sort_key(row), reverse=True)[0]

    def _best_matching_statement_fact(
        self,
        candidates: list[dict[str, Any]],
        anchor: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not candidates:
            return None
        if anchor is None:
            return candidates[0]
        for candidate in candidates:
            if self._same_statement_period(candidate, anchor):
                return candidate
        return None

    def _same_statement_period(self, candidate: dict[str, Any], anchor: dict[str, Any]) -> bool:
        candidate_accn = str(candidate.get("accession_number") or "").strip()
        anchor_accn = str(anchor.get("accession_number") or "").strip()
        if candidate_accn and anchor_accn and candidate_accn == anchor_accn:
            return True
        candidate_end = str(candidate.get("end") or "").strip()
        anchor_end = str(anchor.get("end") or "").strip()
        if not candidate_end or not anchor_end or candidate_end != anchor_end:
            return False
        for key in ("start", "fy", "fp", "form"):
            candidate_value = str(candidate.get(key) or "").strip()
            anchor_value = str(anchor.get(key) or "").strip()
            if candidate_value and anchor_value and candidate_value != anchor_value:
                return False
        return True

    def _latest_fact_for_tags(
        self,
        facts: dict[str, Any],
        tags: list[str],
        as_of: datetime,
    ) -> dict[str, Any] | None:
        candidates: list[tuple[str, dict[str, Any]]] = []
        for tag in tags:
            tag_payload = facts.get(tag)
            if not tag_payload:
                continue
            latest = self._latest_fact_from_units(tag_payload.get("units", {}), as_of)
            if latest is not None:
                candidates.append((tag, latest))
        if not candidates:
            return None
        candidates.sort(
            key=lambda item: (
                str(item[1].get("filed") or ""),
                str(item[1].get("end") or ""),
                -tags.index(item[0]),
            ),
            reverse=True,
        )
        tag, latest = candidates[0]
        return {"tag": tag, **latest}

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
        as_of_dt = self._resolve_as_of(as_of)
        try:
            submissions = self.get_submissions(symbol, as_of=as_of_dt)
        except SECTickerNotFoundError:
            # No CIK means EDGAR lists no filings for this symbol (ETFs, foreign
            # listings, crypto, private or fictional names). Report the absence
            # explicitly instead of raising: an agent tool error here blocked a
            # whole research decision that other evidence had already answered.
            return {
                "symbol": str(symbol).upper(),
                "as_of": as_of_dt.isoformat(),
                "source": "sec_edgar_submissions",
                "available": False,
                "reason": "no_sec_cik",
                "message": (
                    f"The SEC ticker map has no CIK for {str(symbol).upper()}, so EDGAR lists no filings for it. "
                    "Nothing was invented; use another point-in-time source or report the evidence as missing."
                ),
                "filings": [],
            }
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
                    "id": accession,
                    "source": "sec_edgar_submissions",
                    "source_url": submissions.get("source_url"),
                    "published_at": acceptances[idx] if idx < len(acceptances) and acceptances[idx] else filing_dates[idx],
                    "fetched_at": submissions.get("fetched_at"),
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
        return {
            "symbol": symbol.upper(),
            "as_of": as_of_dt.isoformat(),
            "id": submissions.get("id"),
            "source": submissions.get("source"),
            "source_url": submissions.get("source_url"),
            "published_at": max((row["published_at"] for row in rows), default=None),
            "fetched_at": submissions.get("fetched_at"),
            "filings": rows,
        }

    def search_filing(
        self,
        symbol: str,
        *,
        accession_number: str,
        query: str,
        primary_document: str | None = None,
        max_results: int = 5,
        context_chars: int = 600,
        as_of: Any | None = None,
    ) -> dict[str, Any]:
        text_result = self.get_filing_document(
            symbol,
            accession_number=accession_number,
            primary_document=primary_document,
            as_text=True,
            as_of=as_of,
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
            "id": text_result["id"],
            "source": text_result["source"],
            "source_url": text_result["source_url"],
            "published_at": text_result["published_at"],
            "fetched_at": text_result["fetched_at"],
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
        as_of: Any | None = None,
        verify_availability: bool = True,
    ) -> dict[str, Any]:
        cik = self.ticker_to_cik(symbol)
        as_of_dt = self._resolve_as_of(as_of)
        published_raw = None
        if verify_availability:
            submissions = self._get_submissions_payload(symbol)
            recent = submissions.get("filings", {}).get("recent", {})
            accessions = recent.get("accessionNumber", []) if isinstance(recent, dict) else []
            if accession_number in accessions:
                index = accessions.index(accession_number)
                acceptances = recent.get("acceptanceDateTime", [])
                filing_dates = recent.get("filingDate", [])
                published_raw = acceptances[index] if index < len(acceptances) and acceptances[index] else None
                if not published_raw and index < len(filing_dates):
                    published_raw = filing_dates[index]
                published = _parse_dt(published_raw)
                if published is not None and _same_tz(published, as_of_dt) > as_of_dt:
                    raise ValueError(
                        f"SEC accession {accession_number} was not public as of {as_of_dt.isoformat()}."
                    )
        if not primary_document:
            filings = self.get_filings(symbol, as_of=as_of_dt, limit=1000)
            for filing in filings["filings"]:
                if filing["accession_number"] == accession_number:
                    primary_document = filing.get("primary_document")
                    published_raw = filing.get("published_at")
                    break
        if not primary_document:
            raise ValueError("primary_document is required when accession_number is not in recent submissions.")
        url = self._filing_url(cik, accession_number, primary_document)
        cache_path = self._cache_path("filings", cik, accession_number, primary_document)
        raw = self._get_text(url, cache_path)
        text = _strip_html(raw) if as_text else raw
        original_length = len(text)
        truncated = False
        if max_chars is not None and original_length > int(max_chars):
            text = text[: int(max_chars)]
            truncated = True
        return {
            "symbol": symbol.upper(),
            "id": accession_number,
            "source": "sec_edgar_filing_document",
            "source_url": url,
            "published_at": published_raw,
            "fetched_at": self._cache_fetched_at(cache_path),
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

    def list_filing_sections(
        self,
        symbol: str,
        *,
        accession_number: str,
        primary_document: str | None = None,
        as_of: Any | None = None,
    ) -> dict[str, Any]:
        text_result = self.get_filing_document(
            symbol,
            accession_number=accession_number,
            primary_document=primary_document,
            as_text=True,
            max_chars=None,
            as_of=as_of,
        )
        text = text_result["text"]
        sections = self._detect_filing_sections(text)
        return {
            "symbol": symbol.upper(),
            "accession_number": accession_number,
            "id": text_result["id"],
            "source": text_result["source"],
            "source_url": text_result["source_url"],
            "published_at": text_result["published_at"],
            "fetched_at": text_result["fetched_at"],
            "document_url": text_result["document_url"],
            "section_count": len(sections),
            "sections": [
                {
                    "section_id": section["section_id"],
                    "heading": section["heading"],
                    "start": section["start"],
                    "end": section["end"],
                    "char_count": max(int(section["end"]) - int(section["start"]), 0),
                }
                for section in sections
            ],
        }

    def get_filing_section(
        self,
        symbol: str,
        *,
        accession_number: str,
        section: str,
        primary_document: str | None = None,
        max_chars: int | None = 12000,
        as_of: Any | None = None,
    ) -> dict[str, Any]:
        text_result = self.get_filing_document(
            symbol,
            accession_number=accession_number,
            primary_document=primary_document,
            as_text=True,
            max_chars=None,
            as_of=as_of,
        )
        text = text_result["text"]
        sections = self._detect_filing_sections(text)
        candidate_ids = _section_alias_candidates(section)
        matching_sections = [entry for entry in sections if entry["section_id"] in candidate_ids]
        selected = max(
            matching_sections,
            key=lambda entry: max(int(entry["end"]) - int(entry["start"]), 0),
            default=None,
        )
        if selected is None:
            return {
                "ok": False,
                "symbol": symbol.upper(),
                "accession_number": accession_number,
                "id": text_result["id"],
                "source": text_result["source"],
                "source_url": text_result["source_url"],
                "published_at": text_result["published_at"],
                "fetched_at": text_result["fetched_at"],
                "requested_section": section,
                "available_sections": [
                    {"section_id": entry["section_id"], "heading": entry["heading"]} for entry in sections
                ],
                "document_url": text_result["document_url"],
                "error": {
                    "type": "SectionNotFound",
                    "message": f"Could not find section {section!r} in filing.",
                },
            }
        section_text = text[int(selected["start"]) : int(selected["end"])].strip()
        original_length = len(section_text)
        truncated = False
        if max_chars is not None and original_length > int(max_chars):
            section_text = section_text[: int(max_chars)]
            truncated = True
        return {
            "ok": True,
            "symbol": symbol.upper(),
            "accession_number": accession_number,
            "id": text_result["id"],
            "source": text_result["source"],
            "source_url": text_result["source_url"],
            "published_at": text_result["published_at"],
            "fetched_at": text_result["fetched_at"],
            "requested_section": section,
            "section_id": selected["section_id"],
            "heading": selected["heading"],
            "document_url": text_result["document_url"],
            "text": section_text,
            "original_length": original_length,
            "truncated": truncated,
            "max_chars": max_chars,
        }

    def _detect_filing_sections(self, text: str) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for match in _FILING_SECTION_RE.finditer(text):
            heading = re.sub(r"\s+", " ", match.group(1)).strip()
            section_id = _filing_section_id(heading)
            if not section_id:
                continue
            key = (section_id, match.start(1))
            if key in seen:
                continue
            seen.add(key)
            matches.append(
                {
                    "section_id": section_id,
                    "heading": heading,
                    "start": match.start(1),
                    "end": len(text),
                }
            )
        for index, section in enumerate(matches):
            if index + 1 < len(matches):
                section["end"] = matches[index + 1]["start"]
        return matches

    def _filing_url(self, cik: Any, accession_number: str, primary_document: str) -> str:
        cik_int = str(int(str(cik).lstrip("0") or "0"))
        accession_clean = str(accession_number).replace("-", "")
        return urljoin(SEC_ARCHIVES_BASE_URL, f"{cik_int}/{accession_clean}/{primary_document}")
