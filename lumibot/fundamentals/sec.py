import copy
import html
import json
import os
import re
import sys
import threading
import time
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests

SEC_DATA_BASE_URL = "https://data.sec.gov"
SEC_ARCHIVES_BASE_URL = "https://www.sec.gov/Archives/edgar/data/"
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
DEFAULT_SEC_USER_AGENT = "LumiBot open-source trading framework support@lumiwealth.com"
DEFAULT_MUTABLE_CACHE_TTL_SECONDS = 300.0
# SEC fair access allows at most 10 requests per second per client. 0.11 s between request
# starts keeps every LumiBot process just under that, including parallel preload() workers.
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.11
# In a backtest, a shared cached file fetched before the backtest's end date may be missing
# filings inside the window. It is refetched, but at most once per this many seconds.
BACKTEST_STALE_REFETCH_SECONDS = 86400.0
_CACHE_META_KEY = "_lumibot_cache_meta"
_NEW_YORK = ZoneInfo("America/New_York")
_SESSION_OPEN = dt_time(9, 30)
_SESSION_CLOSE = dt_time(16, 0)
# A filing dated D becomes public no earlier than D 00:00 New York (acceptance the evening
# before is dated the next day) and, without an acceptance time, is treated as public from
# D+1 09:30. Acceptance times only matter when the requested time falls in that window
# (33.5 hours, padded for daylight saving changes).
_ACCEPTANCE_WINDOW_SECONDS = 36 * 3600.0
_DEFAULT_REMOTE_CACHE = object()


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


def _timestamp(value: datetime) -> float:
    """POSIX seconds for ``value``; a naive datetime is New York market time."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=_NEW_YORK)
    return value.timestamp()


def _iso_from_timestamp(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, _NEW_YORK).isoformat()


def _next_session_open(moment: datetime) -> datetime:
    """09:30 New York on the first weekday after ``moment``'s calendar day."""
    day = moment.astimezone(_NEW_YORK).date() + timedelta(days=1)
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return datetime.combine(day, _SESSION_OPEN, _NEW_YORK)


def _available_from_filed(filed: Any) -> float | None:
    """A filing with only a ``filed`` date is public from 09:30 New York the next day.

    ``filed`` has no time of day and filings accepted after the close carry that day's date,
    so treating it as midnight of the filing day showed after-close filings at that
    morning's open. The next day's open is never earlier than the real availability.
    """
    text = str(filed or "").strip()[:10]
    if not text:
        return None
    try:
        day = date.fromisoformat(text)
    except ValueError:
        return None
    return datetime.combine(day + timedelta(days=1), _SESSION_OPEN, _NEW_YORK).timestamp()


def _available_from_acceptance(acceptance: Any) -> float | None:
    """When an accepted filing becomes usable: at acceptance during or before a weekday
    session, otherwise at the next session's open. SEC acceptance times are UTC."""
    accepted = _parse_dt(acceptance)
    if accepted is None:
        return None
    if accepted.tzinfo is None:
        accepted = accepted.replace(tzinfo=timezone.utc)
    local = accepted.astimezone(_NEW_YORK)
    if local.weekday() >= 5 or local.time() >= _SESSION_CLOSE:
        return _next_session_open(local).timestamp()
    return local.timestamp()


def _intern(value: Any) -> Any:
    return sys.intern(value) if isinstance(value, str) else value


# Row layout inside a timeline series (tuples keep 500-company universes small in memory).
_R_FILING, _R_END, _R_START, _R_VAL, _R_ACCN, _R_FORM, _R_FY, _R_FP, _R_FRAME, _R_FILED = range(10)


class _Series:
    __slots__ = ("ts", "rows")

    def __init__(self, rows: list[tuple]):
        self.rows = rows
        self.ts: list[float] = []


class _CompanyTimeline:
    """One company's facts, indexed by the moment each value became public.

    Built once per process from the companyfacts file. A statement or history request is
    then a binary search plus a scan of the visible rows, with no file read and no JSON
    parse. Only the tags LumiBot's statements use (plus any tag asked for by name) are
    kept in memory; every filing's availability is kept for all tags.
    """

    def __init__(self, cik: str, payload: dict[str, Any], tags: Iterable[str], source_url: str, fetched_at: str):
        self.cik = cik
        self.source_url = source_url
        self.fetched_at = fetched_at
        self.loaded_at = time.monotonic()
        self.series: dict[str, dict[str, _Series]] = {}
        self.loaded_tags: set[str] = set()
        self.filing_filed: dict[str, str] = {}
        self.filing_ts: dict[str, float] = {}
        self.filing_published: dict[str, str] = {}
        self.acceptance_loaded = False
        self.memo: dict[Any, Any] = {}
        self.lock = threading.RLock()
        facts = payload.get("facts") if isinstance(payload, dict) else None
        facts = facts if isinstance(facts, dict) else {}
        us_gaap = facts.get("us-gaap") if isinstance(facts.get("us-gaap"), dict) else {}
        self.us_gaap_tags = sorted(us_gaap)
        for taxonomy in facts.values():
            if not isinstance(taxonomy, dict):
                continue
            for fact in taxonomy.values():
                units = fact.get("units") if isinstance(fact, dict) else None
                for rows in units.values() if isinstance(units, dict) else ():
                    for row in rows if isinstance(rows, list) else ():
                        if isinstance(row, dict) and row.get("filed"):
                            key = self.filing_key(row)
                            if key not in self.filing_filed:
                                self.filing_filed[key] = _intern(str(row["filed"]))
        for key, filed in self.filing_filed.items():
            ts = _available_from_filed(filed)
            if ts is not None:
                self.filing_ts[key] = ts
                self.filing_published[key] = filed
        self.fallback_ts = sorted(set(self.filing_ts.values()))
        self.add_tags(us_gaap, tags)

    @staticmethod
    def filing_key(row: dict[str, Any]) -> str:
        accn = row.get("accn")
        return _intern(str(accn)) if accn else _intern(f"filed:{row.get('filed')}")

    def add_tags(self, us_gaap: dict[str, Any], tags: Iterable[str]) -> None:
        for tag in tags:
            if tag in self.loaded_tags:
                continue
            self.loaded_tags.add(tag)
            units = (us_gaap.get(tag) or {}).get("units") if isinstance(us_gaap.get(tag), dict) else None
            if not isinstance(units, dict):
                continue
            by_unit: dict[str, _Series] = {}
            for unit, rows in units.items():
                if not isinstance(rows, list):
                    continue
                kept = []
                for row in rows:
                    if not isinstance(row, dict) or not row.get("filed"):
                        continue
                    key = self.filing_key(row)
                    if key not in self.filing_ts:
                        continue
                    kept.append(
                        (
                            key,
                            _intern(row.get("end")),
                            _intern(row.get("start")),
                            row.get("val"),
                            _intern(row.get("accn")),
                            _intern(row.get("form")),
                            row.get("fy"),
                            _intern(row.get("fp")),
                            _intern(row.get("frame")),
                            self.filing_filed[key],
                        )
                    )
                if kept:
                    by_unit[_intern(unit)] = _Series(kept)
            if by_unit:
                self.series[tag] = by_unit
        self.reindex()

    def reindex(self) -> None:
        """Sort every series by availability and rebuild the global state index."""
        for by_unit in self.series.values():
            for series in by_unit.values():
                series.rows.sort(key=lambda row: self.filing_ts[row[_R_FILING]])
                series.ts = [self.filing_ts[row[_R_FILING]] for row in series.rows]
        ordered = sorted(self.filing_ts.items(), key=lambda item: item[1])
        self.state_ts = [ts for _key, ts in ordered]
        # Latest publication visible at each state (old semantics: the newest publication).
        published: list[str | None] = []
        best: datetime | None = None
        best_text: str | None = None
        for key, _ts in ordered:
            text = self.filing_published.get(key)
            parsed = _parse_dt(text)
            if parsed is not None:
                parsed = parsed if parsed.tzinfo else parsed.replace(tzinfo=_NEW_YORK)
                if best is None or parsed > best:
                    best, best_text = parsed, text
            published.append(best_text)
        self.state_published = published
        self.memo.clear()

    def needs_acceptance(self, as_ts: float) -> bool:
        if self.acceptance_loaded:
            return False
        index = bisect_right(self.fallback_ts, as_ts)
        return index < len(self.fallback_ts) and self.fallback_ts[index] - as_ts < _ACCEPTANCE_WINDOW_SECONDS

    def apply_acceptance(self, acceptance_by_accession: dict[str, str]) -> None:
        for key in list(self.filing_ts):
            accepted = acceptance_by_accession.get(key)
            ts = _available_from_acceptance(accepted) if accepted else None
            if ts is not None:
                self.filing_ts[key] = ts
                self.filing_published[key] = accepted
        self.acceptance_loaded = True
        self.reindex()

    def state_index(self, as_ts: float) -> int:
        return bisect_right(self.state_ts, as_ts)

    def published_at(self, as_ts: float) -> str | None:
        index = self.state_index(as_ts)
        if index <= 0:
            return None
        parsed = _parse_dt(self.state_published[index - 1])
        return parsed.isoformat() if parsed is not None else None

    def available_ts(self, row: dict[str, Any]) -> float | None:
        if not row.get("filed"):
            return None
        key = self.filing_key(row)
        ts = self.filing_ts.get(key)
        return ts if ts is not None else _available_from_filed(row.get("filed"))

    def visible_periods(self, tag: str, as_ts: float, unit: str | None = None):
        """Yield ``(unit, row, ts)`` for the latest public value of each period of ``tag``.

        Rows are in availability order, so a later restatement of the same period replaces
        the earlier value only once the restating filing is public.
        """
        for series_unit, series in self.series.get(tag, {}).items():
            if unit is not None and series_unit != unit:
                continue
            visible = bisect_right(series.ts, as_ts)
            latest: dict[tuple, int] = {}
            rows = series.rows
            for index in range(visible):
                row = rows[index]
                latest[(row[_R_START], row[_R_END])] = index
            for index in latest.values():
                yield series_unit, rows[index], series.ts[index]


class SECFundamentals:
    """Direct SEC EDGAR client with mandatory local caching and point-in-time helpers."""

    def __init__(
        self,
        strategy: Any | None = None,
        *,
        cache_dir: str | os.PathLike[str] | None = None,
        user_agent: str | None = None,
        min_request_interval_seconds: float = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
        cache_mode: str = "auto",
        mutable_cache_ttl_seconds: float = DEFAULT_MUTABLE_CACHE_TTL_SECONDS,
        remote_cache: Any = _DEFAULT_REMOTE_CACHE,
        backtest_end: Any | None = None,
    ) -> None:
        """
        Parameters
        ----------
        remote_cache:
            Shared cache for raw SEC files in backtests. Defaults to LumiBot's configured remote
            backtest cache (disabled unless one is configured). Files are stored under
            provider-style keys such as ``sec/companyfacts/CIK0000320193.json`` so every
            backtest machine reuses the same download. Pass ``None`` to keep files local only.
        backtest_end:
            Overrides the backtest end date used to decide whether a shared cached file is
            recent enough. Normally read from the strategy.
        """
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
        self._remote_cache = remote_cache
        self._backtest_end_override = backtest_end
        self._rate_lock = threading.Lock()
        self._next_request_at = 0.0
        self._counter_lock = threading.Lock()
        self.request_count = 0
        self._tickers: dict[str, str] | None = None
        self._tickers_loaded_at = 0.0
        self._tickers_lock = threading.Lock()
        self._timelines: dict[str, _CompanyTimeline] = {}
        self._timeline_locks: dict[str, threading.Lock] = {}
        self._timeline_locks_guard = threading.Lock()

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

    def _backtest_end(self) -> datetime | None:
        candidates = [self._backtest_end_override]
        strategy = self.strategy
        if strategy is not None:
            candidates.append(getattr(strategy, "_backtesting_end", None))
            data_source = getattr(getattr(strategy, "broker", None), "data_source", None)
            candidates.append(getattr(data_source, "datetime_end", None))
        for candidate in candidates:
            parsed = _parse_dt(candidate) if candidate is not None else None
            if parsed is not None:
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=_NEW_YORK)
        return None

    def _cache_fetched_dt(self, cache_path: Path) -> datetime | None:
        fetched = _parse_dt(self._cache_meta(cache_path).get("fetched_at"))
        if fetched is None:
            return None
        return fetched if fetched.tzinfo else fetched.replace(tzinfo=timezone.utc)

    def _cache_is_usable(self, cache_path: Path, *, mutable: bool) -> bool:
        if not cache_path.exists():
            return False
        if not mutable:
            return True
        fetched = self._cache_fetched_dt(cache_path)
        if self._resolved_cache_mode() == "backtest":
            # A copy fetched after the backtest ends holds every filing the backtest can see.
            # An older copy may miss filings inside the window: refetch it, at most once a day.
            backtest_end = self._backtest_end()
            if backtest_end is None:
                return True
            if fetched is None:
                fetched = datetime.fromtimestamp(cache_path.stat().st_mtime, timezone.utc)
            if fetched >= backtest_end:
                return True
            return (datetime.now(timezone.utc) - fetched).total_seconds() < BACKTEST_STALE_REFETCH_SECONDS
        reference = cache_path.stat().st_mtime
        if fetched is not None:
            reference = min(reference, fetched.timestamp())
        age_seconds = max(time.time() - reference, 0.0)
        return age_seconds <= self.mutable_cache_ttl_seconds

    def _remote(self) -> Any | None:
        """The shared backtest cache, or None. Only used in backtest mode."""
        if self._resolved_cache_mode() != "backtest":
            return None
        remote = self._remote_cache
        if remote is _DEFAULT_REMOTE_CACHE:
            try:
                from lumibot.tools.backtest_cache import get_backtest_cache

                remote = get_backtest_cache()
            except Exception:
                remote = None
            self._remote_cache = remote
        if remote is None or not getattr(remote, "enabled", False):
            return None
        return remote

    def _pull_remote(self, cache_path: Path, remote_relative: str | None) -> bool:
        remote = self._remote() if remote_relative else None
        if remote is None:
            return False
        try:
            return bool(remote.ensure_local_file(cache_path, remote_relative=remote_relative))
        except Exception:
            # The shared cache is an accelerator; the SEC remains the source.
            return False

    def _push_remote(self, cache_path: Path, remote_relative: str | None) -> None:
        remote = self._remote() if remote_relative else None
        if remote is None:
            return
        try:
            remote.on_local_update(cache_path, remote_relative=remote_relative)
        except Exception:
            pass

    def _read_cached_json(self, cache_path: Path, *, from_remote: bool = False) -> Any:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and _CACHE_META_KEY in payload:
            meta = payload.pop(_CACHE_META_KEY)
            # A file downloaded from the shared cache carries its SEC fetch time inside it.
            meta_path = self._cache_meta_path(cache_path)
            if isinstance(meta, dict) and meta.get("fetched_at") and (from_remote or not meta_path.exists()):
                self._cache_meta_path(cache_path).write_text(json.dumps(meta, sort_keys=True), encoding="utf-8")
        return payload

    def _write_json_cache(self, cache_path: Path, payload: Any) -> None:
        stored = payload
        if isinstance(payload, dict):
            stored = {**payload, _CACHE_META_KEY: self._cache_meta(cache_path)}
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_name(f"{cache_path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
        tmp_path.write_text(json.dumps(stored, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp_path, cache_path)

    def _get_json(
        self,
        url: str,
        cache_path: Path,
        *,
        mutable: bool = False,
        remote_relative: str | None = None,
    ) -> Any:
        if self._cache_is_usable(cache_path, mutable=mutable):
            return self._read_cached_json(cache_path)
        if self._pull_remote(cache_path, remote_relative):
            payload = self._read_cached_json(cache_path, from_remote=True)
            if self._cache_is_usable(cache_path, mutable=mutable):
                return payload
        self._rate_limit()
        headers = self._headers()
        if mutable and cache_path.exists():
            headers.update(self._revalidation_headers(cache_path))
        response = requests.get(url, headers=headers, timeout=30)
        if getattr(response, "status_code", None) == 304 and cache_path.exists():
            payload = self._read_cached_json(cache_path)
            self._record_cache_fetch(cache_path, getattr(response, "headers", None))
            self._write_json_cache(cache_path, payload)
            self._push_remote(cache_path, remote_relative)
            return payload
        response.raise_for_status()
        payload = response.json()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._record_cache_fetch(cache_path, getattr(response, "headers", None))
        self._write_json_cache(cache_path, payload)
        self._push_remote(cache_path, remote_relative)
        return payload

    def _get_text(self, url: str, cache_path: Path, *, mutable: bool = False, remote_relative: str | None = None) -> str:
        if self._cache_is_usable(cache_path, mutable=mutable):
            return cache_path.read_text(encoding="utf-8", errors="replace")
        if not mutable and self._pull_remote(cache_path, remote_relative) and cache_path.exists():
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
        if not mutable:
            self._push_remote(cache_path, remote_relative)
        return text

    def _rate_limit(self) -> None:
        """Space request starts by ``min_request_interval_seconds`` across all threads."""
        with self._rate_lock:
            now = time.monotonic()
            start = max(now, self._next_request_at)
            self._next_request_at = start + self.min_request_interval_seconds
        if start > now:
            time.sleep(start - now)
        with self._counter_lock:
            self.request_count += 1

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

    def _ticker_map(self) -> dict[str, str]:
        with self._tickers_lock:
            fresh = self._tickers is not None and (
                self._resolved_cache_mode() == "backtest"
                or time.monotonic() - self._tickers_loaded_at <= self.mutable_cache_ttl_seconds
            )
            if fresh:
                return self._tickers  # type: ignore[return-value]
            payload = self._get_json(
                SEC_COMPANY_TICKERS_URL,
                self._cache_path("company_tickers.json"),
                mutable=True,
                remote_relative="sec/company_tickers.json",
            )
            mapping: dict[str, str] = {}
            for entry in payload.values() if isinstance(payload, dict) else ():
                if not isinstance(entry, dict):
                    continue
                ticker = str(entry.get("ticker", "")).upper()
                if ticker and ticker not in mapping and entry.get("cik_str") is not None:
                    mapping[ticker] = f"{int(entry['cik_str']):010d}"
            self._tickers = mapping
            self._tickers_loaded_at = time.monotonic()
            return mapping

    def ticker_to_cik(self, symbol: str) -> str:
        symbol_upper = str(symbol).upper().strip()
        cik = self._ticker_map().get(symbol_upper)
        if cik is None:
            raise SECTickerNotFoundError(f"No SEC CIK found for ticker {symbol!r}.")
        return cik

    def _get_submissions_payload(self, symbol: str) -> dict[str, Any]:
        cik = self.ticker_to_cik(symbol)
        url = f"{SEC_DATA_BASE_URL}/submissions/CIK{cik}.json"
        cache_path = self._cache_path("submissions", f"CIK{cik}.json")
        payload = self._get_json(
            url,
            cache_path,
            mutable=True,
            remote_relative=f"sec/submissions/CIK{cik}.json",
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

    # ------------------------------------------------------------------
    # Point-in-time fact table (parsed once per process per company)
    # ------------------------------------------------------------------

    def _companyfacts_location(self, cik: str) -> tuple[str, Path, str]:
        return (
            f"{SEC_DATA_BASE_URL}/api/xbrl/companyfacts/CIK{cik}.json",
            self._cache_path("companyfacts", f"CIK{cik}.json"),
            f"sec/companyfacts/CIK{cik}.json",
        )

    def _load_companyfacts_payload(self, cik: str) -> dict[str, Any]:
        url, cache_path, remote_relative = self._companyfacts_location(cik)
        payload = self._get_json(url, cache_path, mutable=True, remote_relative=remote_relative)
        return payload if isinstance(payload, dict) else {}

    def _timeline_lock(self, cik: str) -> threading.Lock:
        with self._timeline_locks_guard:
            lock = self._timeline_locks.get(cik)
            if lock is None:
                lock = self._timeline_locks[cik] = threading.Lock()
            return lock

    def _timeline_is_fresh(self, timeline: _CompanyTimeline) -> bool:
        if self._resolved_cache_mode() == "backtest":
            return True
        return time.monotonic() - timeline.loaded_at <= self.mutable_cache_ttl_seconds

    def _timeline(self, symbol: str, extra_tags: Iterable[str] = ()) -> _CompanyTimeline:
        cik = self.ticker_to_cik(symbol)
        extra = [tag for tag in extra_tags if tag]
        timeline = self._timelines.get(cik)
        if timeline is not None and self._timeline_is_fresh(timeline) and all(t in timeline.loaded_tags for t in extra):
            return timeline
        with self._timeline_lock(cik):
            timeline = self._timelines.get(cik)
            if timeline is not None and self._timeline_is_fresh(timeline):
                missing = [tag for tag in extra if tag not in timeline.loaded_tags]
                if missing:
                    payload = self._load_companyfacts_payload(cik)
                    us_gaap = (payload.get("facts") or {}).get("us-gaap") or {}
                    with timeline.lock:
                        timeline.add_tags(us_gaap, missing)
                return timeline
            url, cache_path, _remote_relative = self._companyfacts_location(cik)
            payload = self._load_companyfacts_payload(cik)
            tags = set(_PRIORITY_COMPANY_FACT_TAGS) | set(extra)
            if timeline is not None:
                tags |= timeline.loaded_tags
            rebuilt = _CompanyTimeline(cik, payload, tags, url, self._cache_fetched_at(cache_path))
            self._timelines[cik] = rebuilt
            return rebuilt

    def _ensure_acceptance(self, symbol: str, timeline: _CompanyTimeline, as_ts: float | None, *, force: bool = False) -> None:
        """Load acceptance times only when a filing's exact time can change the answer."""
        if timeline.acceptance_loaded:
            return
        if not force and (as_ts is None or not timeline.needs_acceptance(as_ts)):
            return
        with self._timeline_lock(timeline.cik):
            if timeline.acceptance_loaded:
                return
            acceptance: dict[str, str] = {}
            try:
                recent = self._get_submissions_payload(symbol).get("filings", {}).get("recent", {})
                accessions = recent.get("accessionNumber") or []
                accepted = recent.get("acceptanceDateTime") or []
                for index, accession in enumerate(accessions):
                    if index < len(accepted) and accepted[index]:
                        acceptance[str(accession)] = str(accepted[index])
            except Exception:
                # Without acceptance times every filing stays at the conservative
                # "next day" availability. Never retried within this process.
                acceptance = {}
            with timeline.lock:
                timeline.apply_acceptance(acceptance)

    def _point_in_time(self, symbol: str, as_of: Any | None, extra_tags: Iterable[str] = ()):
        as_of_dt = self._resolve_as_of(as_of)
        as_ts = _timestamp(as_of_dt)
        timeline = self._timeline(symbol, extra_tags)
        self._ensure_acceptance(symbol, timeline, as_ts)
        return as_of_dt, as_ts, timeline

    def _provenance(self, timeline: _CompanyTimeline) -> dict[str, Any]:
        return {
            "id": f"sec-companyfacts-{timeline.cik}",
            "source": "sec_edgar_companyfacts",
            "source_url": timeline.source_url,
            "fetched_at": timeline.fetched_at,
        }

    @staticmethod
    def _fact_output(tag: str | None, unit: str, row: tuple, ts: float) -> dict[str, Any]:
        output = {
            "value": row[_R_VAL],
            "unit": unit,
            "filed": row[_R_FILED],
            "form": row[_R_FORM],
            "fy": row[_R_FY],
            "fp": row[_R_FP],
            "frame": row[_R_FRAME],
            "start": row[_R_START],
            "end": row[_R_END],
            "accession_number": row[_R_ACCN],
            "available_at": _iso_from_timestamp(ts),
        }
        if tag is not None:
            output = {"tag": tag, **output}
        return output

    def preload(
        self,
        symbols: Iterable[str],
        *,
        max_workers: int = 8,
        include_submissions: bool = False,
    ) -> dict[str, Any]:
        """Warm the fact table for a whole universe in one call.

        Downloads (or reads from the shared or local cache) every company's companyfacts file
        in parallel, with request starts spaced to stay under SEC's 10 requests per second,
        and builds each point-in-time table. Later statement calls for these symbols are
        in-memory lookups. Symbols the SEC does not cover are reported, not raised.

        ``include_submissions`` also loads each company's filing acceptance times up front.
        Without it they are loaded on demand, only for a company and date where an exact
        filing time can change the answer.
        """
        started = time.perf_counter()
        requests_before = self.request_count
        unique = list(dict.fromkeys(str(symbol).upper().strip() for symbol in symbols if str(symbol or "").strip()))
        self._ticker_map()

        def load(symbol: str) -> tuple[str, str, str | None]:
            try:
                timeline = self._timeline(symbol)
            except SECTickerNotFoundError:
                return symbol, "missing", "no_sec_cik"
            except requests.HTTPError as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status == 404:
                    return symbol, "missing", "no_companyfacts"
                return symbol, "error", f"{type(exc).__name__}: {exc}"
            except Exception as exc:
                return symbol, "error", f"{type(exc).__name__}: {exc}"
            if not timeline.series:
                return symbol, "missing", "no_us_gaap_facts"
            if include_submissions:
                self._ensure_acceptance(symbol, timeline, None, force=True)
            return symbol, "loaded", None

        results: list[tuple[str, str, str | None]] = []
        if unique:
            workers = max(1, min(int(max_workers), len(unique)))
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sec-preload") as pool:
                results = list(pool.map(load, unique))
        return {
            "loaded": [symbol for symbol, status, _ in results if status == "loaded"],
            "missing": [symbol for symbol, status, _ in results if status == "missing"],
            "errors": {symbol: reason for symbol, status, reason in results if status == "error"},
            "reasons": {symbol: reason for symbol, status, reason in results if status == "missing"},
            "sec_requests": self.request_count - requests_before,
            "seconds": round(time.perf_counter() - started, 3),
        }

    def get_fact_history(
        self,
        symbol: str,
        tags: str | Iterable[str],
        *,
        as_of: Any | None = None,
        unit: str | None = None,
    ) -> dict[str, Any]:
        """Every reported period of a us-gaap fact, with the value known at ``as_of``.

        Each period (start, end) appears once, with the value from the latest filing public at
        ``as_of``. A restatement filed later never shows before its filing is public, and an
        older year amended later never hides newer periods. ``tags`` may be one tag or a list
        in priority order; the first tag that reports a period supplies its value. Values are
        sorted by period end, oldest first, and include annual, quarterly and year-to-date
        periods exactly as filed (use ``start``, ``end``, ``fp`` and ``form`` to pick).
        """
        tag_list = [tags] if isinstance(tags, str) else [str(tag) for tag in tags]
        as_of_dt, as_ts, timeline = self._point_in_time(symbol, as_of, tag_list)
        chosen: dict[tuple, tuple[int, float, str, str, tuple]] = {}
        with timeline.lock:
            for priority, tag in enumerate(tag_list):
                for series_unit, row, ts in timeline.visible_periods(tag, as_ts, unit):
                    key = (row[_R_START], row[_R_END], series_unit)
                    if key not in chosen or chosen[key][0] > priority:
                        chosen[key] = (priority, ts, tag, series_unit, row)
        values = [
            self._fact_output(tag, series_unit, row, ts)
            for _priority, ts, tag, series_unit, row in chosen.values()
        ]
        values.sort(key=lambda item: (str(item.get("end") or ""), str(item.get("start") or "")))
        return {
            "symbol": str(symbol).upper(),
            "cik": timeline.cik,
            "tags": tag_list,
            "as_of": as_of_dt.isoformat(),
            **self._provenance(timeline),
            "published_at": timeline.published_at(as_ts),
            "values": values,
        }

    def get_company_facts(
        self,
        symbol: str,
        *,
        as_of: Any | None = None,
        raw: bool = False,
        max_facts: int | None = _DEFAULT_COMPANY_FACTS_LIMIT,
    ) -> dict[str, Any]:
        as_of_dt, as_ts, timeline = self._point_in_time(symbol, as_of)
        provenance = self._provenance(timeline)
        if raw:
            payload = self._load_companyfacts_payload(timeline.cik)
            filtered = self._filter_company_facts_visible(payload, timeline, as_ts)
            filtered["as_of"] = as_of_dt.isoformat()
            filtered.update(provenance)
            filtered["published_at"] = self._latest_company_fact_publication(filtered)
            return filtered
        limit = None if max_facts is None else max(int(max_facts), 1)
        memo_key = ("compact", limit, timeline.state_index(as_ts))
        with timeline.lock:
            cached = timeline.memo.get(memo_key)
        if cached is None:
            cached = self._compact_facts(timeline, as_ts, limit)
            with timeline.lock:
                timeline.memo[memo_key] = cached
        facts, truncated = cached
        return {
            "symbol": str(symbol).upper(),
            "cik": timeline.cik,
            "as_of": as_of_dt.isoformat(),
            **provenance,
            "facts": {tag: dict(value) for tag, value in facts.items()},
            "fact_count": len(facts),
            "truncated": truncated,
            "max_facts": max_facts,
            "published_at": timeline.published_at(as_ts),
        }

    def _compact_facts(self, timeline: _CompanyTimeline, as_ts: float, limit: int | None):
        available = set(timeline.us_gaap_tags)
        ordered = [tag for tag in _PRIORITY_COMPANY_FACT_TAGS if tag in available]
        seen = set(ordered)
        ordered.extend(tag for tag in timeline.us_gaap_tags if tag not in seen)
        facts: dict[str, Any] = {}
        payload_us_gaap: dict[str, Any] | None = None
        truncated = False
        for tag in ordered:
            if limit is not None and len(facts) >= limit:
                truncated = True
                break
            if tag in timeline.loaded_tags:
                latest = self._latest_visible(timeline, tag, as_ts)
            else:
                # Tags outside the statement maps are not held in memory; read them from the
                # cached file once per call (results are memoised per filing state).
                if payload_us_gaap is None:
                    payload = self._load_companyfacts_payload(timeline.cik)
                    payload_us_gaap = (payload.get("facts") or {}).get("us-gaap") or {}
                latest = self._latest_visible_from_payload(timeline, payload_us_gaap.get(tag) or {}, as_ts)
            if latest is not None:
                facts[tag] = latest
        return facts, truncated

    @staticmethod
    def _latest_key(end: Any, ts: float) -> tuple:
        # Newest period first; for the same period the most recently public value.
        return (str(end or ""), ts)

    def _latest_visible(self, timeline: _CompanyTimeline, tag: str, as_ts: float) -> dict[str, Any] | None:
        best = None
        for series_unit, row, ts in timeline.visible_periods(tag, as_ts):
            key = self._latest_key(row[_R_END], ts)
            if best is None or key > best[0]:
                best = (key, series_unit, row, ts)
        if best is None:
            return None
        return self._fact_output(None, best[1], best[2], best[3])

    def _latest_visible_from_payload(
        self, timeline: _CompanyTimeline, tag_payload: dict[str, Any], as_ts: float
    ) -> dict[str, Any] | None:
        best = None
        units = tag_payload.get("units") if isinstance(tag_payload, dict) else None
        for unit, rows in units.items() if isinstance(units, dict) else ():
            for row in rows if isinstance(rows, list) else ():
                if not isinstance(row, dict):
                    continue
                ts = timeline.available_ts(row)
                if ts is None or ts > as_ts:
                    continue
                key = self._latest_key(row.get("end"), ts)
                if best is None or key > best[0]:
                    best = (key, unit, row, ts)
        if best is None:
            return None
        _key, unit, row, ts = best
        return {
            "value": row.get("val"),
            "unit": unit,
            "filed": row.get("filed"),
            "form": row.get("form"),
            "fy": row.get("fy"),
            "fp": row.get("fp"),
            "frame": row.get("frame"),
            "start": row.get("start"),
            "end": row.get("end"),
            "accession_number": row.get("accn"),
            "available_at": _iso_from_timestamp(ts),
        }

    def _filter_company_facts_visible(
        self, payload: dict[str, Any], timeline: _CompanyTimeline, as_ts: float
    ) -> dict[str, Any]:
        """Raw companyfacts with only the rows public at ``as_ts`` (no deep copy)."""
        filtered = {key: value for key, value in payload.items() if key != "facts"}
        facts_out: dict[str, Any] = {}
        facts = payload.get("facts")
        for taxonomy_name, taxonomy in facts.items() if isinstance(facts, dict) else ():
            if not isinstance(taxonomy, dict):
                facts_out[taxonomy_name] = taxonomy
                continue
            taxonomy_out: dict[str, Any] = {}
            for tag, fact in taxonomy.items():
                if not isinstance(fact, dict):
                    taxonomy_out[tag] = fact
                    continue
                units = fact.get("units")
                fact_out = {key: value for key, value in fact.items() if key != "units"}
                units_out: dict[str, Any] = {}
                for unit, rows in units.items() if isinstance(units, dict) else ():
                    if not isinstance(rows, list):
                        units_out[unit] = rows
                        continue
                    visible = []
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        ts = timeline.available_ts(row)
                        if ts is None or ts <= as_ts:
                            visible.append(row)
                    units_out[unit] = visible
                fact_out["units"] = units_out
                taxonomy_out[tag] = fact_out
            facts_out[taxonomy_name] = taxonomy_out
        filtered["facts"] = facts_out
        return filtered

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
        if raw:
            return self.get_company_facts(symbol, as_of=as_of, raw=True)
        as_of_dt, as_ts, timeline = self._point_in_time(symbol, as_of)
        # The answer only changes when a new filing becomes public, so it is computed once per
        # filing state and then reused for every date until the next filing.
        memo_key = (statement, timeline.state_index(as_ts))
        with timeline.lock:
            cached = timeline.memo.get(memo_key)
        if cached is None:
            with timeline.lock:
                cached = self._compute_statement(timeline, tag_map, as_ts)
                timeline.memo[memo_key] = cached
        values, anchor, warnings = cached
        return {
            "symbol": str(symbol).upper(),
            "cik": timeline.cik,
            "statement": statement,
            "as_of": as_of_dt.isoformat(),
            **self._provenance(timeline),
            "published_at": timeline.published_at(as_ts),
            "values": {field: dict(value) for field, value in values.items()},
            "anchor": dict(anchor) if anchor else None,
            "warnings": copy.deepcopy(warnings),
        }

    def _compute_statement(self, timeline: _CompanyTimeline, tag_map: dict[str, list[str]], as_ts: float):
        field_candidates: dict[str, list[dict[str, Any]]] = {}
        for field, tags in tag_map.items():
            candidates = []
            for tag in tags:
                for series_unit, row, ts in timeline.visible_periods(tag, as_ts):
                    candidate = self._fact_output(tag, series_unit, row, ts)
                    candidate["_ts"] = ts
                    candidates.append(candidate)
            if candidates:
                candidates.sort(key=lambda row, tags=tags: self._statement_candidate_sort_key(row, tags), reverse=True)
                field_candidates[field] = candidates

        anchor = self._statement_anchor(field_candidates)
        values: dict[str, Any] = {}
        warnings: list[dict[str, Any]] = []
        keys = ("tag", "filed", "form", "fy", "fp", "start", "end", "accession_number")
        for field, candidates in field_candidates.items():
            chosen = self._best_matching_statement_fact(candidates, anchor)
            if chosen is not None:
                values[field] = {key: value for key, value in chosen.items() if key != "_ts"}
                continue
            latest = candidates[0]
            warnings.append(
                {
                    "field": field,
                    "message": (
                        "Latest fact did not match the statement period anchor and was omitted "
                        "to avoid mixing facts from different SEC filings or periods."
                    ),
                    "latest_candidate": {key: latest.get(key) for key in keys},
                    "anchor": {key: anchor.get(key) if anchor else None for key in keys},
                }
            )
        anchor_out = {key: anchor.get(key) for key in keys} if anchor else None
        return values, anchor_out, warnings

    def _statement_candidate_sort_key(self, row: dict[str, Any], tags: list[str] | None = None) -> tuple[Any, ...]:
        # Newest period first. Sorting by filing date first let a later amendment of an older
        # year (or a comparative restated in a newer filing) become the "current" statement.
        # For the same period, the most recently public value wins (restatements apply only
        # once their filing is public; see visible_periods).
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
            str(row.get("end") or ""),
            float(row.get("_ts") or 0.0),
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
        candidate_end = str(candidate.get("end") or "").strip()
        anchor_end = str(anchor.get("end") or "").strip()
        if candidate_end and anchor_end and candidate_end != anchor_end:
            # A comparative from the same filing is a different period, never this statement.
            return False
        candidate_accn = str(candidate.get("accession_number") or "").strip()
        anchor_accn = str(anchor.get("accession_number") or "").strip()
        if candidate_accn and anchor_accn and candidate_accn == anchor_accn:
            return True
        if not candidate_end or not anchor_end:
            return False
        for key in ("start", "fy", "fp", "form"):
            candidate_value = str(candidate.get(key) or "").strip()
            anchor_value = str(anchor.get(key) or "").strip()
            if candidate_value and anchor_value and candidate_value != anchor_value:
                return False
        return True

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
        safe_document = re.sub(r"[^A-Za-z0-9_.=-]+", "_", str(primary_document)).strip("_")
        safe_accession = re.sub(r"[^A-Za-z0-9_.=-]+", "_", str(accession_number)).strip("_")
        raw = self._get_text(url, cache_path, remote_relative=f"sec/filings/{cik}/{safe_accession}/{safe_document}")
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
