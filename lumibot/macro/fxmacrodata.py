import hashlib
import json
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests

FXMACRODATA_API_BASE_URL = "https://api.fxmacrodata.com/v1"
FXMACRODATA_MAX_PAGE_SIZE = 100
FXMACRODATA_MAX_PAGES = 500
FXMACRODATA_MAX_PAGINATION_RESTARTS = 2
FXMACRODATA_LATEST_FIRST_PAGE_SIZE = 20


CURATED_FXMACRODATA_INDICATORS: dict[str, dict[str, str]] = {
    "policy_rate": {"category": "rates", "name": "Policy Rate"},
    "inflation": {"category": "inflation", "name": "Inflation"},
    "unemployment": {"category": "labor", "name": "Unemployment Rate"},
    "non_farm_payrolls": {"category": "labor", "name": "US Non-Farm Payrolls"},
    "gdp_growth": {"category": "growth", "name": "GDP Growth"},
    "retail_sales": {"category": "demand", "name": "Retail Sales"},
    "trade_balance": {"category": "trade", "name": "Trade Balance"},
    "current_account": {"category": "trade", "name": "Current Account"},
    "business_confidence": {"category": "sentiment", "name": "Business Confidence"},
    "consumer_confidence": {"category": "sentiment", "name": "Consumer Confidence"},
}


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    elif isinstance(value, (int, float)):
        # The API sends announcement_datetime as Unix epoch seconds.
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    else:
        text = str(value or "").strip()
        if not text:
            return None
        if re.fullmatch(r"\d{9,11}(\.\d+)?", text):
            return _parse_dt(float(text))
        text = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            try:
                parsed = datetime.strptime(text[:10], "%Y-%m-%d")
            except ValueError:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_of_datetime(value: Any) -> datetime:
    parsed = _parse_dt(value)
    if parsed is not None:
        return parsed
    return datetime.now(timezone.utc)


def _date_text(value: Any | None) -> str | None:
    parsed = _parse_dt(value)
    if parsed is None:
        return None
    return parsed.date().isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "."}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _publication_time_summary(
    observations: list[dict[str, Any]],
    dropped: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Count how each returned row was dated.

    Rows without an announcement datetime are gated on their period date only
    (``gated_on == "period_date"``), which usually precedes the actual release,
    so such results are approximate. Backtests drop those rows instead.
    ``publication_time_status`` is passed through from the API as-is; only
    ``confirmed`` is evidence of when a value became public.
    """
    dropped = dropped or {}
    status_counts: dict[str, int] = {}
    for row in observations:
        status = row.get("publication_time_status") or "not_reported"
        status_counts[status] = status_counts.get(status, 0) + 1
    with_announcement = sum(1 for row in observations if row.get("announcement_datetime"))
    without_announcement = len(observations) - with_announcement
    return {
        "rows_with_announcement_datetime": with_announcement,
        "rows_without_announcement_datetime": without_announcement,
        "rows_dropped_undated": dropped.get("undated", 0),
        "rows_dropped_without_announcement_datetime": dropped.get("no_announcement", 0),
        "approximate": without_announcement > 0,
        "publication_time_status_counts": dict(sorted(status_counts.items())),
    }


def _first_present(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return None


class FXMacroData:
    """FXMacroData macro-release client with local caching and strategy-date gating.

    USD data can be fetched without credentials. Non-USD and paid endpoint
    access require ``FXMD_API_KEY`` or ``FXMACRODATA_API_KEY``. Credentials are
    sent in the ``X-API-Key`` header so keys do not appear in request URLs.
    """

    def __init__(
        self,
        strategy: Any | None = None,
        *,
        cache_dir: str | os.PathLike[str] | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
        min_request_interval_seconds: float = 0.2,
    ) -> None:
        self.strategy = strategy
        self.cache_dir = Path(
            cache_dir
            or os.environ.get("LUMIBOT_FXMACRODATA_CACHE_DIR")
            or Path.home() / ".lumibot" / "cache" / "fxmacrodata"
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.api_key = (
            api_key
            or os.environ.get("FXMD_API_KEY")
            or os.environ.get("FXMACRODATA_API_KEY")
        )
        self.base_url = (
            base_url
            or os.environ.get("LUMIBOT_FXMACRODATA_API_BASE_URL")
            or FXMACRODATA_API_BASE_URL
        ).rstrip("/")
        self.min_request_interval_seconds = max(float(min_request_interval_seconds), 0.0)
        self._last_request_at = 0.0

    def _strategy_as_of(self) -> datetime:
        if self.strategy is not None and hasattr(self.strategy, "get_datetime"):
            try:
                return _as_of_datetime(self.strategy.get_datetime())
            except Exception:
                pass
        return datetime.now(timezone.utc)

    def _effective_as_of_datetime(self, as_of: Any | None) -> datetime:
        return _as_of_datetime(as_of) if as_of is not None else self._strategy_as_of()

    def _cache_path(self, *parts: str) -> Path:
        safe = [re.sub(r"[^A-Za-z0-9_.=-]+", "_", str(part)).strip("_") for part in parts]
        return self.cache_dir.joinpath(*safe)

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.min_request_interval_seconds:
            time.sleep(self.min_request_interval_seconds - elapsed)
        self._last_request_at = time.monotonic()

    def _headers(self) -> dict[str, str]:
        if self.api_key:
            return {"X-API-Key": self.api_key}
        return {}

    def _use_cache(self) -> bool:
        is_backtesting = getattr(self.strategy, "is_backtesting", False)
        if callable(is_backtesting):
            is_backtesting = is_backtesting()
        return bool(is_backtesting)

    def _write_cache(self, cache_path: Path, payload: dict[str, Any]) -> None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = cache_path.with_name(f"{cache_path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        try:
            temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, cache_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def _require_key_for_currency(self, currency: str) -> None:
        if currency.lower() != "usd" and not self.api_key:
            raise ValueError(
                "FXMD_API_KEY or FXMACRODATA_API_KEY is required for non-USD FXMacroData requests. "
                "USD announcement data can be fetched without credentials."
            )

    def _get_json(
        self,
        path: str,
        params: dict[str, Any],
        cache_path: Path,
        *,
        max_rows: int | None = None,
        first_page_size: int | None = None,
        until: Callable[[list[Any]], bool] | None = None,
    ) -> dict[str, Any]:
        use_cache = self._use_cache()
        if use_cache and cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            # An entry written while paging ``until`` a condition held may have
            # stopped early for a different strategy time; reuse it only if it
            # satisfies this call or already holds the whole window.
            if (
                until is None
                or cached.get("pages_exhausted", True)
                or until(cached.get("data") or [])
            ):
                return cached
        payload = self._get_all_pages(
            path,
            params,
            max_rows=max_rows,
            first_page_size=first_page_size,
            until=until,
        )
        if use_cache:
            self._write_cache(cache_path, payload)
        return payload

    def _request_page(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any] | None]:
        self._rate_limit()
        response = requests.get(
            f"{self.base_url}{path}",
            params={key: value for key, value in params.items() if value is not None},
            headers=self._headers() or None,
            timeout=30,
        )
        status_code = getattr(response, "status_code", 200)
        if status_code == 409:
            return status_code, None
        response.raise_for_status()
        return status_code, response.json()

    def _get_all_pages(
        self,
        path: str,
        params: dict[str, Any],
        *,
        max_rows: int | None = None,
        first_page_size: int | None = None,
        until: Callable[[list[Any]], bool] | None = None,
    ) -> dict[str, Any]:
        """Follow the API's offset pagination and return one combined payload.

        Rows come back most recent first. Pages after the first are pinned to
        the first page's ``dataset_version``; if the dataset changes mid-way
        (HTTP 409), pagination restarts from the first page.

        ``max_rows`` stops after that many rows. ``until`` stops as soon as it
        returns true for the rows fetched so far; ``first_page_size`` then sets
        the first request's limit and later pages use the API maximum. With
        ``until``, the combined payload records ``pages_exhausted``.
        """
        for _attempt in range(FXMACRODATA_MAX_PAGINATION_RESTARTS + 1):
            first_payload: dict[str, Any] | None = None
            rows: list[Any] = []
            dataset_version = None
            offset = 0
            conflict = False
            exhausted = True
            for _page in range(FXMACRODATA_MAX_PAGES):
                page_size = FXMACRODATA_MAX_PAGE_SIZE
                if max_rows is not None:
                    page_size = max(min(page_size, max_rows - len(rows)), 1)
                elif first_page_size is not None and first_payload is None:
                    page_size = max(min(page_size, first_page_size), 1)
                page_params = {
                    **params,
                    "limit": page_size,
                    "offset": offset or None,
                    "dataset_version": dataset_version,
                }
                status_code, payload = self._request_page(path, page_params)
                if status_code == 409 or payload is None:
                    conflict = True
                    break
                if first_payload is None:
                    first_payload = payload
                    dataset_version = payload.get("dataset_version")
                page_rows = payload.get("data")
                if not isinstance(page_rows, list):
                    # Not a paginated announcement payload; return it unchanged.
                    return payload
                rows.extend(page_rows)
                pagination = payload.get("pagination")
                if not isinstance(pagination, dict) or not pagination.get("has_more") or not page_rows:
                    break
                if max_rows is not None and len(rows) >= max_rows:
                    break
                if until is not None and until(rows):
                    exhausted = False
                    break
                next_offset = pagination.get("next_offset")
                if next_offset is None:
                    next_offset = offset + len(page_rows)
                if int(next_offset) <= offset:
                    break
                offset = int(next_offset)
            if conflict:
                continue
            combined = dict(first_payload or {})
            combined["data"] = rows if max_rows is None else rows[:max_rows]
            combined.pop("pagination", None)
            if until is not None:
                combined["pages_exhausted"] = exhausted
            return combined
        raise RuntimeError(
            f"FXMacroData dataset for {path} kept changing during pagination; retry the request."
        )

    def list_indicators(self, category: str | None = None) -> dict[str, Any]:
        """Return curated FXMacroData indicators, optionally filtered by category."""
        rows = []
        wanted_category = str(category).strip().lower() if category else None
        for indicator, metadata in CURATED_FXMACRODATA_INDICATORS.items():
            if wanted_category and metadata["category"] != wanted_category:
                continue
            rows.append({"indicator": indicator, **metadata})
        categories = sorted(
            {metadata["category"] for metadata in CURATED_FXMACRODATA_INDICATORS.values()}
        )
        return {
            "source": "fxmacrodata",
            "indicators": rows,
            "categories": categories,
            "notes": (
                "These are common FXMacroData announcement indicators. "
                "USD announcement data is public; "
                "set FXMD_API_KEY or FXMACRODATA_API_KEY for non-USD and paid endpoint access."
            ),
        }

    def get_series(
        self,
        currency: str,
        indicator: str,
        *,
        start: Any | None = None,
        end: Any | None = None,
        as_of: Any | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Fetch an FXMacroData announcement series gated to ``as_of``.

        Rows are filtered by ``announcement_datetime`` when the API supplies
        one. Outside backtests a row without one is gated on its period date
        instead and marked ``gated_on == "period_date"``; that is approximate,
        because the period date usually precedes the release. Backtests drop
        such rows so they cannot reach a simulation before publication. Rows
        with no parseable date are always dropped. ``publication_time`` counts
        each case; check each row's ``publication_time_status`` before treating
        a timestamp as proof of when the value became public.

        The full ``start``..``end`` window is fetched page by page; ``limit``
        keeps only the most recent rows.
        """
        return self._series(
            currency, indicator, start=start, end=end, as_of=as_of, limit=limit
        )

    def _series(
        self,
        currency: str,
        indicator: str,
        *,
        start: Any | None = None,
        end: Any | None = None,
        as_of: Any | None = None,
        limit: int | None = None,
        latest_only: bool = False,
    ) -> dict[str, Any]:
        currency_code = str(currency or "").strip().lower()
        indicator_slug = str(indicator or "").strip().lower()
        if not currency_code:
            raise ValueError("currency is required.")
        if not indicator_slug:
            raise ValueError("indicator is required.")
        self._require_key_for_currency(currency_code)

        as_of_dt = self._effective_as_of_datetime(as_of)
        start_text = _date_text(start)
        end_text = _date_text(end)
        if end_text is None or date.fromisoformat(end_text) > as_of_dt.date():
            end_text = as_of_dt.date().isoformat()

        params: dict[str, Any] = {"start_date": start_text, "end_date": end_text}
        max_rows = max(int(limit), 1) if limit is not None and not latest_only else None
        cache_key = json.dumps(
            {
                "authenticated": bool(self.api_key),
                "currency": currency_code,
                "indicator": indicator_slug,
                **({"mode": "latest"} if latest_only else {}),
                "params": {
                    key: value
                    for key, value in {**params, "limit": max_rows}.items()
                    if value is not None
                },
            },
            sort_keys=True,
        )
        def has_eligible_row(rows: list[Any]) -> bool:
            return bool(
                self._normalize_observations(
                    {"data": rows}, currency_code, indicator_slug, as_of_dt
                )[0]
            )

        payload = self._get_json(
            f"/announcements/{currency_code}/{indicator_slug}",
            params,
            self._cache_path(
                "api",
                currency_code,
                indicator_slug,
                f"{hashlib.sha256(cache_key.encode()).hexdigest()}.json",
            ),
            max_rows=max_rows,
            first_page_size=FXMACRODATA_LATEST_FIRST_PAGE_SIZE if latest_only else None,
            until=has_eligible_row if latest_only else None,
        )
        observations, dropped = self._normalize_observations(
            payload,
            currency_code,
            indicator_slug,
            as_of_dt,
        )
        if limit is not None:
            observations = observations[-max(int(limit), 1):]
        return {
            "source": "fxmacrodata_api",
            "currency": currency_code,
            "indicator": indicator_slug,
            "as_of": as_of_dt.isoformat(),
            "publication_time": _publication_time_summary(observations, dropped),
            "observations": observations,
        }

    def get_latest(
        self,
        currency: str,
        indicator: str,
        *,
        as_of: Any | None = None,
    ) -> dict[str, Any]:
        """Return the latest FXMacroData observation for an indicator.

        The newest rows can all still be unpublished at ``as_of``, so this
        pages back until an eligible observation is found or the data runs
        out. Gating follows :meth:`get_series`.
        """
        payload = self._series(
            currency, indicator, as_of=as_of, limit=10, latest_only=True
        )
        observations = payload.get("observations", [])
        return {
            **payload,
            "latest": observations[-1] if observations else None,
        }

    def get_snapshot(
        self,
        currency: str,
        indicators: list[str] | tuple[str, ...] | str,
        *,
        as_of: Any | None = None,
    ) -> dict[str, Any]:
        """Return latest values for several FXMacroData indicators."""
        if isinstance(indicators, str):
            requested = [part.strip() for part in indicators.split(",") if part.strip()]
        else:
            requested = [str(part).strip() for part in indicators if str(part).strip()]
        as_of_dt = self._effective_as_of_datetime(as_of)
        values = {}
        errors = {}
        for indicator in requested:
            key = indicator.lower()
            try:
                values[key] = self.get_latest(currency, key, as_of=as_of_dt)["latest"]
            except (RuntimeError, ValueError, requests.RequestException, OSError) as exc:
                errors[key] = str(exc)
        return {
            "source": "fxmacrodata",
            "currency": str(currency or "").strip().lower(),
            "as_of": as_of_dt.isoformat(),
            "values": values,
            "errors": errors,
        }

    def _normalize_observations(
        self,
        payload: dict[str, Any],
        currency: str,
        indicator: str,
        as_of_dt: datetime,
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        rows = payload.get("data")
        if not isinstance(rows, list):
            rows = payload.get("observations")
        if not isinstance(rows, list):
            rows = payload.get("results")
        if not isinstance(rows, list):
            rows = []

        observations = []
        dropped = {"undated": 0, "no_announcement": 0}
        # A period date usually precedes the release, so gating on it is not
        # point-in-time safe: in a backtest such a row could be seen early.
        backtesting = self._use_cache()
        for row in rows:
            if not isinstance(row, dict):
                continue
            announcement_dt = _parse_dt(
                _first_present(
                    row,
                    (
                        "announcement_datetime",
                        "announcement_datetime_utc",
                        "release_datetime",
                        "published_at",
                    ),
                )
            )
            row_date = _date_text(
                _first_present(row, ("date", "release_date", "observation_date", "period"))
            )
            if announcement_dt is None and backtesting:
                dropped["no_announcement"] += 1
                continue
            comparison_dt = announcement_dt or _parse_dt(row_date)
            if comparison_dt is None:
                dropped["undated"] += 1
                continue
            if comparison_dt > as_of_dt:
                continue
            value = _safe_float(_first_present(row, ("value", "val", "actual", "latest_value")))
            normalized = {
                "date": row_date,
                "value": value,
                "announcement_datetime": (
                    announcement_dt.isoformat() if announcement_dt is not None else None
                ),
                "currency": str(row.get("currency") or currency).lower(),
                "indicator": str(row.get("indicator") or indicator).lower(),
                "gated_on": (
                    "announcement_datetime" if announcement_dt is not None else "period_date"
                ),
            }
            for key in (
                "publication_time_status",
                "publication_time_precision",
                "forecast",
                "previous",
                "revision",
                "unit",
                "source",
                "event_name",
            ):
                if key in row:
                    normalized[key] = row.get(key)
            observations.append(normalized)

        observations.sort(
            key=lambda row: (row.get("announcement_datetime") or row.get("date") or "")
        )
        return observations, dropped
