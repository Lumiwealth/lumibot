import hashlib
import json
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests

FXMACRODATA_API_BASE_URL = "https://api.fxmacrodata.com/v1"


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
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    else:
        text = str(value or "").strip()
        if not text:
            return None
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

    def _require_key_for_currency(self, currency: str) -> None:
        if currency.lower() != "usd" and not self.api_key:
            raise ValueError(
                "FXMD_API_KEY or FXMACRODATA_API_KEY is required for non-USD FXMacroData requests. "
                "USD announcement data can be fetched without credentials."
            )

    def _get_json(self, path: str, params: dict[str, Any], cache_path: Path) -> dict[str, Any]:
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))
        self._rate_limit()
        response = requests.get(
            f"{self.base_url}{path}",
            params={key: value for key, value in params.items() if value is not None},
            headers=self._headers() or None,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return payload

    def list_indicators(self, category: str | None = None) -> dict[str, Any]:
        rows = []
        wanted_category = str(category).strip().lower() if category else None
        for indicator, metadata in CURATED_FXMACRODATA_INDICATORS.items():
            if wanted_category and metadata["category"] != wanted_category:
                continue
            rows.append({"indicator": indicator, **metadata})
        return {
            "source": "fxmacrodata",
            "indicators": rows,
            "categories": sorted({metadata["category"] for metadata in CURATED_FXMACRODATA_INDICATORS.values()}),
            "notes": (
                "These are common FXMacroData announcement indicators. USD announcement data is public; "
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
        if limit is not None:
            params["limit"] = max(int(limit), 1)
        cache_key = json.dumps(
            {
                "authenticated": bool(self.api_key),
                "currency": currency_code,
                "indicator": indicator_slug,
                "params": {key: value for key, value in params.items() if value is not None},
            },
            sort_keys=True,
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
        )
        observations = self._normalize_observations(payload, currency_code, indicator_slug, as_of_dt)
        if limit is not None:
            observations = observations[-max(int(limit), 1):]
        return {
            "source": "fxmacrodata_api",
            "currency": currency_code,
            "indicator": indicator_slug,
            "as_of": as_of_dt.isoformat(),
            "point_in_time_safe": True,
            "observations": observations,
        }

    def get_latest(self, currency: str, indicator: str, *, as_of: Any | None = None) -> dict[str, Any]:
        payload = self.get_series(currency, indicator, as_of=as_of, limit=20)
        observations = payload.get("observations", [])
        latest = observations[-1] if observations else None
        return {**payload, "latest": latest, "observations": observations[-10:]}

    def get_snapshot(
        self,
        currency: str,
        indicators: list[str] | tuple[str, ...] | str,
        *,
        as_of: Any | None = None,
    ) -> dict[str, Any]:
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
            except Exception as exc:
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
    ) -> list[dict[str, Any]]:
        rows = payload.get("data")
        if not isinstance(rows, list):
            rows = payload.get("observations")
        if not isinstance(rows, list):
            rows = payload.get("results")
        if not isinstance(rows, list):
            rows = []

        observations = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            announcement_dt = _parse_dt(
                _first_present(
                    row,
                    ("announcement_datetime", "announcement_datetime_utc", "release_datetime", "published_at"),
                )
            )
            row_date = _date_text(_first_present(row, ("date", "release_date", "observation_date", "period")))
            comparison_dt = announcement_dt or _parse_dt(row_date)
            if comparison_dt is not None and comparison_dt > as_of_dt:
                continue
            value = _safe_float(_first_present(row, ("value", "val", "actual", "latest_value")))
            normalized = {
                "date": row_date,
                "value": value,
                "announcement_datetime": announcement_dt.isoformat() if announcement_dt is not None else None,
                "currency": str(row.get("currency") or currency).lower(),
                "indicator": str(row.get("indicator") or indicator).lower(),
            }
            for key in ("forecast", "previous", "revision", "unit", "source", "event_name"):
                if key in row:
                    normalized[key] = row.get(key)
            observations.append(normalized)

        observations.sort(key=lambda row: (row.get("announcement_datetime") or row.get("date") or ""))
        return observations
