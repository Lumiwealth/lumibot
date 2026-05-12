import hashlib
import json
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests


FRED_API_BASE_URL = "https://api.stlouisfed.org/fred"


CURATED_FRED_SERIES: dict[str, dict[str, str]] = {
    "FEDFUNDS": {"category": "rates", "name": "Federal Funds Effective Rate"},
    "DGS2": {"category": "rates", "name": "2-Year Treasury Constant Maturity Rate"},
    "DGS10": {"category": "rates", "name": "10-Year Treasury Constant Maturity Rate"},
    "T10Y2Y": {"category": "rates", "name": "10-Year Treasury Minus 2-Year Treasury"},
    "CPIAUCSL": {"category": "inflation", "name": "Consumer Price Index for All Urban Consumers"},
    "PCEPI": {"category": "inflation", "name": "Personal Consumption Expenditures Price Index"},
    "UNRATE": {"category": "labor", "name": "Unemployment Rate"},
    "PAYEMS": {"category": "labor", "name": "All Employees, Total Nonfarm"},
    "GDP": {"category": "growth", "name": "Gross Domestic Product"},
    "GDPC1": {"category": "growth", "name": "Real Gross Domestic Product"},
    "M2SL": {"category": "liquidity", "name": "M2 Money Stock"},
    "WALCL": {"category": "liquidity", "name": "Federal Reserve Total Assets"},
    "BAMLH0A0HYM2": {"category": "credit", "name": "ICE BofA US High Yield Option-Adjusted Spread"},
    "VIXCLS": {"category": "risk", "name": "CBOE Volatility Index"},
}


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


def _date_text(value: Any | None) -> str | None:
    parsed = _parse_dt(value)
    if parsed is None:
        return None
    return parsed.date().isoformat()


def _safe_float(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text or text == ".":
        return None
    try:
        return float(text)
    except Exception:
        return None


class FREDMacroData:
    """FRED/ALFRED macro data client with local caching and strategy-date gating.

    Data access requires ``FRED_API_KEY`` and uses the official API with
    ``realtime_start``/``realtime_end`` so backtests can request the vintage
    observations known as of the simulated strategy datetime.
    """

    def __init__(
        self,
        strategy: Any | None = None,
        *,
        cache_dir: str | os.PathLike[str] | None = None,
        api_key: str | None = None,
        min_request_interval_seconds: float = 0.2,
    ) -> None:
        self.strategy = strategy
        self.cache_dir = Path(
            cache_dir
            or os.environ.get("LUMIBOT_FRED_CACHE_DIR")
            or Path.home() / ".lumibot" / "cache" / "fred"
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.api_key = api_key or os.environ.get("FRED_API_KEY")
        self.min_request_interval_seconds = max(float(min_request_interval_seconds), 0.0)
        self._last_request_at = 0.0

    def _strategy_as_of(self) -> datetime:
        if self.strategy is not None and hasattr(self.strategy, "get_datetime"):
            try:
                return _as_of_datetime(self.strategy.get_datetime())
            except Exception:
                pass
        return datetime.now(timezone.utc)

    def _cache_path(self, *parts: str) -> Path:
        safe = [re.sub(r"[^A-Za-z0-9_.=-]+", "_", str(part)).strip("_") for part in parts]
        return self.cache_dir.joinpath(*safe)

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.min_request_interval_seconds:
            time.sleep(self.min_request_interval_seconds - elapsed)
        self._last_request_at = time.monotonic()

    def _get_json(self, url: str, params: dict[str, Any], cache_path: Path) -> dict[str, Any]:
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))
        self._rate_limit()
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return payload

    def list_series(self, category: str | None = None) -> dict[str, Any]:
        rows = []
        wanted_category = str(category).strip().lower() if category else None
        for series_id, metadata in CURATED_FRED_SERIES.items():
            if wanted_category and metadata["category"] != wanted_category:
                continue
            rows.append({"series_id": series_id, **metadata})
        return {
            "source": "fred",
            "series": rows,
            "categories": sorted({metadata["category"] for metadata in CURATED_FRED_SERIES.values()}),
            "notes": (
                "These are curated common macro series. Set FRED_API_KEY for official FRED/ALFRED "
                "API access and point-in-time vintage observations."
            ),
        }

    def get_series(
        self,
        series_id: str,
        *,
        start: Any | None = None,
        end: Any | None = None,
        as_of: Any | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        series = str(series_id or "").strip().upper()
        if not series:
            raise ValueError("series_id is required.")
        as_of_dt = _as_of_datetime(as_of) if as_of is not None else self._strategy_as_of()
        as_of_date = as_of_dt.date()
        start_text = _date_text(start)
        end_text = _date_text(end)
        if end_text is None or date.fromisoformat(end_text) > as_of_date:
            end_text = as_of_date.isoformat()

        if not self.api_key:
            raise ValueError(
                "FRED_API_KEY is required to fetch FRED macro data. "
                "Lumibot uses the official FRED/ALFRED API so backtests can request point-in-time vintage observations."
            )
        return self._get_series_from_api(series, start_text=start_text, end_text=end_text, as_of_date=as_of_date, limit=limit)

    def get_latest(self, series_id: str, *, as_of: Any | None = None) -> dict[str, Any]:
        payload = self.get_series(series_id, as_of=as_of)
        observations = payload.get("observations", [])
        latest = observations[-1] if observations else None
        return {**payload, "latest": latest, "observations": observations[-10:]}

    def get_snapshot(self, series_ids: list[str] | tuple[str, ...] | str, *, as_of: Any | None = None) -> dict[str, Any]:
        if isinstance(series_ids, str):
            requested = [part.strip() for part in series_ids.split(",") if part.strip()]
        else:
            requested = [str(part).strip() for part in series_ids if str(part).strip()]
        values = {}
        errors = {}
        for series_id in requested:
            try:
                values[series_id.upper()] = self.get_latest(series_id, as_of=as_of)["latest"]
            except Exception as exc:
                errors[series_id.upper()] = str(exc)
        as_of_dt = _as_of_datetime(as_of) if as_of is not None else self._strategy_as_of()
        return {"source": "fred", "as_of": as_of_dt.isoformat(), "values": values, "errors": errors}

    def _get_series_from_api(
        self,
        series_id: str,
        *,
        start_text: str | None,
        end_text: str,
        as_of_date: date,
        limit: int | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_end": end_text,
            "realtime_start": as_of_date.isoformat(),
            "realtime_end": as_of_date.isoformat(),
        }
        if start_text:
            params["observation_start"] = start_text
        cache_key = json.dumps({k: v for k, v in params.items() if k != "api_key"}, sort_keys=True)
        payload = self._get_json(
            f"{FRED_API_BASE_URL}/series/observations",
            params,
            self._cache_path("api", series_id, f"{hashlib.sha256(cache_key.encode()).hexdigest()}.json"),
        )
        observations = self._normalize_api_observations(payload.get("observations", []), as_of_date)
        if limit is not None:
            observations = observations[-max(int(limit), 1):]
        return {
            "source": "fred_api",
            "series_id": series_id,
            "as_of": as_of_date.isoformat(),
            "point_in_time_safe": True,
            "uses_revised_data": False,
            "observations": observations,
        }

    def _normalize_api_observations(self, rows: list[dict[str, Any]], as_of_date: date) -> list[dict[str, Any]]:
        observations = []
        for row in rows:
            obs_date_text = row.get("date")
            obs_dt = _parse_dt(obs_date_text)
            if obs_dt is None or obs_dt.date() > as_of_date:
                continue
            observations.append(
                {
                    "date": obs_dt.date().isoformat(),
                    "value": _safe_float(row.get("value")),
                    "realtime_start": row.get("realtime_start"),
                    "realtime_end": row.get("realtime_end"),
                }
            )
        observations.sort(key=lambda row: row["date"])
        return observations
