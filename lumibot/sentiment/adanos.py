import os
from datetime import date, datetime, timezone
from typing import Any

import requests

ADANOS_API_BASE_URL = "https://api.adanos.org"
ADANOS_SOURCES = ("reddit", "x", "news", "polymarket")


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
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


def _source_list(sources: list[str] | tuple[str, ...] | str | None) -> list[str]:
    if sources is None:
        requested = list(ADANOS_SOURCES)
    elif isinstance(sources, str):
        requested = [part.strip().lower() for part in sources.split(",") if part.strip()]
    else:
        requested = [str(part).strip().lower() for part in sources if str(part).strip()]
    if not requested:
        raise ValueError("No Adanos sources provided.")
    invalid = [source for source in requested if source not in ADANOS_SOURCES]
    if invalid:
        raise ValueError(f"Unsupported Adanos source(s): {', '.join(invalid)}")
    return requested


class AdanosMarketSentiment:
    """Small Adanos Market Sentiment API client for US-equity research.

    The helper is intentionally transport-level and optional. Set
    ``ADANOS_API_KEY`` or pass ``api_key=...`` explicitly before calling live
    endpoints.
    """

    def __init__(
        self,
        strategy: Any | None = None,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.strategy = strategy
        self.api_key = api_key or os.environ.get("ADANOS_API_KEY")
        self.base_url = (base_url or os.environ.get("ADANOS_API_BASE_URL") or ADANOS_API_BASE_URL).rstrip("/")
        self.timeout = float(timeout)

    def _strategy_as_of(self) -> datetime:
        if self.strategy is not None and hasattr(self.strategy, "get_datetime"):
            try:
                return _as_of_datetime(self.strategy.get_datetime())
            except Exception:
                pass
        return datetime.now(timezone.utc)

    def _headers(self) -> dict[str, str]:
        key = str(self.api_key or os.environ.get("ADANOS_API_KEY") or "").strip()
        if not key:
            raise ValueError("ADANOS_API_KEY is required to fetch Adanos market sentiment data.")
        return {"X-API-Key": key}

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        response = requests.get(
            f"{self.base_url}{path}",
            headers=self._headers(),
            params={key: value for key, value in params.items() if value not in (None, "")},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            return payload
        return {"data": payload}

    def get_stock_sentiment(
        self,
        symbol: str,
        *,
        sources: list[str] | tuple[str, ...] | str | None = None,
        days: int = 7,
        end: Any | None = None,
    ) -> dict[str, Any]:
        ticker = str(symbol or "").strip().upper()
        if not ticker:
            raise ValueError("symbol is required.")
        day_count = max(int(days), 1)
        end_text = _date_text(end) or self._strategy_as_of().date().isoformat()
        results: dict[str, Any] = {}
        errors: dict[str, str] = {}
        for source in _source_list(sources):
            try:
                results[source] = self._get_json(
                    f"/{source}/stocks/v1/stock/{ticker}",
                    {"days": day_count, "to": end_text},
                )
            except Exception as exc:
                errors[source] = str(exc)
        return {
            "source": "adanos",
            "symbol": ticker,
            "sources": list(results),
            "requested_sources": _source_list(sources),
            "days": day_count,
            "end": end_text,
            "results": results,
            "errors": errors,
        }

    def get_market_sentiment(
        self,
        *,
        sources: list[str] | tuple[str, ...] | str | None = None,
        days: int = 7,
        end: Any | None = None,
    ) -> dict[str, Any]:
        day_count = max(int(days), 1)
        end_text = _date_text(end) or self._strategy_as_of().date().isoformat()
        results: dict[str, Any] = {}
        errors: dict[str, str] = {}
        for source in _source_list(sources):
            try:
                results[source] = self._get_json(
                    f"/{source}/stocks/v1/market-sentiment",
                    {"days": day_count, "to": end_text},
                )
            except Exception as exc:
                errors[source] = str(exc)
        return {
            "source": "adanos",
            "sources": list(results),
            "requested_sources": _source_list(sources),
            "days": day_count,
            "end": end_text,
            "results": results,
            "errors": errors,
        }
