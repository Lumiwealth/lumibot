#!/usr/bin/env python3
"""Run controlled crypto backtests and write validation artifacts.

This runner is intentionally outside the normal unit-test path because it uses
CCXT/Coinbase public market data. Unit tests should cover deterministic fixtures;
this script proves the same invariants with real downloaded crypto candles.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_ROOT = REPO_ROOT.parent / "support-artifacts"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _json_default(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _git_metadata() -> dict[str, Any]:
    def run_git(*args: str) -> str | None:
        try:
            return subprocess.check_output(["git", "-C", str(REPO_ROOT), *args], text=True).strip()
        except Exception:
            return None

    return {
        "branch": run_git("branch", "--show-current"),
        "sha": run_git("rev-parse", "HEAD"),
        "status_short": run_git("status", "--short"),
    }


def _quote_asset_type(symbol: str) -> str:
    return "forex" if symbol.upper() in {"USD", "EUR", "GBP", "JPY"} else "crypto"


def _normalize_timestep(value: str) -> str:
    normalized = str(value or "minute").strip().lower()
    mapping = {
        "1m": "minute",
        "m": "minute",
        "min": "minute",
        "minute": "minute",
        "minutes": "minute",
        "1h": "hour",
        "h": "hour",
        "hour": "hour",
        "hours": "hour",
        "1d": "day",
        "d": "day",
        "day": "day",
        "days": "day",
    }
    if normalized not in mapping:
        raise ValueError(f"Unsupported timestep {value!r}; expected minute, hour, or day")
    return mapping[normalized]


def _load_trade_events(case_dir: Path) -> pd.DataFrame:
    candidates = sorted((case_dir / "logs").glob("*_trade_events.csv"))
    if not candidates:
        return pd.DataFrame()
    return pd.read_csv(candidates[-1])


def _latest_file(case_dir: Path, pattern: str) -> Path | None:
    candidates = sorted((case_dir / "logs").glob(pattern))
    return candidates[-1].resolve() if candidates else None


def _iso_at_fraction(start: datetime, end: datetime, fraction: float) -> str:
    span = end - start
    return (start + span * fraction).isoformat()


def _build_case_plan(
    *,
    start: datetime,
    end: datetime,
    profile: str,
    strategy_classes: dict[str, type],
    alternating_orders: int | None = None,
    alternating_interval_hours: int | None = None,
) -> list[tuple[str, type, dict[str, Any]]]:
    if end <= start:
        raise ValueError("end must be after start")

    if profile == "short":
        buy_at = (start + pd.Timedelta(hours=2)).isoformat()
        sell_at = (start + pd.Timedelta(hours=10)).isoformat()
        interval_hours = 6 if alternating_interval_hours is None else alternating_interval_hours
        max_orders = 4 if alternating_orders is None else alternating_orders
    elif profile == "long":
        buy_at = _iso_at_fraction(start, end, 0.2)
        sell_at = _iso_at_fraction(start, end, 0.8)
        max_orders = 12 if alternating_orders is None else alternating_orders
        if alternating_interval_hours is None:
            span_hours = max((end - start).total_seconds() / 3600, 1)
            interval_hours = max(24, int(span_hours // max(max_orders, 1)))
        else:
            interval_hours = alternating_interval_hours
    else:
        raise ValueError(f"Unknown schedule profile: {profile}")

    return [
        ("buy_hold", strategy_classes["buy_hold"], {"allocation": 1.0}),
        ("round_trip", strategy_classes["round_trip"], {"buy_at": buy_at, "sell_at": sell_at}),
        (
            "alternating",
            strategy_classes["alternating"],
            {"interval_hours": interval_hours, "max_orders": max_orders},
        ),
        ("order_matrix", strategy_classes["order_matrix"], {}),
    ]


def _write_index(run_root: Path, manifest: dict[str, Any]) -> None:
    lines = [
        "# Crypto Backtest Validation Run",
        "",
        f"- Run root: `{run_root}`",
        f"- Created: `{manifest.get('created_at')}`",
        f"- Command: `{manifest.get('command')}`",
        f"- Branch: `{(manifest.get('git') or {}).get('branch')}`",
        f"- SHA: `{(manifest.get('git') or {}).get('sha')}`",
        "",
        "| Case | Symbol | Timestep | Window | Fills | Wall Time | Coverage OK | Price OK | Inside Candle | Time OK | Cache OK | Tear Sheet | Checks |",
        "| --- | --- | --- | --- | ---: | ---: | --- | --- | --- | --- | --- | --- | --- |",
    ]

    for case in manifest.get("cases", []):
        checks = case.get("cache_price_checks") or {}
        coverage = case.get("cache_coverage") or {}
        events = case.get("trade_events") or {}
        artifacts = case.get("artifacts") or {}
        start = str(case.get("start", ""))[:10]
        end = str(case.get("end", ""))[:10]
        tear = artifacts.get("tearsheet_html")
        checks_path = artifacts.get("cache_price_checks")
        tear_link = f"[HTML]({tear})" if tear else ""
        checks_link = f"[CSV]({checks_path})" if checks_path else ""
        lines.append(
            "| "
            + " | ".join(
                [
                    str(case.get("case")),
                    str(case.get("symbol")),
                    str(case.get("timestep")),
                    f"{start} to {end}",
                    str(events.get("fill_rows")),
                    f"{float(case.get('wall_seconds') or 0):.2f}s",
                    str(coverage.get("coverage_ok")),
                    str(checks.get("all_fill_prices_match_expected_execution")),
                    str(checks.get("all_fill_prices_inside_cache_bars")),
                    str(checks.get("all_audit_bar_times_match_fill_times")),
                    str(checks.get("all_audit_bars_match_requested_symbol_cache")),
                    tear_link,
                    checks_link,
                ]
            )
            + " |"
        )

    (run_root / "INDEX.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _parse_event_time(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _extract_submit_bar_timestamp(raw_json: Any) -> pd.Timestamp | pd.NaT:
    if not isinstance(raw_json, str) or not raw_json.strip():
        return pd.NaT
    try:
        payload = json.loads(raw_json)
    except Exception:
        return pd.NaT
    value = payload.get("bar_timestamp")
    if value is None:
        return pd.NaT
    return pd.to_datetime(value, errors="coerce", utc=True)


def _timeframe_from_audit(value: Any) -> str:
    token = str(value or "minute").strip().lower()
    if token in {"minute", "1m", "m"}:
        return "1m"
    if token in {"hour", "1h", "h"}:
        return "1h"
    if token in {"day", "1d", "d"}:
        return "1d"
    return "1m"


def _timestamps_match_timeframe_bucket(left: Any, right: Any, timeframe: str) -> bool:
    left_ts = pd.to_datetime(left, errors="coerce", utc=True)
    right_ts = pd.to_datetime(right, errors="coerce", utc=True)
    if pd.isna(left_ts) or pd.isna(right_ts):
        return False

    if timeframe == "1d":
        return left_ts.date() == right_ts.date()
    if timeframe == "1h":
        return left_ts.floor("h") == right_ts.floor("h")
    return left_ts.floor("min") == right_ts.floor("min")


def _utc_naive_timestamp(value: datetime | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def _cache_coverage_checks(
    *,
    symbol: str,
    exchange_id: str,
    timestep: str,
    start: datetime,
    end: datetime,
    tolerance_hours: float,
) -> dict[str, Any]:
    from lumibot.constants import LUMIBOT_CACHE_FOLDER

    timeframe = _timeframe_from_audit(timestep)
    cache_file = Path(LUMIBOT_CACHE_FOLDER) / exchange_id / f"{symbol.replace('/', '_')}_{timeframe}.duckdb"
    tolerance_seconds = max(float(tolerance_hours) * 3600, 24 * 3600 if timeframe == "1d" else 0)
    requested_start = _utc_naive_timestamp(start)
    requested_end = _utc_naive_timestamp(end)

    result: dict[str, Any] = {
        "cache_file": str(cache_file),
        "timeframe": timeframe,
        "coverage_ok": False,
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "tolerance_seconds": tolerance_seconds,
        "row_count": 0,
        "coverage_start": None,
        "coverage_end": None,
        "start_gap_seconds": None,
        "end_gap_seconds": None,
        "range_metadata_start": None,
        "range_metadata_end": None,
        "range_metadata_count": 0,
        "error": None,
    }
    if not cache_file.exists():
        result["error"] = "cache file missing"
        return result

    try:
        import duckdb

        with duckdb.connect(str(cache_file), read_only=True) as con:
            row_count, coverage_start, coverage_end = con.execute(
                "select count(*), min(datetime), max(datetime) from candles"
            ).fetchone()
            range_count, range_start, range_end = con.execute(
                "select count(*), min(start_dt), max(end_dt) from cache_dt_ranges"
            ).fetchone()
    except Exception as exc:
        result["error"] = str(exc)
        return result

    result["row_count"] = int(row_count or 0)
    result["range_metadata_count"] = int(range_count or 0)
    result["range_metadata_start"] = range_start.isoformat() if range_start is not None else None
    result["range_metadata_end"] = range_end.isoformat() if range_end is not None else None

    if not row_count or coverage_start is None or coverage_end is None:
        result["error"] = "no executable candle rows"
        return result

    coverage_start_ts = pd.Timestamp(coverage_start)
    coverage_end_ts = pd.Timestamp(coverage_end)
    start_gap = max((coverage_start_ts - requested_start).total_seconds(), 0.0)
    end_gap = max((requested_end - coverage_end_ts).total_seconds(), 0.0)

    result["coverage_start"] = coverage_start_ts.isoformat()
    result["coverage_end"] = coverage_end_ts.isoformat()
    result["start_gap_seconds"] = start_gap
    result["end_gap_seconds"] = end_gap
    result["coverage_ok"] = bool(start_gap <= tolerance_seconds and end_gap <= tolerance_seconds)
    if not result["coverage_ok"]:
        result["error"] = "actual candle coverage does not reach requested window"
    return result


def _cache_price_checks(
    *,
    events: pd.DataFrame,
    symbol: str,
    exchange_id: str,
) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()

    from lumibot.tools.ccxt_data_store import CcxtCacheDB

    cache = CcxtCacheDB(exchange_id)
    fills = events[events.get("status", pd.Series(dtype=str)).astype(str).str.lower().eq("fill")].copy()
    rows: list[dict[str, Any]] = []

    def _numeric(value: Any) -> float | None:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(parsed):
            return None
        return float(parsed)

    def _close_enough(left: Any, right: Any, *, tolerance: float = 1e-9) -> bool | None:
        left_num = _numeric(left)
        right_num = _numeric(right)
        if left_num is None or right_num is None:
            return None
        return abs(left_num - right_num) <= tolerance

    for _, row in fills.iterrows():
        fill_time = pd.to_datetime(row.get("time"), errors="coerce", utc=True)
        if pd.isna(fill_time):
            continue
        timeframe = _timeframe_from_audit(row.get("audit.timestep"))
        side = str(row.get("side", "")).strip().lower()
        audit_fill_price = _numeric(row.get("audit.fill.price"))
        if audit_fill_price is not None:
            expected_execution_price = audit_fill_price
            expected_execution_source = "audit_fill_price"
        elif side == "buy":
            expected_execution_price = row.get("audit.asset_quote.final_ask", row.get("audit.asset_quote.ask"))
            expected_execution_source = "quote_ask"
        elif side == "sell":
            expected_execution_price = row.get("audit.asset_quote.final_bid", row.get("audit.asset_quote.bid"))
            expected_execution_source = "quote_bid"
        else:
            expected_execution_price = row.get("audit.asset_quote.price")
            expected_execution_source = "quote_price"

        fill_price = _numeric(row.get("price"))
        expected_execution_price = _numeric(expected_execution_price)
        if expected_execution_price is None:
            expected_execution_price = _numeric(row.get("audit.bar.open"))
            expected_execution_source = "bar_open"
        audit_bar_timestamp = _extract_submit_bar_timestamp(row.get("audit.asset_quote.raw_json"))
        if pd.isna(audit_bar_timestamp):
            audit_bar_timestamp = pd.to_datetime(row.get("audit.bar.datetime"), errors="coerce", utc=True)
        bar_time_matches_fill_time = _timestamps_match_timeframe_bucket(fill_time, audit_bar_timestamp, timeframe)
        price_matches_expected_execution = bool(
            fill_price is not None
            and expected_execution_price is not None
            and abs(fill_price - expected_execution_price) < 1e-9
        )
        audit_bar_open = _numeric(row.get("audit.bar.open"))
        audit_bar_high = _numeric(row.get("audit.bar.high"))
        audit_bar_low = _numeric(row.get("audit.bar.low"))
        audit_bar_close = _numeric(row.get("audit.bar.close"))
        audit_bar_volume = _numeric(row.get("audit.bar.volume"))
        order_type = str(
            row.get("order_type")
            or row.get("audit.fill.order_type")
            or row.get("audit.submit.order_type")
            or ""
        ).lower()
        market_order_expected_open = "market" in order_type

        start = fill_time.to_pydatetime()
        try:
            cache_df = cache.get_data_from_cache(symbol, timeframe, start, start)
        except Exception as exc:
            rows.append(
                {
                    "identifier": row.get("identifier"),
                    "fill_time": fill_time.isoformat(),
                    "timeframe": timeframe,
                    "side": side,
                    "fill_price": fill_price,
                    "expected_execution_price": expected_execution_price,
                    "expected_execution_source": expected_execution_source,
                    "audit_bar_timestamp": audit_bar_timestamp.isoformat() if pd.notna(audit_bar_timestamp) else None,
                    "audit_bar_open": audit_bar_open,
                    "audit_bar_high": audit_bar_high,
                    "audit_bar_low": audit_bar_low,
                    "audit_bar_close": audit_bar_close,
                    "audit_bar_volume": audit_bar_volume,
                    "bar_time_matches_fill_time": bar_time_matches_fill_time,
                    "price_matches_expected_execution": price_matches_expected_execution,
                    "cache_row_exists": False,
                    "cache_open": None,
                    "cache_high": None,
                    "cache_low": None,
                    "cache_close": None,
                    "cache_volume": None,
                    "fill_price_inside_cache_bar": False,
                    "market_fill_matches_cache_open": False if market_order_expected_open else None,
                    "audit_bar_matches_requested_symbol_cache": False,
                    "cache_lookup_error": str(exc),
                }
            )
            continue

        if cache_df.empty:
            rows.append(
                {
                    "identifier": row.get("identifier"),
                    "fill_time": fill_time.isoformat(),
                    "timeframe": timeframe,
                    "side": side,
                    "fill_price": fill_price,
                    "expected_execution_price": expected_execution_price,
                    "expected_execution_source": expected_execution_source,
                    "audit_bar_timestamp": audit_bar_timestamp.isoformat() if pd.notna(audit_bar_timestamp) else None,
                    "audit_bar_open": audit_bar_open,
                    "audit_bar_high": audit_bar_high,
                    "audit_bar_low": audit_bar_low,
                    "audit_bar_close": audit_bar_close,
                    "audit_bar_volume": audit_bar_volume,
                    "bar_time_matches_fill_time": bar_time_matches_fill_time,
                    "price_matches_expected_execution": price_matches_expected_execution,
                    "cache_row_exists": False,
                    "cache_open": None,
                    "cache_high": None,
                    "cache_low": None,
                    "cache_close": None,
                    "cache_volume": None,
                    "fill_price_inside_cache_bar": False,
                    "market_fill_matches_cache_open": False if market_order_expected_open else None,
                    "audit_bar_matches_requested_symbol_cache": False,
                    "cache_lookup_error": "no cached row",
                }
            )
            continue

        cache_row = cache_df.iloc[-1]
        cache_low = float(cache_row["low"])
        cache_high = float(cache_row["high"])
        cache_open = float(cache_row["open"])
        fill_price_inside_cache_bar = bool(
            fill_price is not None
            and cache_low <= fill_price <= cache_high
        )
        market_fill_matches_cache_open = (
            bool(fill_price is not None and abs(fill_price - cache_open) < 1e-9)
            if market_order_expected_open
            else None
        )
        audit_cache_checks = [
            _close_enough(audit_bar_open, cache_row["open"]),
            _close_enough(audit_bar_high, cache_row["high"]),
            _close_enough(audit_bar_low, cache_row["low"]),
            _close_enough(audit_bar_close, cache_row["close"]),
            _close_enough(audit_bar_volume, cache_row["volume"]),
        ]
        present_audit_cache_checks = [check for check in audit_cache_checks if check is not None]
        audit_bar_matches_requested_symbol_cache = bool(
            present_audit_cache_checks and all(present_audit_cache_checks)
        )
        rows.append(
            {
                "identifier": row.get("identifier"),
                "fill_time": fill_time.isoformat(),
                "timeframe": timeframe,
                "side": side,
                "fill_price": fill_price,
                "expected_execution_price": expected_execution_price,
                "expected_execution_source": expected_execution_source,
                "audit_bar_timestamp": audit_bar_timestamp.isoformat() if pd.notna(audit_bar_timestamp) else None,
                "audit_bar_open": audit_bar_open,
                "audit_bar_high": audit_bar_high,
                "audit_bar_low": audit_bar_low,
                "audit_bar_close": audit_bar_close,
                "audit_bar_volume": audit_bar_volume,
                "bar_time_matches_fill_time": bar_time_matches_fill_time,
                "price_matches_expected_execution": price_matches_expected_execution,
                "cache_row_exists": True,
                "cache_open": cache_open,
                "cache_high": cache_high,
                "cache_low": cache_low,
                "cache_close": float(cache_row["close"]),
                "cache_volume": float(cache_row["volume"]),
                "fill_price_inside_cache_bar": fill_price_inside_cache_bar,
                "market_fill_matches_cache_open": market_fill_matches_cache_open,
                "audit_bar_matches_requested_symbol_cache": audit_bar_matches_requested_symbol_cache,
                "cache_lookup_error": None,
            }
        )
    return pd.DataFrame(rows)


def _analyze_trade_events(events: pd.DataFrame) -> dict[str, Any]:
    if events.empty:
        return {
            "trade_event_rows": 0,
            "fill_rows": 0,
            "max_fill_event_vs_bar_gap_seconds": None,
            "max_submit_quote_staleness_seconds": None,
            "stale_submit_quote_rows": 0,
        }

    status = events.get("status", pd.Series(index=events.index, dtype=str)).astype(str).str.lower()
    fills = events[status.eq("fill")].copy()
    new_orders = events[status.eq("new")].copy()

    max_fill_gap = None
    if not fills.empty and {"time", "audit.bar.datetime"}.issubset(fills.columns):
        fill_times = _parse_event_time(fills["time"])
        bar_times = _parse_event_time(fills["audit.bar.datetime"])
        gaps = (fill_times - bar_times).dt.total_seconds().abs().dropna()
        max_fill_gap = float(gaps.max()) if not gaps.empty else None

    stale_rows = 0
    max_submit_staleness = None
    if not new_orders.empty and "audit.submit.asset_quote.raw_json" in new_orders.columns:
        submit_times = _parse_event_time(new_orders["time"])
        quote_bar_times = pd.to_datetime(
            new_orders["audit.submit.asset_quote.raw_json"].apply(_extract_submit_bar_timestamp),
            errors="coerce",
            utc=True,
        )
        stale = (submit_times - quote_bar_times).dt.total_seconds().abs().dropna()
        stale_rows = int((stale > 60).sum())
        max_submit_staleness = float(stale.max()) if not stale.empty else None

    return {
        "trade_event_rows": int(len(events)),
        "fill_rows": int(len(fills)),
        "new_order_rows": int(len(new_orders)),
        "max_fill_event_vs_bar_gap_seconds": max_fill_gap,
        "max_submit_quote_staleness_seconds": max_submit_staleness,
        "stale_submit_quote_rows": stale_rows,
    }


def _build_strategy_classes():
    from lumibot.entities import Asset, Order
    from lumibot.strategies.strategy import Strategy

    class _CryptoValidationBase(Strategy):
        def initialize(
            self,
            base_symbol: str = "BTC",
            quote_symbol: str = "USDT",
            allocation: float = 0.5,
            sleeptime: str = "1H",
            time_in_force: str = "gtc",
            **_kwargs,
        ):
            params = dict(getattr(self, "parameters", {}) or {})
            base_symbol = str(params.get("base_symbol", base_symbol))
            quote_symbol = str(params.get("quote_symbol", quote_symbol))
            allocation = params.get("allocation", allocation)
            sleeptime = str(params.get("sleeptime", sleeptime))
            time_in_force = str(params.get("time_in_force", time_in_force))
            data_timestep = _normalize_timestep(params.get("timestep", "minute"))

            self.set_market("24/7")
            self.sleeptime = sleeptime
            self.base = Asset(base_symbol.upper(), asset_type=Asset.AssetType.CRYPTO)
            self.quote = Asset(quote_symbol.upper(), asset_type=_quote_asset_type(quote_symbol))
            self.asset_pair = (self.base, self.quote)
            self.allocation = float(allocation)
            self.time_in_force = time_in_force
            self.data_timestep = data_timestep
            self.validation_events: list[dict[str, Any]] = []
            if hasattr(self.broker, "data_source"):
                setattr(self.broker.data_source, "_timestep", data_timestep)
            self._event("data_timestep_set", timestep=data_timestep)

        def _event(self, event: str, **fields: Any) -> None:
            try:
                now = self.get_datetime().isoformat()
            except Exception:
                now = None
            self.validation_events.append({"time": now, "event": event, **fields})

        def _last_price(self) -> float | None:
            price = self.get_last_price(self.base, quote=self.quote)
            if price is None or float(price) <= 0:
                self._event("missing_price")
                return None
            return float(price)

        def _position_quantity(self) -> float:
            try:
                position = self.get_position(self.base)
            except Exception:
                position = None
            if position is None:
                return 0.0
            return abs(float(position.quantity or 0.0))

        def _quantity_from_cash(self) -> float:
            price = self._last_price()
            if price is None:
                return 0.0
            cash = float(self.get_cash())
            return round((cash * self.allocation) / price, 8)

        def _submit_market(self, side, quantity: float):
            if quantity <= 0:
                return None
            order = self.create_order(
                self.base,
                quantity,
                side=side,
                type=Order.OrderType.MARKET,
                quote=self.quote,
                time_in_force=self.time_in_force,
            )
            self.submit_order(order)
            self._event(
                "submitted",
                side=str(side),
                quantity=quantity,
                order_type=str(Order.OrderType.MARKET),
                order_class=str(getattr(order, "order_class", None)),
                time_in_force=self.time_in_force,
                identifier=getattr(order, "identifier", None),
            )
            return order

        def _submit_order(self, *, label: str, side, quantity: float, **kwargs):
            if quantity <= 0:
                return None
            order = self.create_order(
                self.base,
                quantity,
                side=side,
                quote=self.quote,
                time_in_force=self.time_in_force,
                **kwargs,
            )
            self.submit_order(order)
            self._event(
                "submitted",
                label=label,
                side=str(side),
                quantity=quantity,
                order_type=str(getattr(order, "order_type", None)),
                order_class=str(getattr(order, "order_class", None)),
                limit_price=getattr(order, "limit_price", None),
                stop_price=getattr(order, "stop_price", None),
                stop_limit_price=getattr(order, "stop_limit_price", None),
                trail_price=getattr(order, "trail_price", None),
                trail_percent=getattr(order, "trail_percent", None),
                identifier=getattr(order, "identifier", None),
                children=len(getattr(order, "child_orders", []) or []),
            )
            return order

    class BuyAndHoldCryptoValidation(_CryptoValidationBase):
        def initialize(self, **kwargs):
            super().initialize(**kwargs)
            self.bought = False

        def on_trading_iteration(self):
            if self.bought:
                return
            quantity = self._quantity_from_cash()
            order = self._submit_market(Order.OrderSide.BUY, quantity)
            if order is not None:
                self.bought = True

    class ScheduledRoundTripCryptoValidation(_CryptoValidationBase):
        def initialize(self, buy_at: str | None = None, sell_at: str | None = None, **kwargs):
            super().initialize(**kwargs)
            params = dict(getattr(self, "parameters", {}) or {})
            buy_at = params.get("buy_at", buy_at)
            sell_at = params.get("sell_at", sell_at)
            if buy_at is None or sell_at is None:
                raise ValueError("ScheduledRoundTripCryptoValidation requires buy_at and sell_at parameters")
            self.buy_at = _parse_dt(buy_at)
            self.sell_at = _parse_dt(sell_at)
            self.bought = False
            self.sold = False
            self.quantity = 0.0

        def on_trading_iteration(self):
            current = self.get_datetime()
            if current.tzinfo is None:
                current = current.replace(tzinfo=timezone.utc)
            current = current.astimezone(timezone.utc)
            if not self.bought and current >= self.buy_at:
                self.quantity = self._quantity_from_cash()
                order = self._submit_market(Order.OrderSide.BUY, self.quantity)
                if order is not None:
                    self.bought = True
            elif self.bought and not self.sold and current >= self.sell_at:
                order = self._submit_market(Order.OrderSide.SELL, self.quantity)
                if order is not None:
                    self.sold = True

    class AlternatingCryptoValidation(_CryptoValidationBase):
        def initialize(self, interval_hours: int = 6, max_orders: int = 6, **kwargs):
            super().initialize(**kwargs)
            params = dict(getattr(self, "parameters", {}) or {})
            interval_hours = params.get("interval_hours", interval_hours)
            max_orders = params.get("max_orders", max_orders)
            self.interval_hours = int(interval_hours)
            self.max_orders = int(max_orders)
            self.next_trade_at = None
            self.order_count = 0
            self.side = Order.OrderSide.BUY
            self.quantity = 0.0

        def on_trading_iteration(self):
            current = self.get_datetime()
            if current.tzinfo is None:
                current = current.replace(tzinfo=timezone.utc)
            current = current.astimezone(timezone.utc)
            if self.next_trade_at is None:
                self.next_trade_at = current
            if self.order_count >= self.max_orders or current < self.next_trade_at:
                return

            if self.side == Order.OrderSide.BUY:
                self.quantity = self._quantity_from_cash()
                order = self._submit_market(Order.OrderSide.BUY, self.quantity)
                if order is None:
                    return
                self.side = Order.OrderSide.SELL
            else:
                order = self._submit_market(Order.OrderSide.SELL, self.quantity)
                if order is None:
                    return
                self.side = Order.OrderSide.BUY
            self.order_count += 1
            self.next_trade_at = current + pd.Timedelta(hours=self.interval_hours).to_pytimedelta()

    class OrderMatrixCryptoValidation(_CryptoValidationBase):
        ACTIONS = (
            "market_buy",
            "market_sell",
            "limit_buy",
            "limit_sell",
            "stop_buy",
            "stop_sell",
            "stop_limit_buy",
            "stop_limit_sell",
            "trailing_entry",
            "trailing_sell",
            "bracket_take_profit",
            "bracket_stop",
            "bracket_trailing",
            "oco_entry_limit",
            "oco_limit_exit",
            "oco_entry_stop",
            "oco_stop_exit",
            "oto_limit",
            "oto_stop",
        )

        def initialize(
            self,
            window_start: str | None = None,
            window_end: str | None = None,
            matrix_notional_fraction: float = 0.08,
            matrix_spacing_hours: int | None = None,
            **kwargs,
        ):
            super().initialize(**kwargs)
            params = dict(getattr(self, "parameters", {}) or {})
            self.window_start = _parse_dt(params.get("window_start", window_start)) if params.get("window_start", window_start) else None
            self.window_end = _parse_dt(params.get("window_end", window_end)) if params.get("window_end", window_end) else None
            self.matrix_notional_fraction = float(params.get("matrix_notional_fraction", matrix_notional_fraction))
            self.action_index = 0
            self.next_action_at = self.window_start
            spacing_override = params.get("matrix_spacing_hours", matrix_spacing_hours)
            if spacing_override is not None:
                spacing_hours = max(1, int(spacing_override))
            elif self.window_start is not None and self.window_end is not None and self.window_end > self.window_start:
                span_hours = max((self.window_end - self.window_start).total_seconds() / 3600, 1)
                spacing_hours = max(2, int(span_hours // (len(self.ACTIONS) + 2)))
            else:
                spacing_hours = 24
            self.spacing = pd.Timedelta(hours=spacing_hours).to_pytimedelta()
            self._event("matrix_initialized", actions=len(self.ACTIONS), spacing_hours=spacing_hours)

        def _active_order_count(self) -> int:
            try:
                return len(self.get_orders(statuses=Order.ACTIVE_STATUSES))
            except Exception:
                return 0

        def _current_quantity(self) -> float:
            price = self._last_price()
            if price is None:
                return 0.0
            cash = max(0.0, float(self.get_cash()))
            return round((cash * self.matrix_notional_fraction) / price, 8)

        def _price(self) -> float | None:
            return self._last_price()

        def _ensure_flat(self) -> bool:
            qty = self._position_quantity()
            if qty <= 0:
                return True
            self._event("flatten_before_next_action", quantity=qty)
            self._submit_market(Order.OrderSide.SELL, qty)
            return False

        def _ensure_position(self) -> float:
            qty = self._position_quantity()
            if qty > 0:
                return qty
            qty = self._current_quantity()
            if qty > 0:
                self._event("entry_before_exit_action", quantity=qty)
                self._submit_market(Order.OrderSide.BUY, qty)
            return 0.0

        def _advance(self, current: datetime, action: str) -> None:
            self._event("action_completed", action=action, action_index=self.action_index)
            self.action_index += 1
            self.next_action_at = current + self.spacing

        def _run_action(self, action: str) -> bool:
            price = self._price()
            if price is None:
                return False

            qty = self._current_quantity()
            if action in {"market_buy", "limit_buy", "stop_buy", "stop_limit_buy", "trailing_entry"}:
                if not self._ensure_flat():
                    return False
            elif action in {"bracket_take_profit", "bracket_stop", "bracket_stop_limit", "bracket_trailing", "oto_limit", "oto_stop"}:
                if not self._ensure_flat():
                    return False
            elif action.startswith("oco_entry"):
                if not self._ensure_flat():
                    return False

            if action == "market_buy":
                self._submit_market(Order.OrderSide.BUY, qty)
            elif action == "market_sell":
                position_qty = self._ensure_position()
                if position_qty <= 0:
                    return False
                self._submit_market(Order.OrderSide.SELL, position_qty)
            elif action == "limit_buy":
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.BUY,
                    quantity=qty,
                    type=Order.OrderType.LIMIT,
                    limit_price=round(price * 1.02, 2),
                )
            elif action == "limit_sell":
                position_qty = self._ensure_position()
                if position_qty <= 0:
                    return False
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.SELL,
                    quantity=position_qty,
                    type=Order.OrderType.LIMIT,
                    limit_price=round(price * 0.98, 2),
                )
            elif action == "stop_buy":
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.BUY,
                    quantity=qty,
                    type=Order.OrderType.STOP,
                    stop_price=round(price * 0.98, 2),
                )
            elif action == "stop_sell":
                position_qty = self._ensure_position()
                if position_qty <= 0:
                    return False
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.SELL,
                    quantity=position_qty,
                    type=Order.OrderType.STOP,
                    stop_price=round(price * 1.02, 2),
                )
            elif action == "stop_limit_buy":
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.BUY,
                    quantity=qty,
                    type=Order.OrderType.STOP_LIMIT,
                    stop_price=round(price * 0.98, 2),
                    limit_price=round(price * 1.02, 2),
                )
            elif action == "stop_limit_sell":
                position_qty = self._ensure_position()
                if position_qty <= 0:
                    return False
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.SELL,
                    quantity=position_qty,
                    type=Order.OrderType.STOP_LIMIT,
                    stop_price=round(price * 1.02, 2),
                    limit_price=round(price * 0.98, 2),
                )
            elif action == "trailing_entry":
                self._submit_market(Order.OrderSide.BUY, qty)
            elif action == "trailing_sell":
                position_qty = self._ensure_position()
                if position_qty <= 0:
                    return False
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.SELL,
                    quantity=position_qty,
                    type=Order.OrderType.TRAIL,
                    trail_price=max(round(price * 0.002, 2), 0.01),
                )
            elif action == "bracket_take_profit":
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.BUY,
                    quantity=qty,
                    type=Order.OrderType.MARKET,
                    order_class=Order.OrderClass.BRACKET,
                    secondary_limit_price=round(price * 0.98, 2),
                    secondary_stop_price=round(price * 0.50, 2),
                )
            elif action == "bracket_stop":
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.BUY,
                    quantity=qty,
                    type=Order.OrderType.MARKET,
                    order_class=Order.OrderClass.BRACKET,
                    secondary_limit_price=round(price * 1.50, 2),
                    secondary_stop_price=round(price * 1.02, 2),
                )
            elif action == "bracket_trailing":
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.BUY,
                    quantity=qty,
                    type=Order.OrderType.MARKET,
                    order_class=Order.OrderClass.BRACKET,
                    secondary_limit_price=round(price * 1.50, 2),
                    secondary_stop_price=round(price * 0.98, 2),
                    secondary_trail_price=max(round(price * 0.002, 2), 0.01),
                )
            elif action in {"oco_entry_limit", "oco_entry_stop"}:
                self._submit_market(Order.OrderSide.BUY, qty)
            elif action == "oco_limit_exit":
                position_qty = self._ensure_position()
                if position_qty <= 0:
                    return False
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.SELL,
                    quantity=position_qty,
                    order_class=Order.OrderClass.OCO,
                    limit_price=round(price * 0.98, 2),
                    stop_price=round(price * 0.50, 2),
                )
            elif action == "oco_stop_exit":
                position_qty = self._ensure_position()
                if position_qty <= 0:
                    return False
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.SELL,
                    quantity=position_qty,
                    order_class=Order.OrderClass.OCO,
                    limit_price=round(price * 1.50, 2),
                    stop_price=round(price * 1.02, 2),
                )
            elif action == "oto_limit":
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.BUY,
                    quantity=qty,
                    type=Order.OrderType.MARKET,
                    order_class=Order.OrderClass.OTO,
                    secondary_limit_price=round(price * 0.98, 2),
                )
            elif action == "oto_stop":
                self._submit_order(
                    label=action,
                    side=Order.OrderSide.BUY,
                    quantity=qty,
                    type=Order.OrderType.MARKET,
                    order_class=Order.OrderClass.OTO,
                    secondary_stop_price=round(price * 1.02, 2),
                )
            else:
                raise ValueError(f"Unknown matrix action {action}")

            return True

        def on_trading_iteration(self):
            if self.action_index >= len(self.ACTIONS):
                return
            current = self.get_datetime()
            if current.tzinfo is None:
                current = current.replace(tzinfo=timezone.utc)
            current = current.astimezone(timezone.utc)
            if self.next_action_at is not None and current < self.next_action_at:
                return

            active_count = self._active_order_count()
            if active_count:
                self._event("waiting_for_active_orders", active_count=active_count, action_index=self.action_index)
                return

            action = self.ACTIONS[self.action_index]
            self._event("action_started", action=action, action_index=self.action_index)
            if self._run_action(action):
                self._advance(current, action)

    return {
        "buy_hold": BuyAndHoldCryptoValidation,
        "round_trip": ScheduledRoundTripCryptoValidation,
        "alternating": AlternatingCryptoValidation,
        "order_matrix": OrderMatrixCryptoValidation,
    }


def _run_case(
    *,
    case_name: str,
    strategy_cls,
    run_root: Path,
    start: datetime,
    end: datetime,
    exchange_id: str,
    base_symbol: str,
    quote_symbol: str,
    sleeptime: str,
    timestep: str,
    budget: float,
    max_download_limit: int,
    coverage_tolerance_hours: float,
    extra_parameters: dict[str, Any],
) -> dict[str, Any]:
    from lumibot.backtesting import CcxtBacktesting
    from lumibot.entities import Asset

    case_dir = run_root / case_name
    case_dir.mkdir(parents=True, exist_ok=True)
    previous_cwd = Path.cwd()
    symbol = f"{base_symbol.upper()}/{quote_symbol.upper()}"
    quote_asset = Asset(quote_symbol.upper(), asset_type=_quote_asset_type(quote_symbol))

    params = {
        "base_symbol": base_symbol,
        "quote_symbol": quote_symbol,
        "sleeptime": sleeptime,
        "timestep": timestep,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        **extra_parameters,
    }

    started = time.monotonic()
    try:
        os.chdir(case_dir)
        result, strategy = strategy_cls.run_backtest(
            CcxtBacktesting,
            start,
            end,
            benchmark_asset=symbol,
            quote_asset=quote_asset,
            parameters=params,
            exchange_id=exchange_id,
            max_data_download_limit=max_download_limit,
            market="24/7",
            budget=budget,
            analyze_backtest=True,
            show_plot=False,
            show_tearsheet=False,
            show_indicators=False,
            save_tearsheet=True,
            quiet_logs=True,
            show_progress_bar=False,
            name=case_name,
        )
    finally:
        os.chdir(previous_cwd)
    wall_seconds = time.monotonic() - started

    events = _load_trade_events(case_dir)
    checks = _cache_price_checks(events=events, symbol=symbol, exchange_id=exchange_id)
    if not checks.empty:
        checks.to_csv(case_dir / "cache_price_checks.csv", index=False)
    coverage_checks = _cache_coverage_checks(
        symbol=symbol,
        exchange_id=exchange_id,
        timestep=timestep,
        start=start,
        end=end,
        tolerance_hours=coverage_tolerance_hours,
    )

    summary = {
        "case": case_name,
        "symbol": symbol,
        "exchange_id": exchange_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sleeptime": sleeptime,
        "timestep": timestep,
        "wall_seconds": wall_seconds,
        "result": result,
        "artifacts": {
            "case_dir": str(case_dir.resolve()),
            "logs": str((case_dir / "logs").resolve()),
            "tearsheet_html": str(_latest_file(case_dir, "*_tearsheet.html")) if _latest_file(case_dir, "*_tearsheet.html") else None,
            "trades_csv": str(_latest_file(case_dir, "*_trades.csv")) if _latest_file(case_dir, "*_trades.csv") else None,
            "trade_events_csv": str(_latest_file(case_dir, "*_trade_events.csv")) if _latest_file(case_dir, "*_trade_events.csv") else None,
            "cache_price_checks": str((case_dir / "cache_price_checks.csv").resolve()) if not checks.empty else None,
        },
        "cache_coverage": coverage_checks,
        "trade_events": _analyze_trade_events(events),
        "cache_price_checks": {
            "rows": int(len(checks)),
            "all_cache_rows_exist": bool(
                not checks.empty and checks["cache_row_exists"].fillna(False).all()
            ),
            "all_fill_prices_match_expected_execution": bool(
                not checks.empty and checks["price_matches_expected_execution"].fillna(False).all()
            ),
            "all_audit_bar_times_match_fill_times": bool(
                not checks.empty and checks["bar_time_matches_fill_time"].fillna(False).all()
            ),
            "all_fill_prices_inside_cache_bars": bool(
                not checks.empty and checks["fill_price_inside_cache_bar"].fillna(False).all()
            ),
            "all_market_fills_match_cache_open": bool(
                not checks.empty
                and checks["market_fill_matches_cache_open"].dropna().astype(bool).all()
            ),
            "all_audit_bars_match_requested_symbol_cache": bool(
                not checks.empty and checks["audit_bar_matches_requested_symbol_cache"].fillna(False).all()
            ),
            "missing_cache_rows": int((~checks.get("cache_row_exists", pd.Series(dtype=bool)).fillna(False)).sum())
            if not checks.empty
            else 0,
            "mismatched_expected_execution_price_rows": int(
                (~checks.get("price_matches_expected_execution", pd.Series(dtype=bool)).fillna(False)).sum()
            )
            if not checks.empty
            else 0,
            "mismatched_bar_time_rows": int(
                (~checks.get("bar_time_matches_fill_time", pd.Series(dtype=bool)).fillna(False)).sum()
            )
            if not checks.empty
            else 0,
            "fill_price_outside_cache_bar_rows": int(
                (~checks.get("fill_price_inside_cache_bar", pd.Series(dtype=bool)).fillna(False)).sum()
            )
            if not checks.empty
            else 0,
            "market_fill_cache_open_mismatch_rows": int(
                (~checks.get("market_fill_matches_cache_open", pd.Series(dtype=bool)).dropna().astype(bool)).sum()
            )
            if not checks.empty and "market_fill_matches_cache_open" in checks
            else 0,
            "mismatched_requested_symbol_cache_rows": int(
                (~checks.get("audit_bar_matches_requested_symbol_cache", pd.Series(dtype=bool)).fillna(False)).sum()
            )
            if not checks.empty
            else 0,
        },
        "strategy_validation_events": getattr(strategy, "validation_events", []),
    }
    (case_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=_json_default), encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exchange", default="coinbase")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--start", default="2026-03-15T00:00:00+00:00")
    parser.add_argument("--end", default="2026-03-17T00:00:00+00:00")
    parser.add_argument("--sleeptime", default="1H")
    parser.add_argument(
        "--timestep",
        default="minute",
        help="Native CCXT cache/backtest timestep: minute/1m, hour/1h, or day/1d.",
    )
    parser.add_argument("--budget", type=float, default=10_000.0)
    parser.add_argument("--max-download-limit", type=int, default=500_000)
    parser.add_argument(
        "--coverage-tolerance-hours",
        type=float,
        default=24.0,
        help="Maximum allowed actual candle coverage gap at either end of the requested window.",
    )
    parser.add_argument("--output-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--warm-repeat", action="store_true", help="Run the same cases twice against the same cache root.")
    parser.add_argument(
        "--schedule-profile",
        choices=("short", "long"),
        default="short",
        help="short keeps quick smoke timings; long spreads trades across the full requested window.",
    )
    parser.add_argument("--alternating-orders", type=int, default=None)
    parser.add_argument("--alternating-interval-hours", type=int, default=None)
    args = parser.parse_args()

    base_symbol, quote_symbol = [part.strip().upper() for part in args.symbol.replace("-", "/").split("/", 1)]
    start = _parse_dt(args.start)
    end = _parse_dt(args.end)
    args.timestep = _normalize_timestep(args.timestep)
    run_id = args.run_id or f"crypto-backtest-validation-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    run_root = Path(args.output_root).expanduser().resolve() / run_id
    run_root.mkdir(parents=True, exist_ok=True)

    cache_root = run_root / "cache"
    os.environ["LUMIBOT_CACHE_FOLDER"] = str(cache_root)
    os.environ["LUMIBOT_BACKTEST_AUDIT"] = "1"
    os.environ["BACKTESTING_DATA_SOURCE"] = "ccxt"
    os.environ["LUMIBOT_DISABLE_DOTENV"] = "1"
    os.environ["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"

    import lumibot

    strategy_classes = _build_strategy_classes()
    cases = _build_case_plan(
        start=start,
        end=end,
        profile=args.schedule_profile,
        strategy_classes=strategy_classes,
        alternating_orders=args.alternating_orders,
        alternating_interval_hours=args.alternating_interval_hours,
    )
    if args.warm_repeat:
        cases = cases + [(f"{name}_warm", cls, params) for name, cls, params in cases]

    manifest = {
        "run_root": str(run_root),
        "cache_root": str(cache_root),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git": _git_metadata(),
        "command": " ".join(os.sys.argv),
        "inputs": vars(args),
        "lumibot": {
            "version": getattr(lumibot, "__version__", None),
            "file": getattr(lumibot, "__file__", None),
        },
        "effective_environment": {
            "BACKTESTING_DATA_SOURCE": os.environ.get("BACKTESTING_DATA_SOURCE"),
            "LUMIBOT_DISABLE_DOTENV": os.environ.get("LUMIBOT_DISABLE_DOTENV"),
            "LUMIBOT_DISABLE_DOTENV_LOCAL": os.environ.get("LUMIBOT_DISABLE_DOTENV_LOCAL"),
        },
        "cases": [],
    }
    (run_root / "manifest.json").write_text(json.dumps(manifest, indent=2, default=_json_default), encoding="utf-8")
    _write_index(run_root, manifest)

    for case_name, cls, params in cases:
        summary = _run_case(
            case_name=case_name,
            strategy_cls=cls,
            run_root=run_root,
            start=start,
            end=end,
            exchange_id=args.exchange,
            base_symbol=base_symbol,
            quote_symbol=quote_symbol,
            sleeptime=args.sleeptime,
            timestep=args.timestep,
            budget=args.budget,
            max_download_limit=args.max_download_limit,
            coverage_tolerance_hours=args.coverage_tolerance_hours,
            extra_parameters=params,
        )
        manifest["cases"].append(summary)
        (run_root / "manifest.json").write_text(json.dumps(manifest, indent=2, default=_json_default), encoding="utf-8")
        _write_index(run_root, manifest)

    print(json.dumps({"run_root": str(run_root), "cases": len(manifest["cases"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
