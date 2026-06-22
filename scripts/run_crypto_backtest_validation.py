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


def _load_trade_events(case_dir: Path) -> pd.DataFrame:
    candidates = sorted((case_dir / "logs").glob("*_trade_events.csv"))
    if not candidates:
        return pd.DataFrame()
    return pd.read_csv(candidates[-1])


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
        if side == "buy":
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
        bar_time_matches_fill_time = bool(
            pd.notna(audit_bar_timestamp)
            and abs((fill_time - audit_bar_timestamp).total_seconds()) < 1e-9
        )
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
                    "audit_bar_matches_requested_symbol_cache": False,
                    "cache_lookup_error": "no cached row",
                }
            )
            continue

        cache_row = cache_df.iloc[-1]
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
                "cache_open": float(cache_row["open"]),
                "cache_high": float(cache_row["high"]),
                "cache_low": float(cache_row["low"]),
                "cache_close": float(cache_row["close"]),
                "cache_volume": float(cache_row["volume"]),
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

            self.set_market("24/7")
            self.sleeptime = sleeptime
            self.base = Asset(base_symbol.upper(), asset_type=Asset.AssetType.CRYPTO)
            self.quote = Asset(quote_symbol.upper(), asset_type=_quote_asset_type(quote_symbol))
            self.asset_pair = (self.base, self.quote)
            self.allocation = float(allocation)
            self.time_in_force = time_in_force
            self.validation_events: list[dict[str, Any]] = []

        def _quantity_from_cash(self) -> float:
            price = self.get_last_price(self.base, quote=self.quote)
            if price is None or float(price) <= 0:
                self.validation_events.append(
                    {
                        "time": self.get_datetime().isoformat(),
                        "event": "missing_price",
                    }
                )
                return 0.0
            cash = float(self.get_cash())
            return round((cash * self.allocation) / float(price), 8)

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
            self.validation_events.append(
                {
                    "time": self.get_datetime().isoformat(),
                    "event": "submitted",
                    "side": str(side),
                    "quantity": quantity,
                    "time_in_force": self.time_in_force,
                }
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

    return {
        "buy_hold": BuyAndHoldCryptoValidation,
        "round_trip": ScheduledRoundTripCryptoValidation,
        "alternating": AlternatingCryptoValidation,
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
    budget: float,
    max_download_limit: int,
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

    summary = {
        "case": case_name,
        "symbol": symbol,
        "exchange_id": exchange_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sleeptime": sleeptime,
        "wall_seconds": wall_seconds,
        "result": result,
        "artifacts": {
            "case_dir": str(case_dir.resolve()),
            "logs": str((case_dir / "logs").resolve()),
            "cache_price_checks": str((case_dir / "cache_price_checks.csv").resolve()) if not checks.empty else None,
        },
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
    parser.add_argument("--budget", type=float, default=10_000.0)
    parser.add_argument("--max-download-limit", type=int, default=500_000)
    parser.add_argument("--output-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--warm-repeat", action="store_true", help="Run the same cases twice against the same cache root.")
    args = parser.parse_args()

    base_symbol, quote_symbol = [part.strip().upper() for part in args.symbol.replace("-", "/").split("/", 1)]
    start = _parse_dt(args.start)
    end = _parse_dt(args.end)
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
    buy_at = (start + pd.Timedelta(hours=2)).isoformat()
    sell_at = (start + pd.Timedelta(hours=10)).isoformat()
    cases = [
        ("buy_hold", strategy_classes["buy_hold"], {}),
        ("round_trip", strategy_classes["round_trip"], {"buy_at": buy_at, "sell_at": sell_at}),
        ("alternating", strategy_classes["alternating"], {"interval_hours": 6, "max_orders": 4}),
    ]
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
            budget=args.budget,
            max_download_limit=args.max_download_limit,
            extra_parameters=params,
        )
        manifest["cases"].append(summary)
        (run_root / "manifest.json").write_text(json.dumps(manifest, indent=2, default=_json_default), encoding="utf-8")

    print(json.dumps({"run_root": str(run_root), "cases": len(manifest["cases"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
