#!/usr/bin/env python3
"""
Check IBKR daily-history depth for stocks/indexes through LumiBot's IBKR helper.

Example:
  python3 scripts/check_ibkr_daily_history_depth.py \
    --symbols SPY:stock QQQ:stock SPX:index NDX:index VIX:index \
    --min-years 10 \
    --probe-years 25
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd

from lumibot.entities import Asset
from lumibot.tools import ibkr_helper


DEFAULT_SYMBOLS: list[str] = [
    "SPY:stock",
    "QQQ:stock",
    "SPX:index",
    "NDX:index",
    "VIX:index",
]


@dataclass
class SymbolSpec:
    symbol: str
    asset_type: str


def _parse_symbol_specs(items: Iterable[str]) -> list[SymbolSpec]:
    parsed: list[SymbolSpec] = []
    allowed_types = {"stock", "index"}
    for raw in items:
        token = str(raw).strip()
        if ":" not in token:
            raise ValueError(f"Invalid symbol token '{token}'. Expected SYMBOL:TYPE (e.g. SPY:stock).")
        symbol, asset_type = token.split(":", 1)
        symbol = symbol.strip().upper()
        asset_type = asset_type.strip().lower()
        if asset_type not in allowed_types:
            raise ValueError(
                f"Unsupported asset type '{asset_type}' for '{token}'. Allowed: {sorted(allowed_types)}."
            )
        parsed.append(SymbolSpec(symbol=symbol, asset_type=asset_type))
    return parsed


def _format_dt(value: pd.Timestamp) -> str:
    return value.isoformat()


def _as_utc(value: datetime | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help="Space-separated SYMBOL:TYPE entries (TYPE: stock|index).",
    )
    parser.add_argument(
        "--min-years",
        type=float,
        default=10.0,
        help="Minimum required daily-history depth in years (default: 10).",
    )
    parser.add_argument(
        "--probe-years",
        type=float,
        default=25.0,
        help="How far back to request from IBKR (default: 25). Must be >= min-years.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Optional end date in YYYY-MM-DD. Defaults to now.",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="Trades",
        help="IBKR history source (default: Trades).",
    )
    args = parser.parse_args()

    if args.probe_years < args.min_years:
        print("--probe-years must be >= --min-years", file=sys.stderr)
        return 2

    end_dt = (
        datetime.strptime(args.end_date, "%Y-%m-%d")
        if args.end_date
        else datetime.now()
    )
    start_dt = end_dt - timedelta(days=int(args.probe_years * 365.2425))
    min_cutoff = end_dt - timedelta(days=int(args.min_years * 365.2425))

    try:
        symbols = _parse_symbol_specs(args.symbols)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(f"Probe window: {start_dt.date()} -> {end_dt.date()} (source={args.source})")
    print(f"Minimum required depth: {args.min_years:.2f} years (cutoff <= {min_cutoff.date()})")
    print("")

    any_fail = False
    for spec in symbols:
        asset = Asset(spec.symbol, asset_type=spec.asset_type)
        try:
            df = ibkr_helper.get_price_data(
                asset=asset,
                quote=None,
                timestep="day",
                start_dt=start_dt,
                end_dt=end_dt,
                exchange=None,
                include_after_hours=True,
                source=args.source,
            )
        except Exception as exc:  # pragma: no cover - live API failures
            any_fail = True
            print(f"[FAIL] {spec.symbol}:{spec.asset_type} error: {exc.__class__.__name__}: {exc}")
            continue

        if df is None or df.empty:
            any_fail = True
            print(f"[FAIL] {spec.symbol}:{spec.asset_type} no rows returned")
            continue

        idx = pd.to_datetime(df.index)
        earliest = idx.min()
        latest = idx.max()
        years = max(0.0, (latest - earliest).days / 365.2425)
        status = "PASS" if _as_utc(earliest) <= _as_utc(min_cutoff) else "FAIL"
        if status == "FAIL":
            any_fail = True
        print(
            f"[{status}] {spec.symbol}:{spec.asset_type} rows={len(df)} "
            f"start={_format_dt(earliest)} end={_format_dt(latest)} years={years:.2f}"
        )

    return 1 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
