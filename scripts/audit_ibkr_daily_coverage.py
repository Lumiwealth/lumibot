#!/usr/bin/env python3
"""Audit IBKR daily history coverage for required stock/index symbols.

Checks that each symbol has at least 10 years of daily history available.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset
from lumibot.tools import ibkr_helper


@dataclass
class CoverageRow:
    symbol: str
    asset_type: str
    rows: int
    first_dt: str | None
    last_dt: str | None
    span_days: int | None
    span_years: float | None
    pass_10y: bool
    notes: str


def _fetch_daily(symbol: str, asset_type: str, start_dt: datetime, end_dt: datetime):
    asset = Asset(symbol, asset_type=asset_type)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    include_after_hours = asset_type not in {"stock", "index"}
    return ibkr_helper.get_price_data(
        asset=asset,
        quote=quote,
        timestep="day",
        start_dt=start_dt,
        end_dt=end_dt,
        exchange=None,
        include_after_hours=include_after_hours,
    )


def _to_aware(dt: datetime) -> datetime:
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None:
        ts = ts.tz_localize(LUMIBOT_DEFAULT_PYTZ)
    else:
        ts = ts.tz_convert(LUMIBOT_DEFAULT_PYTZ)
    return ts.to_pydatetime()


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit IBKR daily coverage (>=10y)")
    parser.add_argument(
        "--symbols",
        default="TQQQ:stock,SPY:stock,QQQ:stock,AAPL:stock,XSP:index,SPX:index,VIX:index",
        help="Comma list symbol:type",
    )
    parser.add_argument("--years", type=int, default=15, help="Lookback window in calendar years")
    parser.add_argument("--out", default=None, help="Optional output JSON path")
    args = parser.parse_args()

    now = _to_aware(datetime.now())
    start = _to_aware(now - timedelta(days=max(365 * args.years, 3650)))

    rows: list[CoverageRow] = []
    for item in [x.strip() for x in args.symbols.split(",") if x.strip()]:
        if ":" not in item:
            rows.append(
                CoverageRow(
                    symbol=item,
                    asset_type="stock",
                    rows=0,
                    first_dt=None,
                    last_dt=None,
                    span_days=None,
                    span_years=None,
                    pass_10y=False,
                    notes="invalid symbol:type entry",
                )
            )
            continue

        symbol, asset_type = [x.strip() for x in item.split(":", 1)]
        try:
            df = _fetch_daily(symbol=symbol, asset_type=asset_type, start_dt=start, end_dt=now)
        except Exception as exc:
            rows.append(
                CoverageRow(
                    symbol=symbol,
                    asset_type=asset_type,
                    rows=0,
                    first_dt=None,
                    last_dt=None,
                    span_days=None,
                    span_years=None,
                    pass_10y=False,
                    notes=f"fetch error: {exc}",
                )
            )
            continue

        if df is None or df.empty:
            rows.append(
                CoverageRow(
                    symbol=symbol,
                    asset_type=asset_type,
                    rows=0,
                    first_dt=None,
                    last_dt=None,
                    span_days=None,
                    span_years=None,
                    pass_10y=False,
                    notes="no data",
                )
            )
            continue

        idx = pd.DatetimeIndex(df.index)
        if idx.tz is None:
            idx = idx.tz_localize(LUMIBOT_DEFAULT_PYTZ)
        else:
            idx = idx.tz_convert(LUMIBOT_DEFAULT_PYTZ)
        first_dt = idx.min()
        last_dt = idx.max()
        span_days = int((last_dt - first_dt).days)
        span_years = span_days / 365.25
        pass_10y = span_days >= 3652

        rows.append(
            CoverageRow(
                symbol=symbol,
                asset_type=asset_type,
                rows=int(len(df)),
                first_dt=str(first_dt),
                last_dt=str(last_dt),
                span_days=span_days,
                span_years=round(span_years, 2),
                pass_10y=pass_10y,
                notes="",
            )
        )

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "start": str(start),
        "end": str(now),
        "rows": [asdict(r) for r in rows],
        "pass_all": all(r.pass_10y for r in rows),
    }

    if args.out:
        out = Path(args.out).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"[coverage] out={out}")

    print("symbol,type,rows,first,last,span_years,pass_10y,notes")
    for r in rows:
        print(
            f"{r.symbol},{r.asset_type},{r.rows},{r.first_dt or ''},{r.last_dt or ''},"
            f"{r.span_years if r.span_years is not None else ''},{r.pass_10y},{r.notes}"
        )
    print(f"[coverage] pass_all={payload['pass_all']}")
    return 0 if payload["pass_all"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
