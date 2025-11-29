"""
Summarise minute-level option coverage for ThetaData vs Polygon.

This script inspects the option contracts touched by the
WeeklyMomentumOptionsStrategy September 2025 runs and reports
row counts, placeholder rows, and first/last timestamps for each
datasource. Credentials must be configured via environment
variables (ThetaData username/password, Polygon API key).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import pytz

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumibot.backtesting.polygon_backtesting import PolygonDataBacktesting
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper

EST = pytz.timezone("US/Eastern")


@dataclass(frozen=True)
class OptionRequest:
    symbol: str
    expiration: date
    strike: float
    right: str = "call"  # call/put

    def to_asset(self) -> Asset:
        return Asset(
            symbol=self.symbol,
            asset_type=Asset.AssetType.OPTION,
            expiration=self.expiration,
            strike=self.strike,
            right=(Asset.OptionRight.CALL if self.right.lower() == "call" else Asset.OptionRight.PUT),
        )


def summarise_dataframe(df: pd.DataFrame) -> tuple[int, int, datetime | None, datetime | None]:
    """Return (row_count, placeholder_count, first_ts, last_ts)."""
    if df is None or df.empty:
        return 0, 0, None, None

    placeholder_count = 0
    if "missing" in df.columns:
        placeholder_count = int(df["missing"].fillna(False).astype(bool).sum())

    index = df.index if isinstance(df.index, pd.DatetimeIndex) else None
    if index is None or index.empty:
        if "datetime" in df.columns:
            index = pd.to_datetime(df["datetime"])
        elif "time" in df.columns:
            index = pd.to_datetime(df["time"])
        else:
            index = None

    if isinstance(index, pd.Series):
        index = pd.DatetimeIndex(index)

    if index is None or len(index) == 0:
        first = last = None
    else:
        if getattr(index, "tz", None) is not None:
            first = index.min().astimezone(EST)
            last = index.max().astimezone(EST)
        else:
            first = index.min().tz_localize(EST)
            last = index.max().tz_localize(EST)

    return len(df), placeholder_count, first, last


def fetch_summary(data_source, asset: Asset, timestep: str = "minute", length: int = 5000) -> dict[str, object]:
    bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep, include_after_hours=True)
    if bars is None:
        return {"rows": 0, "placeholders": 0, "first": None, "last": None}
    df = bars.df if hasattr(bars, "df") else bars
    if hasattr(df, "to_pandas"):
        df = df.to_pandas()
    rows, placeholders, first, last = summarise_dataframe(df)
    return {
        "rows": rows,
        "placeholders": placeholders,
        "first": first,
        "last": last,
    }


def fetch_theta_summary(data_source, asset: Asset, timestep: str = "minute", datastyle: str = "quote") -> dict[str, object]:
    cache_path = thetadata_helper.build_cache_filename(asset, timestep, datastyle)
    if not cache_path.exists():
        data_source.get_historical_prices(asset=asset, length=5000, timestep=timestep, include_after_hours=True)
    if not cache_path.exists():
        return {"rows": 0, "placeholders": 0, "first": None, "last": None}
    df = thetadata_helper.ensure_missing_column(pd.read_parquet(cache_path))
    rows, placeholders, first, last = summarise_dataframe(df)
    return {
        "rows": rows,
        "placeholders": placeholders,
        "first": first,
        "last": last,
    }


def main() -> None:
    contracts: Iterable[OptionRequest] = (
        OptionRequest("WDC", date(2025, 9, 19), 86),
        OptionRequest("HOOD", date(2025, 9, 26), 118),
        OptionRequest("APP", date(2025, 10, 3), 610),
        OptionRequest("APP", date(2025, 10, 10), 645),
    )

    start = EST.localize(datetime(2025, 8, 1))
    end = EST.localize(datetime(2025, 10, 15))

    theta_ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, use_quote_data=True)
    polygon_ds = PolygonDataBacktesting(datetime_start=start, datetime_end=end)
    theta_ds.datetime = end
    polygon_ds.datetime = end

    results = []
    for request in contracts:
        option_asset = request.to_asset()
        target_dt = EST.localize(datetime.combine(request.expiration, datetime.min.time()))
        theta_ds.datetime = target_dt
        polygon_ds.datetime = target_dt
        theta_stats = fetch_theta_summary(theta_ds, option_asset)
        polygon_stats = fetch_summary(polygon_ds, option_asset)
        results.append((request, theta_stats, polygon_stats))

    header = f"{'Option':35} | {'ThetaData (rows/placeholders)':32} | {'Theta first -> last':42} | {'Polygon (rows/placeholders)':32} | {'Polygon first -> last':42}"
    print(header)
    print("-" * len(header))

    for request, theta_stats, polygon_stats in results:
        label = f"{request.symbol} {request.expiration.isoformat()} {request.strike:>6.1f}{request.right.upper()[0]}"
        theta_span = ""
        if theta_stats["first"] and theta_stats["last"]:
            theta_span = f"{theta_stats['first']:%Y-%m-%d %H:%M} -> {theta_stats['last']:%Y-%m-%d %H:%M}"
        polygon_span = ""
        if polygon_stats["first"] and polygon_stats["last"]:
            polygon_span = f"{polygon_stats['first']:%Y-%m-%d %H:%M} -> {polygon_stats['last']:%Y-%m-%d %H:%M}"

        print(
            f"{label:35} | "
            f"{theta_stats['rows']:>6}/{theta_stats['placeholders']:<6}                | "
            f"{theta_span:42} | "
            f"{polygon_stats['rows']:>6}/{polygon_stats['placeholders']:<6}                | "
            f"{polygon_span:42}"
        )


if __name__ == "__main__":
    main()
