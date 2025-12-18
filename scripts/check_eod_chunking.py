#!/usr/bin/env python3
"""
Utility script to verify ThetaData EOD chunking against the shared downloader.

Usage:
    DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080 \\
    DATADOWNLOADER_API_KEY=... \\
    DATADOWNLOADER_SKIP_LOCAL_START=true \\
    THETADATA_USERNAME=... \\
    THETADATA_PASSWORD=... \\
    python scripts/check_eod_chunking.py --symbol MSFT --start 2023-01-03 --end 2024-12-31
"""

import argparse
import datetime as dt
import os
import sys

import pytz

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def _parse_args():
    parser = argparse.ArgumentParser(description="Smoke-test long ThetaData EOD spans.")
    parser.add_argument("--symbol", required=True, help="Ticker symbol to download")
    parser.add_argument(
        "--asset-type",
        default="stock",
        choices=sorted(thetadata_helper.EOD_ENDPOINTS.keys()),
        help="Asset type for the symbol (default: stock)",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    return parser.parse_args()


def _parse_date(value: str) -> dt.datetime:
    return pytz.UTC.localize(dt.datetime.strptime(value, "%Y-%m-%d"))


def main():
    args = _parse_args()

    base_url = os.environ.get("DATADOWNLOADER_BASE_URL")
    api_key = os.environ.get("DATADOWNLOADER_API_KEY")

    if not base_url or not api_key:
        sys.stderr.write("Downloader base URL/API key must be set via environment variables.\n")
        sys.exit(2)

    os.environ.setdefault("DATADOWNLOADER_SKIP_LOCAL_START", "true")

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    asset_kwargs = {"asset_type": args.asset_type, "symbol": args.symbol}
    asset = Asset(**asset_kwargs)

    df = thetadata_helper.get_historical_eod_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        username=os.environ.get("THETADATA_USERNAME", ""),
        password=os.environ.get("THETADATA_PASSWORD", ""),
    )

    if df is None or df.empty:
        print("No rows returned.")
        sys.exit(1)

    print(
        f"Downloaded {len(df)} rows for {asset.symbol} "
        f"from {df.index.min()} to {df.index.max()} via {base_url}"
    )


if __name__ == "__main__":
    main()
