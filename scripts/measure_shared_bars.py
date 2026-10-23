"""Measure the shared history cache against a real broker in one fresh process.

Run it twice with the same ``--run-id``: the first process fills the cache, the
second reads it back (fresh local folder, so only the remote store can help).
``--phase uncached`` reads with the cache turned off, for comparison.

Prints one JSON line. Requires broker credentials in the environment:
  alpaca: ALPACA_TEST_API_KEY / ALPACA_TEST_API_SECRET (paper)
  tradier: TRADIER_TEST_ACCESS_TOKEN / TRADIER_TEST_ACCOUNT_NUMBER (sandbox)
  kraken: none (public CCXT candles)
The remote store is whatever LUMIBOT_CACHE_* configures. The run id is mixed into
the connection scope, so measurements never read or overwrite customers' series.

Example:
  python scripts/measure_shared_bars.py --broker kraken --run-id demo1 --phase cold
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import tempfile
import time


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", required=True, choices=["alpaca", "tradier", "kraken"])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--phase", required=True, choices=["cold", "warm", "uncached"])
    args = parser.parse_args()

    folder = tempfile.mkdtemp(prefix=f"shared-bars-{args.phase}-")
    os.environ["LUMIBOT_CACHE_FOLDER"] = folder
    os.environ["LUMIBOT_SHARED_BARS"] = "false" if args.phase == "uncached" else "true"
    started_import = time.perf_counter()

    from lumibot.entities import Asset
    from lumibot.tools import shared_bar_cache

    original_scope = shared_bar_cache.credential_scope
    shared_bar_cache.credential_scope = lambda *parts: original_scope("measurement", args.run_id, *parts)
    uploaded: list[str] = []
    original_put = shared_bar_cache._remote_put

    def recording_put(client, manager, path, body):
        original_put(client, manager, path, body)
        uploaded.append(manager.remote_key_for(path))

    shared_bar_cache._remote_put = recording_put
    import_ms = (time.perf_counter() - started_import) * 1000

    quote = None
    if args.broker == "alpaca":
        from lumibot.data_sources import AlpacaData

        source = AlpacaData(
            {
                "API_KEY": os.environ["ALPACA_TEST_API_KEY"],
                "API_SECRET": os.environ["ALPACA_TEST_API_SECRET"],
                "PAPER": True,
            }
        )
        asset = Asset("SPY")
    elif args.broker == "tradier":
        from lumibot.data_sources.tradier_data import TradierData

        source = TradierData(
            account_number=os.environ["TRADIER_TEST_ACCOUNT_NUMBER"],
            access_token=os.environ["TRADIER_TEST_ACCESS_TOKEN"],
            paper=True,
        )
        asset = Asset("SPY")
    else:
        from lumibot.data_sources.ccxt_data import CcxtData

        source = CcxtData({"exchange_id": "kraken", "sandbox": False})
        asset = Asset("BTC", asset_type="crypto")
        quote = Asset("USD", asset_type="forex")

    def read(length, timestep, timeshift=None):
        began = time.perf_counter()
        bars = source.get_historical_prices(asset, length, timestep, timeshift=timeshift, quote=quote)
        wall = (time.perf_counter() - began) * 1000
        frame = bars.df if bars is not None else None
        rows = 0 if frame is None else len(frame)
        digest = None
        if frame is not None and rows:
            prices = frame[["open", "high", "low", "close"]].round(6)
            digest = hashlib.sha256(prices.to_csv().encode()).hexdigest()
        return {
            "wallMs": round(wall, 1),
            "rows": rows,
            "first": None if not rows else str(frame.index[0]),
            "last": None if not rows else str(frame.index[-1]),
            "pricesDigest": digest,
            "historyCache": dict(frame.attrs.get("historyCache", {})) if frame is not None else {},
        }

    result = {
        "broker": args.broker,
        "phase": args.phase,
        "importMs": round(import_ms, 1),
        "settledDaily": read(30, "day", dt.timedelta(days=40)),
        "liveMinute": read(300, "minute"),
    }
    flushed = shared_bar_cache.flush_shared_bar_writes(60)
    result["flush"] = flushed
    result["uploadedKeys"] = sorted(set(uploaded))
    print("SHARED_BARS_MEASUREMENT " + json.dumps(result, default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
