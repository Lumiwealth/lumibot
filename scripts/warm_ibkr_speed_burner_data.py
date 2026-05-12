#!/usr/bin/env python3
from __future__ import annotations

"""
warm_ibkr_speed_burner_data.py

One-time cache warmer for the IBKR speed burner benchmarks.

This script intentionally DOES hit the downloader (cold run) to populate parquet cache so that
warm-cache benchmarks can be measured as queue-free and bounded.

It does not print any secrets; it relies on environment variables already configured in the shell.
"""

import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from shutil import copyfile


def _force_source_tree_imports() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))


def _lock_down_env() -> None:
    # Avoid recursive `.env` discovery (latency + accidental secrets loading).
    os.environ.setdefault("LUMIBOT_DISABLE_DOTENV", "true")
    os.environ.setdefault("IS_BACKTESTING", "true")

    # Keep benchmark artifacts/caches inside the repo so we don't write into user cache folders
    # outside `~/Documents/Development/`.
    os.environ.setdefault("LUMIBOT_CACHE_FOLDER", "tests/backtest/_ibkr_speed_burner_cache")


def _load_repo_dotenv_if_needed() -> None:
    """Best-effort local `.env` load for developer convenience.

    This script is explicitly a cache warmer and needs downloader credentials.
    We avoid LumiBot's recursive `.env` discovery (which logs) and instead load only the
    repo-local `.env` if present, without printing any values.
    """
    if (os.environ.get("DATADOWNLOADER_BASE_URL") or "").strip() and (os.environ.get("DATADOWNLOADER_API_KEY") or "").strip():
        return
    try:
        from dotenv import dotenv_values
    except Exception:
        return

    repo_root = Path(__file__).resolve().parents[1]
    env_path = repo_root / ".env"
    if not env_path.exists():
        return

    values = dotenv_values(env_path)
    for key in ("DATADOWNLOADER_BASE_URL", "DATADOWNLOADER_API_KEY", "DATADOWNLOADER_API_KEY_HEADER"):
        val = (values.get(key) or "").strip()
        if val and not (os.environ.get(key) or "").strip():
            os.environ[key] = val


def _require_env(name: str) -> str:
    val = (os.environ.get(name) or "").strip()
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def main() -> int:
    _lock_down_env()
    _force_source_tree_imports()

    # Require downloader creds. This script is explicitly for warming caches.
    _load_repo_dotenv_if_needed()
    base_url = _require_env("DATADOWNLOADER_BASE_URL").rstrip("/")
    api_key = _require_env("DATADOWNLOADER_API_KEY")
    api_key_header = (os.environ.get("DATADOWNLOADER_API_KEY_HEADER") or "X-Downloader-Key").strip() or "X-Downloader-Key"

    # Fail fast if the downloader is unreachable instead of stalling on queue waits.
    try:
        import requests

        resp = requests.get(
            f"{base_url}/healthz",
            headers={api_key_header: api_key},
            timeout=5,
        )
        resp.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"Downloader not reachable/healthy (warm step aborted): {exc}") from exc

    import lumibot.tools.ibkr_helper as ibkr_helper
    from lumibot.entities import Asset

    # Match the warm-cache benchmark window.
    # `bench_ibkr_speed_burner_warm_cache.py` uses America/New_York 09:30–19:28, which corresponds
    # to 14:30–00:28 UTC on this date.
    window_start = datetime(2025, 12, 8, 14, 30, tzinfo=UTC)
    window_end = datetime(2025, 12, 9, 0, 28, tzinfo=UTC)

    # Ensure the local conid registry exists for expired-contract discovery (this benchmark window
    # uses an expired futures contract month).
    repo_root = Path(__file__).resolve().parents[1]
    cache_root = Path((os.environ.get("LUMIBOT_CACHE_FOLDER") or "tests/backtest/_ibkr_speed_burner_cache").strip())
    conids_path = cache_root / "ibkr" / "conids.json"
    if not conids_path.exists():
        seed = repo_root / "data" / "ibkr_tws_backfill_cache_dev_v2" / "ibkr" / "conids.json"
        if seed.exists():
            conids_path.parent.mkdir(parents=True, exist_ok=True)
            copyfile(seed, conids_path)
            print("WARM seeded ibkr/conids.json from repo backfill cache (for expired-contract lookup)", flush=True)

    # IMPORTANT: IBKR futures backtesting can expand the prefetch window backward to the prior
    # session open (18:00 America/New_York). Warm that range so a subsequent warm-cache benchmark
    # does not attempt any downloader fetches.
    try:
        import pytz

        ny = pytz.timezone("America/New_York")
        local_start = window_start.astimezone(ny)
        prev_day = local_start.date() - timedelta(days=1)
        futures_minute_start = ny.localize(datetime(prev_day.year, prev_day.month, prev_day.day, 18, 0)).astimezone(UTC)
    except Exception:
        futures_minute_start = window_start - timedelta(hours=20)

    usd = Asset("USD", "forex")
    fut_mes = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)
    fut_mnq = Asset("MNQ", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=2)
    btc = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    eth = Asset("ETH", asset_type=Asset.AssetType.CRYPTO)
    sol = Asset("SOL", asset_type=Asset.AssetType.CRYPTO)

    # Pull enough history for lookbacks (minute=100, day=20) + a bit of padding.
    crypto_minute_start = window_start - timedelta(hours=4)
    # Futures daily bars are aligned to the `us_futures` session calendar (weekends/holidays), so
    # requesting extra padding is reasonable.
    futures_day_start = window_start - timedelta(days=90)
    # Crypto daily bars are currently derived from intraday history; avoid warming enormous minute
    # ranges by keeping padding small but sufficient for length=20.
    crypto_day_start = window_start - timedelta(days=30)

    assets = [
        # Futures (Trades) – sufficient for OHLC/trade-based fills.
        (fut_mes, "minute", futures_minute_start, window_end, "Trades"),
        (fut_mnq, "minute", futures_minute_start, window_end, "Trades"),
        (fut_mes, "15minute", futures_minute_start, window_end, "Trades"),
        (fut_mes, "day", futures_day_start, window_end, "Trades"),
        (fut_mnq, "day", futures_day_start, window_end, "Trades"),
        # Crypto (Trades + quote reconstruction sources for actionable bid/ask).
        (btc, "minute", crypto_minute_start, window_end, "Trades"),
        (eth, "minute", crypto_minute_start, window_end, "Trades"),
        (sol, "minute", crypto_minute_start, window_end, "Trades"),
        (btc, "minute", crypto_minute_start, window_end, "Bid_Ask"),
        (btc, "minute", crypto_minute_start, window_end, "Midpoint"),
        (eth, "minute", crypto_minute_start, window_end, "Bid_Ask"),
        (eth, "minute", crypto_minute_start, window_end, "Midpoint"),
        (sol, "minute", crypto_minute_start, window_end, "Bid_Ask"),
        (sol, "minute", crypto_minute_start, window_end, "Midpoint"),
        (btc, "day", crypto_day_start, window_end, "Trades"),
        (eth, "day", crypto_day_start, window_end, "Trades"),
        (sol, "day", crypto_day_start, window_end, "Trades"),
    ]

    for asset, timestep, start, end, source in assets:
        print(
            f"WARM start asset={getattr(asset, 'symbol', asset)} type={getattr(asset, 'asset_type', '')} timestep={timestep} source={source}",
            flush=True,
        )
        df = ibkr_helper.get_price_data(
            asset=asset,
            quote=usd,
            timestep=timestep,
            start_dt=start,
            end_dt=end,
            exchange=None,
            include_after_hours=True,
            source=source,
        )
        if df is None or df.empty:
            raise RuntimeError(f"Failed to warm {asset} timestep={timestep} source={source}: empty dataframe")
        print(f"WARM ok asset={getattr(asset, 'symbol', asset)} timestep={timestep} source={source} rows={len(df)}", flush=True)

    print("IBKR speed burner warm: OK (parquet cache populated)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
