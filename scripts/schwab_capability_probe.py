#!/usr/bin/env python3
"""Read-only Schwab capability probe.

Prints status codes, bar counts, and date spans. Places no orders.
A crypto order check uses Schwab's preview endpoint only.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["LUMIBOT_DISABLE_DOTENV"] = "1"
os.environ["IS_BACKTESTING"] = "false"
os.environ["TRADING_BROKER"] = "schwab"
os.environ.pop("LUMIBOT_OAUTH_REFRESH_MODE", None)
os.environ.pop("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", None)
os.environ.pop("SCHWAB_TOKEN", None)

TOKEN_PATH = REPO_ROOT / "schwab_token.json"
RESULT_PATH = REPO_ROOT / "docs" / "investigations" / "2026-09-22_schwab-capability-probe-result.json"
ENV_PATH = REPO_ROOT.parent / "botspot_node" / ".env-local"


def _load_app_env() -> None:
    wanted = {"SCHWAB_APP_KEY", "SCHWAB_APP_SECRET"}
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key in wanted and not os.environ.get(key):
            os.environ[key] = value.strip().strip('"').strip("'")


def _detail(text: str) -> str:
    cleaned = re.sub(r"\d{5,}", "####", text or "")
    if "access_token" in cleaned or "refresh_token" in cleaned:
        return "[redacted]"
    return cleaned[:180]


def _candle_span(payload: dict) -> dict:
    candles = payload.get("candles") if isinstance(payload, dict) else None
    if not isinstance(candles, list) or not candles:
        return {"count": 0, "first": None, "last": None}
    first_ms = candles[0].get("datetime")
    last_ms = candles[-1].get("datetime")

    def _day(ms):
        if not isinstance(ms, (int, float)):
            return None
        return datetime.utcfromtimestamp(ms / 1000).date().isoformat()

    return {"count": len(candles), "first": _day(first_ms), "last": _day(last_ms)}


def _response_summary(response) -> dict:
    status = getattr(response, "status_code", None)
    if status != 200:
        return {"status": status, "count": 0, "first": None, "last": None, "detail": _detail(getattr(response, "text", "") or "")}
    try:
        data = response.json()
    except Exception as exc:
        return {"status": status, "count": 0, "first": None, "last": None, "detail": type(exc).__name__}
    span = _candle_span(data if isinstance(data, dict) else {})
    span["status"] = status
    span["detail"] = ""
    return span


def _print(name: str, payload: dict) -> None:
    print(
        f"RESULT {name} status={payload.get('status')} count={payload.get('count')} "
        f"first={payload.get('first')} last={payload.get('last')} detail={payload.get('detail', '')}",
        flush=True,
    )


def main() -> None:
    _load_app_env()
    os.environ["SCHWAB_TOKEN_PATH"] = str(TOKEN_PATH)
    from lumibot.brokers.schwab import LUMI_DEFAULT_CALLBACK, Schwab
    from lumibot.entities import Asset
    from schwab.orders.equities import equity_buy_market

    os.environ.setdefault("SCHWAB_BACKEND_CALLBACK_URL", LUMI_DEFAULT_CALLBACK)
    Schwab._launch_stream = lambda self: None
    broker = Schwab(
        config={
            "SCHWAB_TOKEN_PATH": str(TOKEN_PATH),
            "SCHWAB_ACCOUNT_NUMBER": "000",
            "SCHWAB_APP_KEY": os.environ["SCHWAB_APP_KEY"],
            "SCHWAB_APP_SECRET": os.environ["SCHWAB_APP_SECRET"],
            "SCHWAB_BACKEND_CALLBACK_URL": os.environ["SCHWAB_BACKEND_CALLBACK_URL"],
            "MARKET": "NASDAQ",
        }
    )
    if broker.data_source.client is None:
        broker.data_source.set_client(broker.client)
    client = broker.client
    data_source = broker.data_source
    results = {}

    numbers = client.get_account_numbers()
    accounts_response = client.get_accounts()
    account_rows = []
    if getattr(accounts_response, "status_code", None) == 200:
        for item in accounts_response.json() or []:
            securities = item.get("securitiesAccount") if isinstance(item, dict) else None
            if not isinstance(securities, dict):
                continue
            number = str(securities.get("accountNumber") or "")
            account_rows.append(
                {
                    "last4": number[-4:] if number else None,
                    "type": securities.get("type"),
                    "roundTrips": securities.get("roundTrips"),
                    "isDayTrader": securities.get("isDayTrader"),
                }
            )
    results["accounts"] = {
        "status": getattr(accounts_response, "status_code", None),
        "count": len(account_rows),
        "rows": account_rows,
        "number_status": getattr(numbers, "status_code", None),
        "number_count": len(numbers.json() or []) if getattr(numbers, "status_code", None) == 200 else 0,
    }
    print(
        f"RESULT accounts status={results['accounts']['status']} count={results['accounts']['count']} "
        f"types={[row.get('type') for row in account_rows]}",
        flush=True,
    )

    now = datetime.now()
    daily_windows = {
        "7d": 7,
        "1y": 365,
        "2y": 365 * 2,
        "5y": 365 * 5,
        "10y": 365 * 10,
        "20y": 365 * 20,
    }
    minute_windows = {"1d": 1, "10d": 10, "48d": 48, "90d": 90}
    half_hour_windows = {"30d": 30, "180d": 180, "365d": 365}

    spy = Asset("SPY")
    chains = data_source.get_chains(spy, strike_count=10)
    call_map = (chains or {}).get("Chains", {}).get("CALL", {})
    expirations = sorted(exp for exp in call_map if exp >= now.date().isoformat())
    expiration = expirations[0]
    strike = float(call_map[expiration][len(call_map[expiration]) // 2])
    option = Asset(
        "SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime.fromisoformat(expiration).date(),
        strike=strike,
        right="CALL",
    )
    from lumibot.data_sources.schwab_data import _schwab_option_symbol

    option_symbol = _schwab_option_symbol(option)
    print(f"RESULT option_symbol={option_symbol.strip()} expiration={expiration} strike={strike}", flush=True)
    results["option_contract"] = {"expiration": expiration, "strike": strike}

    symbols = {"SPY": "SPY", "option": option_symbol, "BTC": "BTC"}
    for label, symbol in symbols.items():
        for name, days in daily_windows.items():
            key = f"direct_daily_{label}_{name}"
            summary = _response_summary(
                client.get_price_history_every_day(symbol, start_datetime=now - timedelta(days=days), end_datetime=now)
            )
            results[key] = summary
            _print(key, summary)
        for name, days in minute_windows.items():
            key = f"direct_1min_{label}_{name}"
            summary = _response_summary(
                client.get_price_history_every_minute(symbol, start_datetime=now - timedelta(days=days), end_datetime=now)
            )
            results[key] = summary
            _print(key, summary)
        for name, days in half_hour_windows.items():
            key = f"direct_30min_{label}_{name}"
            summary = _response_summary(
                client.get_price_history_every_thirty_minutes(
                    symbol, start_datetime=now - timedelta(days=days), end_datetime=now
                )
            )
            results[key] = summary
            _print(key, summary)
        key = f"direct_60min_{label}"
        try:
            response = client.get_price_history(
                symbol,
                period_type=client.PriceHistory.PeriodType.DAY,
                period=client.PriceHistory.Period.FIVE_DAYS,
                frequency_type=client.PriceHistory.FrequencyType.MINUTE,
                frequency=60,
                start_datetime=now - timedelta(days=5),
                end_datetime=now,
            )
            summary = _response_summary(response)
        except Exception as exc:
            summary = {"status": None, "count": 0, "first": None, "last": None, "detail": type(exc).__name__}
        results[key] = summary
        _print(key, summary)
        key = f"direct_second_{label}"
        try:
            response = client.get_price_history(
                symbol,
                frequency_type="second",
                frequency=1,
                start_datetime=now - timedelta(hours=1),
                end_datetime=now,
            )
            summary = _response_summary(response)
        except Exception as exc:
            summary = {"status": None, "count": 0, "first": None, "last": None, "detail": type(exc).__name__}
        results[key] = summary
        _print(key, summary)

    lumibot_cases = [
        ("SPY", spy, 2500, "day"),
        ("SPY", spy, 100, "minute"),
        ("SPY", spy, 40, "hour"),
        ("SPY", spy, 10, "second"),
        ("option", option, 30, "day"),
        ("option", option, 30, "minute"),
        ("option", option, 20, "hour"),
        ("BTC", Asset("BTC", asset_type=Asset.AssetType.CRYPTO), 2500, "day"),
        ("BTC", Asset("BTC", asset_type=Asset.AssetType.CRYPTO), 100, "minute"),
        ("BTC", Asset("BTC", asset_type=Asset.AssetType.CRYPTO), 40, "hour"),
        ("BTCUSD", Asset("BTC/USD", asset_type=Asset.AssetType.CRYPTO), 20, "day"),
        ("ETH", Asset("ETH", asset_type=Asset.AssetType.CRYPTO), 20, "day"),
        ("future", Asset("ES", asset_type=Asset.AssetType.FUTURE), 5, "day"),
    ]
    for label, asset, length, timestep in lumibot_cases:
        key = f"lumibot_{label}_{timestep}_{length}"
        try:
            bars = data_source.get_historical_prices(asset, length, timestep)
        except Exception as exc:
            summary = {"status": None, "count": 0, "first": None, "last": None, "detail": type(exc).__name__}
        else:
            if bars is None or getattr(bars, "df", None) is None or bars.df.empty:
                summary = {"status": None, "count": 0, "first": None, "last": None, "detail": "none"}
            else:
                index = bars.df.index
                summary = {
                    "status": 200,
                    "count": len(bars.df),
                    "first": index[0].date().isoformat(),
                    "last": index[-1].date().isoformat(),
                    "detail": "",
                }
        results[key] = summary
        _print(key, summary)

    quote_assets = [
        ("SPY", spy),
        ("option", option),
        ("BTC", Asset("BTC", asset_type=Asset.AssetType.CRYPTO)),
        ("BTCUSD", Asset("BTC/USD", asset_type=Asset.AssetType.CRYPTO)),
        ("ETH", Asset("ETH", asset_type=Asset.AssetType.CRYPTO)),
    ]
    for label, asset in quote_assets:
        quote = data_source.get_quote(asset)
        price = None if quote is None else getattr(quote, "price", None)
        bid = None if quote is None else getattr(quote, "bid", None)
        ask = None if quote is None else getattr(quote, "ask", None)
        last = data_source.get_last_price(asset)
        results[f"quote_{label}"] = {"price": price, "bid": bid, "ask": ask, "last_price": last}
        print(f"RESULT quote_{label} price={price} bid={bid} ask={ask} last_price={last}", flush=True)

    for symbol in ("BTC", "ETH", "DOGE", "SOL", "LTC", "BCH"):
        response = client.get_instruments(symbol, client.Instrument.Projection.SYMBOL_SEARCH)
        status = getattr(response, "status_code", None)
        instruments = []
        if status == 200:
            payload = response.json() or {}
            raw = payload.get("instruments") if isinstance(payload, dict) else None
            if isinstance(raw, list):
                for item in raw[:5]:
                    instruments.append(
                        {
                            "symbol": item.get("symbol"),
                            "assetType": item.get("assetType"),
                            "description": (item.get("description") or "")[:80],
                        }
                    )
        results[f"instrument_{symbol}"] = {"status": status, "instruments": instruments}
        print(f"RESULT instrument_{symbol} status={status} instruments={instruments}", flush=True)

    preview = {"status": None, "detail": "no account hash", "placed": False}
    if getattr(numbers, "status_code", None) == 200 and numbers.json():
        account_hash = numbers.json()[0].get("hashValue")
        if account_hash:
            response = client.preview_order(account_hash, equity_buy_market("BTC", 1))
            preview = {
                "status": getattr(response, "status_code", None),
                "detail": _detail(getattr(response, "text", "") or ""),
                "placed": False,
            }
    results["crypto_order_preview"] = preview
    print(
        f"RESULT crypto_order_preview status={preview.get('status')} placed={preview.get('placed')} detail={preview.get('detail')}",
        flush=True,
    )

    RESULT_PATH.write_text(json.dumps(results, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"PROBE_DONE {RESULT_PATH}", flush=True)


if __name__ == "__main__":
    main()
