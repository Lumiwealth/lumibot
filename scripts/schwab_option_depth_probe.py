#!/usr/bin/env python3
"""Read-only Schwab probe: candle fields, bid/ask, and Tesla call depth.

Places no orders. Prints bar counts, dates, and field names only.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["LUMIBOT_DISABLE_DOTENV"] = "1"
os.environ["IS_BACKTESTING"] = "false"
os.environ["TRADING_BROKER"] = "schwab"
os.environ.pop("LUMIBOT_OAUTH_REFRESH_MODE", None)
os.environ.pop("SCHWAB_TOKEN", None)
os.environ.pop("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", None)

TOKEN_PATH = REPO_ROOT / "schwab_token_probe.json"
ENV_PATH = REPO_ROOT.parent / "botspot_node" / ".env-local"


def _load_app_env() -> None:
    wanted = {"SCHWAB_APP_KEY", "SCHWAB_APP_SECRET"}
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in wanted or os.environ.get(key):
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


class _SecretLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if "access_token" in message or "refresh_token=" in message:
            record.msg = "[redacted token log]"
            record.args = ()
        return True


def _occ(root: str, expiration: date, strike: float) -> str:
    whole = int(strike)
    decimal = int(round((float(strike) - whole) * 1000))
    return (
        f"{root.ljust(6)}{expiration.strftime('%y%m%d')}C"
        f"{whole:05d}{decimal:03d}"
    )


def _candle_summary(response) -> str:
    status = getattr(response, "status_code", None)
    if status != 200:
        text = (getattr(response, "text", "") or "")[:180]
        return f"status={status} detail={text}"
    data = response.json()
    candles = data.get("candles") or []
    if not candles:
        return f"status=200 count=0 extra_keys={sorted(set(data) - {'candles'})}"
    first = candles[0]
    last = candles[-1]
    keys = sorted(first.keys())
    bid_ask = [key for key in keys if "bid" in key.lower() or "ask" in key.lower()]
    return (
        f"status=200 count={len(candles)} "
        f"first={datetime.utcfromtimestamp(first['datetime'] / 1000).date()} "
        f"last={datetime.utcfromtimestamp(last['datetime'] / 1000).date()} "
        f"keys={keys} bid_ask_keys={bid_ask}"
    )


def _quote_summary(response, symbol: str) -> str:
    status = getattr(response, "status_code", None)
    if status != 200:
        return f"status={status}"
    payload = response.json()
    row = None
    for key, value in payload.items():
        if key.replace(" ", "") == symbol.replace(" ", ""):
            row = value
            break
    if not isinstance(row, dict):
        return f"status=200 top_keys={sorted(payload)[:8]}"
    quote = row.get("quote") if isinstance(row.get("quote"), dict) else {}
    keys = sorted(quote)
    bid_ask = [key for key in keys if "bid" in key.lower() or "ask" in key.lower() or "last" in key.lower()]
    bits = []
    for key in ("bidPrice", "askPrice", "lastPrice", "mark", "closePrice"):
        if key in quote:
            bits.append(f"{key}={quote.get(key)}")
    return f"status=200 quote_keys={keys} price_fields={bid_ask} {' '.join(bits)}"


def _history(client, symbol: str, **kwargs):
    return client.get_price_history(symbol, **kwargs)


def _pick_expirations(chains: dict, root: str):
    calls = (chains or {}).get("Chains", {}).get("CALL", {})
    today = date.today()
    expirations = sorted(date.fromisoformat(exp) for exp in calls if date.fromisoformat(exp) >= today)
    if not expirations:
        return []
    year_target = today + timedelta(days=365)
    year_exp = min(expirations, key=lambda exp: abs((exp - year_target).days))
    chosen = []
    for label, exp in (("year", year_exp), ("farthest", expirations[-1])):
        strikes = calls.get(exp.isoformat()) or []
        if not strikes:
            continue
        strike = float(strikes[len(strikes) // 2])
        chosen.append((label, exp, strike, _occ(root, exp, strike)))
    return chosen


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger().addFilter(_SecretLogFilter())
    _load_app_env()
    os.environ["SCHWAB_TOKEN_PATH"] = str(TOKEN_PATH)
    from lumibot.brokers.schwab import LUMI_DEFAULT_CALLBACK, Schwab
    from lumibot.entities import Asset

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
    if broker.client is None:
        raise SystemExit("Schwab client did not initialize.")
    if broker.data_source.client is None:
        broker.data_source.set_client(broker.client)
    client = broker.client
    price = client.PriceHistory

    spy_symbol = "SPY   270917C00775000"
    print("RESULT spy_option_daily " + _candle_summary(_history(
        client,
        spy_symbol,
        period_type=price.PeriodType.YEAR,
        period=price.Period.ONE_YEAR,
        frequency_type=price.FrequencyType.DAILY,
        frequency=price.Frequency.DAILY,
    )), flush=True)
    print("RESULT spy_stock_daily " + _candle_summary(client.get_price_history_every_day("SPY")), flush=True)
    print("RESULT spy_option_quote " + _quote_summary(client.get_quotes([spy_symbol]), spy_symbol), flush=True)
    print("RESULT spy_stock_quote " + _quote_summary(client.get_quotes(["SPY"]), "SPY"), flush=True)

    for root in ("TSLA",):
        chains = broker.data_source.get_chains(Asset(root), strike_count=12)
        call_count = len((chains or {}).get("Chains", {}).get("CALL", {}))
        print(f"RESULT {root}_expirations count={call_count}", flush=True)
        for label, exp, strike, symbol in _pick_expirations(chains, root):
            print(f"RESULT {root}_{label} exp={exp.isoformat()} strike={strike} symbol={symbol.strip()}", flush=True)
            print("RESULT quote " + _quote_summary(client.get_quotes([symbol]), symbol), flush=True)
            windows = [
                ("daily_30d", dict(period_type=price.PeriodType.MONTH, period=price.Period.ONE_MONTH, frequency_type=price.FrequencyType.DAILY, frequency=price.Frequency.DAILY)),
                ("daily_1y", dict(period_type=price.PeriodType.YEAR, period=price.Period.ONE_YEAR, frequency_type=price.FrequencyType.DAILY, frequency=price.Frequency.DAILY)),
                ("daily_2y", dict(period_type=price.PeriodType.YEAR, period=price.Period.TWO_YEARS, frequency_type=price.FrequencyType.DAILY, frequency=price.Frequency.DAILY)),
                ("daily_5y", dict(period_type=price.PeriodType.YEAR, period=price.Period.FIVE_YEARS, frequency_type=price.FrequencyType.DAILY, frequency=price.Frequency.DAILY)),
                ("minute_10d", dict(period_type=price.PeriodType.DAY, period=price.Period.TEN_DAYS, frequency_type=price.FrequencyType.MINUTE, frequency=price.Frequency.EVERY_MINUTE)),
                ("minute_48d", dict(
                    start_datetime=datetime.now() - timedelta(days=48),
                    end_datetime=datetime.now(),
                    frequency_type=price.FrequencyType.MINUTE,
                    frequency=price.Frequency.EVERY_MINUTE,
                )),
            ]
            for name, kwargs in windows:
                print(f"RESULT {root}_{label}_{name} " + _candle_summary(_history(client, symbol, **kwargs)), flush=True)

    print("PROBE_DONE", flush=True)


if __name__ == "__main__":
    main()
