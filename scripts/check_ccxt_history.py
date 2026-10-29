"""Check that LumiBot can load public candle history from CCXT exchanges.

Uses public market data only (no API keys). For each exchange it loads the
markets, picks a BTC pair, and asks LumiBot's CCXT data source for minute and
daily history. Prints one line per check so a problem with a single exchange
is easy to spot.

    python scripts/check_ccxt_history.py                 # a default list
    python scripts/check_ccxt_history.py --all           # every CCXT exchange with fetchOHLCV
    python scripts/check_ccxt_history.py phemex kraken   # named exchanges
"""

import argparse
import os

os.environ.setdefault("LUMIBOT_SHARED_BARS", "false")

import ccxt  # noqa: E402

from lumibot.data_sources.ccxt_data import CcxtData  # noqa: E402

DEFAULT = ["phemex", "kraken", "okx", "bybit", "coinbase", "kucoin", "gate", "bitget",
           "mexc", "htx", "cryptocom", "bitfinex", "gemini", "bitstamp"]
PAIRS = ["BTC/USDT", "BTC/USD", "BTC/USDC", "BTC/USDT:USDT"]


def check(exchange_id, minute_bars, daily_bars):
    try:
        api = getattr(ccxt, exchange_id)({"enableRateLimit": True})
        if not api.has.get("fetchOHLCV"):
            return [f"{exchange_id}: no historical candles (fetchOHLCV)"]
        api.load_markets()
    except Exception as error:  # noqa: BLE001 - report and move on
        return [f"{exchange_id}: could not load markets ({type(error).__name__})"]
    symbols = [pair for pair in PAIRS if pair in api.markets][:2] or list(api.markets)[:1]
    lines = []
    for symbol in symbols:
        for timeframe, count in (("1m", minute_bars), ("1d", daily_bars)):
            source = CcxtData.__new__(CcxtData)
            source.api = api
            source._ensure_markets_loaded = lambda: None
            source.config = {"exchange_id": exchange_id}
            source.shared_bars_scope = None
            try:
                frame = source.get_barset_from_api(api, symbol, timeframe, limit=count)
                got = 0 if frame is None else len(frame)
                lines.append(f"{exchange_id} {symbol} {timeframe}: {'ok' if got >= count else 'short'} ({got})")
            except Exception as error:  # noqa: BLE001
                lines.append(f"{exchange_id} {symbol} {timeframe}: FAILED {type(error).__name__}: {str(error)[:120]}")
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("exchanges", nargs="*")
    parser.add_argument("--all", action="store_true", help="check every CCXT exchange")
    parser.add_argument("--minute-bars", type=int, default=500)
    parser.add_argument("--daily-bars", type=int, default=300)
    args = parser.parse_args()
    exchanges = ccxt.exchanges if args.all else (args.exchanges or DEFAULT)
    for exchange_id in exchanges:
        for line in check(exchange_id, args.minute_bars, args.daily_bars):
            print(line, flush=True)


if __name__ == "__main__":
    main()
