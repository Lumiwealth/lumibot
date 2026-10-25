"""Stream indicators live from a public crypto exchange, in plain Python.

No hosted service, no API key: CCXT reads public candles from the exchange,
:func:`~lumibot.tools.candles.choose_candle_source` uses the exchange's own
timeframe when it has one (otherwise 1 minute candles are built up with
:class:`~lumibot.tools.candles.CandleBuilder`), and every indicator gets a
forming value on each poll and a committed value when the candle closes.

Run it::

    python -m lumibot.example_indicators.live_crypto_stream kraken BTC/USD 15m

Press Ctrl+C to stop.
"""

from __future__ import annotations

import sys
import time

from lumibot.indicators import registry
from lumibot.tools.candles import CandleBuilder, choose_candle_source


def stream(exchange, symbol: str, timeframe: str, indicators: dict, *, history: int = 300, polls: int | None = None,
           poll_seconds: float = 5.0, sleep=time.sleep):
    """Yield ``(candle, values)`` for every candle update.

    ``exchange`` is a CCXT exchange object (or anything with ``timeframes``
    and ``fetch_ohlcv``). ``indicators`` maps a name to an indicator instance.
    ``values`` maps each name to its outputs. ``candle.closed`` tells you
    whether the values are final (closed candle) or live (forming).
    ``polls=None`` runs until stopped.
    """
    source_tf, build = choose_candle_source(timeframe, exchange.timeframes or {timeframe: timeframe})
    builder = CandleBuilder(timeframe, source_timeframe=source_tf)

    # 1. History: every candle except the newest one is closed.
    limit = history * (builder.timeframe // builder.source_timeframe) if build else history
    rows = exchange.fetch_ohlcv(symbol, source_tf, limit=limit)
    for row in rows[:-1]:
        for candle in builder.update(row, closed=True):
            if candle.closed:
                for indicator in indicators.values():
                    indicator.update(candle, closed=True)
    last_seen = rows[-1][0] if rows else None
    if rows:
        rows = rows[-1:]

    # 2. Live: poll the newest source candles. The last one is still forming.
    count = 0
    while polls is None or count < polls:
        for i, row in enumerate(rows):
            forming_source = i == len(rows) - 1
            for candle in builder.update(row, closed=not forming_source):
                values = {name: ind.update(candle, closed=candle.closed) for name, ind in indicators.items()}
                yield candle, values
        count += 1
        if polls is not None and count >= polls:
            break
        sleep(poll_seconds)
        fresh = exchange.fetch_ohlcv(symbol, source_tf, limit=3)
        rows = [row for row in fresh if last_seen is None or row[0] >= last_seen]
        if rows:
            last_seen = rows[-1][0]


def main(argv=None) -> None:  # pragma: no cover - manual entry point
    import ccxt

    args = list(sys.argv[1:] if argv is None else argv)
    exchange_id, symbol, timeframe = (args + ["kraken", "BTC/USD", "15m"][len(args):])[:3]
    exchange = getattr(ccxt, exchange_id)({"enableRateLimit": True})
    indicators = {"rsi": registry.create("rsi", length=14), "ema": registry.create("ema", length=20)}
    try:
        for candle, values in stream(exchange, symbol, timeframe, indicators):
            state = "closed " if candle.closed else "forming"
            print(f"{candle.time:%Y-%m-%d %H:%M} {state} close={candle.close:.2f} "
                  f"rsi={values['rsi']['rsi']} ema={values['ema']['ema']}")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":  # pragma: no cover
    main()
