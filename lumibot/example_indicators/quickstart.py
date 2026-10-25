"""Run a custom indicator on bars, then feed it live bars.

Uses made-up prices so it runs anywhere without a network. Swap ``df`` for
any OHLCV DataFrame (a CSV, yfinance, your broker) with a time index.

    python -m lumibot.example_indicators.quickstart
"""

import numpy as np
import pandas as pd

from lumibot.example_indicators.donchian_midline import DonchianMidline


def main():
    # 1. Some daily bars (index = bar time; columns open/high/low/close/volume).
    index = pd.date_range("2025-01-01", periods=60, freq="1D", tz="UTC")
    close = 100 + np.cumsum(np.random.default_rng(0).normal(0, 1, len(index)))
    df = pd.DataFrame({"open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 1000.0}, index=index)

    # 2. Whole series at once: lists aligned with the bars, None while warming up.
    series = DonchianMidline(length=20).compute(df)
    print("last 3 midline values:", series["mid"][-3:])

    # 3. Streaming: history first, then live bars one at a time.
    dcm = DonchianMidline(length=20)
    dcm.feed(df)  # every history bar is closed
    next_time = index[-1] + pd.Timedelta(days=1)
    tick = {"time": next_time, "open": close[-1], "high": close[-1] + 5, "low": close[-1] - 1, "close": close[-1] + 4}
    print("forming:", dcm.update(tick, closed=False))  # live tick: nothing saved
    print("closed: ", dcm.update(tick, closed=True))  # bar finished: saved
    return series, dcm


if __name__ == "__main__":
    main()
