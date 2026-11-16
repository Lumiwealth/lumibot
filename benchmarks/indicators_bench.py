"""Time pandas-ta-classic against fin-primitives (and LumiBot's Indicator v2 per-bar path).

    python benchmarks/indicators_bench.py --bars 100000 --repeats 7

Same synthetic OHLC series (seeded random walk, prices rounded to cents) for every engine.
Each cell is the median wall time of --repeats full-series computations. fin-primitives
runs its whole-series float64 API (``fin_primitives.fast``, 3.4.0+). The last column
checks that fin-primitives matches pandas-ta-classic on every row (identical NaN placement,
largest relative difference where both have a value).
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if not hasattr(np, "NaN"):
    np.NaN = np.nan
import fin_primitives as fp  # noqa: E402
from fin_primitives import fast  # noqa: E402
import pandas_ta_classic as ta  # noqa: E402

from lumibot.indicators import builtin  # noqa: E402
from lumibot.indicators.base import iter_bars  # noqa: E402


def make_bars(n: int, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
    high = close * (1 + np.abs(rng.normal(0, 0.003, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.003, n)))
    index = pd.date_range("2020-01-01", periods=n, freq="min", tz="UTC")
    return pd.DataFrame({"open": close, "high": high, "low": low, "close": close, "volume": 1000.0},
                        index=index).round(2)


def median_ms(fn, repeats: int) -> float:
    fn()  # warm-up
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return statistics.median(times) * 1e3


# fin-primitives' whole-series float64 API (fin_primitives.fast, 3.4.0+): one Rust pass
# over the numpy array with the GIL released, pandas-ta-classic's (TA-Lib's) seeding.
# Each call starts from the DataFrame columns, as the pandas-ta calls do.
def fp_sma(df, length=20):
    return fast.sma(df["close"].to_numpy(), length)


def fp_ema(df, length=20):
    return fast.ema(df["close"].to_numpy(), length)


def fp_rsi(df, length=14):
    return fast.rsi(df["close"].to_numpy(), length)


def fp_macd(df, fast_=12, slow=26, signal=9):
    return fast.macd(df["close"].to_numpy(), fast_, slow, signal)  # (line, signal, hist)


def fp_bbands(df, length=20, mult=2.0):
    return fast.bbands(df["close"].to_numpy(), length, mult)  # (lower, mid, upper)


def fp_atr(df, length=14, seed="talib"):
    return fast.atr(df["high"].to_numpy(), df["low"].to_numpy(), df["close"].to_numpy(), length, seed=seed)


def v2_per_bar(cls, df, **inputs):
    ind = cls(**inputs)
    return [ind.update(bar, closed=True) for bar in iter_bars(df)]


def max_rel_diff(a, b) -> float:
    a, b = np.asarray(a, float), np.asarray(b, float)
    both = ~np.isnan(a) & ~np.isnan(b)
    if (np.isnan(a) != np.isnan(b)).any():
        return float("inf")
    return float(np.max(np.abs(a[both] - b[both]) / np.maximum(np.abs(b[both]), 1e-12)))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bars", type=int, default=100_000)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--skip-v2", action="store_true", help="skip the slow per-bar Indicator v2 column")
    args = parser.parse_args()
    df = make_bars(args.bars)
    c, h, l = df["close"], df["high"], df["low"]

    cases = [
        ("sma(20)", lambda: ta.sma(c, 20), lambda: fp_sma(df, 20),
         (builtin.SMAIndicator, {"length": 20})),
        ("ema(20)", lambda: ta.ema(c, 20), lambda: fp_ema(df, 20),
         (builtin.EMAIndicator, {"length": 20})),
        ("rsi(14)", lambda: ta.rsi(c, 14), lambda: fp_rsi(df, 14),
         (builtin.RSIIndicator, {"length": 14})),
        ("macd(12,26,9)", lambda: ta.macd(c, 12, 26, 9), lambda: fp_macd(df),
         (builtin.MACDIndicator, {})),
        ("bbands(20,2)", lambda: ta.bbands(c, 20, 2.0), lambda: fp_bbands(df),
         (builtin.BollingerBandsIndicator, {"length": 20, "mult": 2.0})),
        # pandas-ta-classic ATR seeds from bar 1 (TA-Lib), as fast.atr does by default.
        ("atr(14)", lambda: ta.atr(h, l, c, 14), lambda: fp_atr(df, 14),
         (builtin.ATRIndicator, {"length": 14})),
    ]

    # Every fin-primitives column is compared with pandas-ta-classic on the same rows:
    # NaN placement must be identical (inf otherwise) and the value is the largest
    # relative difference. pandas-ta's macd columns are (line, hist, signal) and bbands'
    # are (lower, mid, upper, ...).
    parity = {
        "sma(20)": lambda: max_rel_diff(fp_sma(df, 20), ta.sma(c, 20)),
        "ema(20)": lambda: max_rel_diff(fp_ema(df, 20), ta.ema(c, 20)),
        "rsi(14)": lambda: max_rel_diff(fp_rsi(df, 14), ta.rsi(c, 14)),
        "macd(12,26,9)": lambda: max(max_rel_diff(x, ta.macd(c, 12, 26, 9).iloc[:, i])
                                     for i, x in zip((0, 2, 1), fp_macd(df))),
        "bbands(20,2)": lambda: max(max_rel_diff(x, ta.bbands(c, 20, 2.0).iloc[:, i])
                                    for i, x in enumerate(fp_bbands(df))),
        "atr(14)": lambda: max_rel_diff(fp_atr(df, 14), ta.atr(h, l, c, 14)),
    }

    try:
        import numba
        numba_note = f"numba {numba.__version__}"
    except ImportError:
        numba_note = "no numba (pandas-ta-classic's MACD loop runs in pure Python; pip install numba)"
    print(f"{args.bars:,} bars, median of {args.repeats} runs, fin-primitives {fp.__version__}, "
          f"pandas-ta-classic {getattr(ta, 'version', '?')}, {numba_note}, Python {sys.version.split()[0]}")
    print()
    print("| indicator | pandas-ta-classic (ms) | fin-primitives (ms) | fp / pandas-ta | v2 per-bar (ms) | max rel diff |")
    print("|---|---:|---:|---:|---:|---:|")
    for name, ta_fn, fp_fn, (cls, inputs) in cases:
        t_ta = median_ms(ta_fn, args.repeats)
        t_fp = median_ms(fp_fn, args.repeats)
        t_v2 = "skipped" if args.skip_v2 else f"{median_ms(lambda: v2_per_bar(cls, df, **inputs), 1):,.0f}"
        print(f"| {name} | {t_ta:,.1f} | {t_fp:,.1f} | {t_fp / t_ta:.2f}x | {t_v2} | {parity[name]():.1e} |")
    if not args.skip_v2:
        v2_atr = [r["atr"] if r["atr"] is not None else np.nan
                  for r in v2_per_bar(builtin.ATRIndicator, df, length=14)]
        print()
        print("fast.atr(seed='tradingview') vs LumiBot v2 ATR (Pine seed): max rel diff "
              f"{max_rel_diff(fp_atr(df, 14, seed='tradingview'), v2_atr):.1e}")
    lumibot_path(args)


def lumibot_path(args) -> None:
    """The same six calls through ``Indicators._compute`` (what ``self.indicators.<name>``
    runs), with the fin-primitives backend switched off and on. This includes building the
    pandas Series/DataFrame each call returns."""
    from lumibot.indicators import fast_backend
    from lumibot.indicators.indicators import Indicators

    df = make_bars(args.bars)
    accessor = Indicators.__new__(Indicators)
    calls = [("sma", {"length": 20}), ("ema", {"length": 20}), ("rsi", {"length": 14}),
             ("macd", {}), ("bbands", {"length": 20, "std": 2.0}), ("atr", {"length": 14})]
    print()
    print(f"Through Indicators._compute, {args.bars:,} bars, median of {args.repeats} runs")
    print()
    print("| call | pandas-ta-classic (ms) | fin-primitives (ms) | speedup |")
    print("|---|---:|---:|---:|")
    backend = fast_backend._fast
    for name, kwargs in calls:
        fast_backend._fast = None
        t_old = median_ms(lambda: accessor._compute(df, name, dict(kwargs), None), args.repeats)
        fast_backend._fast = backend
        t_new = median_ms(lambda: accessor._compute(df, name, dict(kwargs), None), args.repeats)
        print(f"| {name}({', '.join(f'{k}={v}' for k, v in kwargs.items())}) | {t_old:,.1f} | {t_new:,.1f} "
              f"| {t_old / t_new:.1f}x |")


if __name__ == "__main__":
    main()
