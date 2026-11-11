"""trades.html is built from the same values with time labels prepared once.

plotly turns a timezone-aware time index into an array of pandas Timestamps and deep-copies
it for every trace: on a 12-month minute backtest (98,000 points, 4 lines) that was most of
trades.html's 8 s. The time labels are now converted once, by plotly's own encoder, to the
exact strings it would write, and passed as a numpy string array. The HTML must be the same
bytes (apart from plotly's random element ids).
"""

import re

import numpy as np
import pandas as pd

from lumibot.tools import indicators


def _inputs(n=3000):
    idx = pd.date_range("2026-03-06 09:30", periods=n, freq="min", tz="America/New_York")
    # A daylight-saving change and a timestamp with microseconds.
    idx = idx.append(pd.DatetimeIndex([pd.Timestamp("2026-03-09 10:00:00.123456", tz="America/New_York")]))
    value = 100_000 + np.cumsum(np.sin(np.arange(len(idx)) / 50.0))
    strategy_df = pd.DataFrame({
        "return": np.r_[0.0, np.diff(value) / value[:-1]],
        "portfolio_value": value,
        "cash_adjusted_portfolio_value": value,
        "positions": [[{"asset": "SPY", "quantity": 1.0}]] * len(idx),
        "cash": value / 2,
    }, index=idx.tz_convert("UTC"))
    benchmark_df = pd.DataFrame({"return": 0.0, "symbol_cumprod": np.linspace(1, 1.05, len(idx))},
                                index=idx.tz_convert("UTC"))
    trades_df = pd.DataFrame({
        "time": [idx[10], idx[200], idx[-1]],
        "status": ["fill", "fill", "fill"],
        "side": ["buy", "sell", "buy"],
        "asset": ["SPY", "SPY", "SPY"],
        "symbol": ["SPY", "SPY", "SPY"],
        "filled_quantity": [1, 1, 1],
        "price": [500.0, 501.0, 502.5],
        "type": ["market"] * 3,
    })
    return strategy_df, benchmark_df, trades_df


def _html(tmp_path, name):
    strategy_df, benchmark_df, trades_df = _inputs()
    out = tmp_path / f"{name}.html"
    indicators.plot_returns(strategy_df, "Strategy", benchmark_df, "SPY", plot_file_html=str(out),
                            trades_file=str(tmp_path / f"{name}.csv"), trades_df=trades_df,
                            show_plot=True, initial_budget=100_000.0)
    text = out.read_text()
    return re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "ID", text)


def test_trades_html_is_identical_with_prepared_time_labels(tmp_path, monkeypatch):
    fast = _html(tmp_path, "fast")
    monkeypatch.setattr(indicators, "_plotly_x_values", lambda index: index)
    slow = _html(tmp_path, "slow")
    assert len(fast) > 100_000
    assert fast == slow


def test_time_labels_are_a_numpy_string_array_of_plotlys_own_strings():
    idx = pd.date_range("2026-03-08 01:00", periods=5, freq="h", tz="America/New_York")
    labels = indicators._plotly_x_values(idx)
    assert isinstance(labels, np.ndarray) and labels.dtype.kind == "U"
    assert list(labels) == [t.isoformat() for t in idx]
