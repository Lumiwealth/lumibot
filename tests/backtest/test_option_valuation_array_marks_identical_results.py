"""Held option legs are valued from the stored price arrays, with identical results.

Why: in a two-year weekly call-spread backtest on cloud workers, about half of the loop was
portfolio valuation. On every bar each held leg asked the data source for its newest bars
through get_historical_prices, which builds a pandas Bars object, and the broker then copied
and re-filtered that frame, only to read the newest trade's time and close. A data source that
already keeps the contract's trades as arrays can answer that directly
(``recent_bar_arrays``); the broker uses it for valuation and keeps the old path otherwise.

Invariant under test: the trade log and every per-iteration account value, cash and option
price are exactly the same with and without the array lookup (compared in the same process),
for sparse contracts (estimated marks) and liquid ones (real marks), and the array lookup
removes most Bars objects built during the run. Counts, never seconds.
"""

from datetime import date

import pytest

import tests.backtest.test_alpaca_option_first_load_month_identical_results as fixture
from lumibot.entities import bars as bars_module


def _liquid_rows():
    rows = {"SPY": fixture.ROWS["SPY"], "SPY260821C00650000": []}
    for symbol, base in (("SPY260821C00640000", 9.0), ("SPY260821C00645000", 6.0)):
        out = []
        for i, day in enumerate(fixture._trading_days()):
            if day < date(2026, 6, 22):
                continue
            for minute in range(0, 390, 7):
                px = round(base - 0.01 * i + 0.001 * minute, 2)
                out.append((fixture._stamp(day, minute), px, px + 0.05, px - 0.05, px, 3))
        rows[symbol] = out
    return rows


def _counted_run(monkeypatch, tmp_path, fast):
    built = {"n": 0}
    original = bars_module.Bars.__init__

    def counting(self, *args, **kwargs):
        built["n"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(bars_module.Bars, "__init__", counting)
    result, _windows = fixture._run(monkeypatch, tmp_path, True, fast_marks=fast)
    monkeypatch.setattr(bars_module.Bars, "__init__", original)
    return result, built["n"]


@pytest.mark.parametrize(
    "liquid, most_bars_kept",
    # Sparse contracts often have fewer than three prints in the loaded month; those lookups
    # still take the full path (earlier prints decide the answer), so fewer Bars are saved.
    [(False, 0.8), (True, 0.6)],
    ids=["sparse_estimated_marks", "liquid_real_marks"],
)
def test_array_marks_give_identical_results_and_build_fewer_bars(
    monkeypatch, tmp_path, disable_datasource_override, liquid, most_bars_kept
):
    if liquid:
        monkeypatch.setattr(fixture, "ROWS", _liquid_rows())
    slow, slow_bars = _counted_run(monkeypatch, tmp_path / "slow", fast=False)
    fast, fast_bars = _counted_run(monkeypatch, tmp_path / "fast", fast=True)

    assert slow[2] > 0, "the fixture must fill orders"
    assert fast == slow
    # Valuation looked up each held leg on every bar; those lookups no longer build Bars.
    assert fast_bars < slow_bars * most_bars_kept, (fast_bars, slow_bars)
