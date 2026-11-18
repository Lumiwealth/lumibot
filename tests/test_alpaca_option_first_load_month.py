"""The first read of an option contract loads from the lookup month, not from the backtest start.

A strategy that reads one contract at a time (a nearest-strike pick, an order leg, a fill)
used to load each contract's whole window, from the start of the backtest to its expiry. In
a two-year run a contract picked in the last month planned about 24 monthly shared-cache
partitions, almost all from before the contract existed; a warm two-year weekly-spread run
read 3,648 such partitions (about 8 KB each). The batched pick path already loads from the
month a week before the lookup and reads earlier prints only when a lookup needs them. A
single contract's first read now takes the same path. Every answer must stay identical.
"""

from datetime import date, datetime

import pandas as pd

from lumibot.entities import Asset
from tests.test_alpaca_backtesting_multitimeframe_unit import _NY_TZ, _alpaca_source
from tests.test_alpaca_option_pick_month import CLOCKS, STRIKES, _Client, _call


def _source(monkeypatch, path, *, month_first=True):
    # A long window: the contracts expire 2026-08-21, the backtest starts two years earlier.
    source = _alpaca_source(monkeypatch, path, start=datetime(2024, 8, 19), end=datetime(2026, 8, 31))
    source._option_client = _Client()
    source.OPTION_FIRST_LOAD_FROM_LOOKUP_MONTH = month_first
    return source


def _answers(source):
    out = []
    for clock in CLOCKS:
        source._datetime = _NY_TZ.localize(clock)
        out.append(tuple(source.get_last_price(_call(k)) for k in STRIKES))
        bars = source.get_historical_prices(_call(640), 2, "minute")
        out.append(None if bars is None else tuple(bars.df["close"].tolist()))
    bars = source.get_historical_prices(_call(640), 3, "minute")
    out.append(None if bars is None else tuple(bars.df["close"].tolist()))
    quote = source.get_quote(_call(645))
    out.append(quote.price)
    return out


def test_first_single_read_loads_from_the_lookup_month_with_identical_answers(monkeypatch, tmp_path):
    expected = _answers(_source(monkeypatch, tmp_path / "whole", month_first=False))
    assert expected[4][1] == 0.72  # the June print: found only by reading before the lookup month

    month = _source(monkeypatch, tmp_path / "month")
    assert _answers(month) == expected

    first_reads = {}
    for symbols, start, _end in month._option_client.windows:
        for symbol in symbols:
            first_reads.setdefault(symbol, start)
    # The first read of each contract starts at the month a week before the lookup (July),
    # never at the backtest start two years earlier.
    assert first_reads, "no option reads recorded"
    assert all(start >= pd.Timestamp("2026-07-01T00:00:00Z") for start in first_reads.values()), first_reads


def test_whole_window_reads_plan_every_month_before_the_contract_existed(monkeypatch, tmp_path):
    """The reference path still reads from the backtest start (what the new path avoids)."""
    whole = _source(monkeypatch, tmp_path / "whole", month_first=False)
    whole._datetime = _NY_TZ.localize(CLOCKS[0])
    whole.get_last_price(_call(640))
    assert min(w[1] for w in whole._option_client.windows) < pd.Timestamp("2024-09-01T00:00:00Z")


def test_stocks_keep_the_whole_window_read(monkeypatch, tmp_path):
    source = _source(monkeypatch, tmp_path / "x")
    source._datetime = _NY_TZ.localize(CLOCKS[0])
    assert source._first_load_from_lookup_month(Asset("SPY")) is False
    assert source._first_load_from_lookup_month(_call(640)) is True
