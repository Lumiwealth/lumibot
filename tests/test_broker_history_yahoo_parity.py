"""Broker daily history matches the Yahoo daily source across a stock split (live data).

NVDA split 10-for-1 on 2024-06-10. Both sources must return split-adjusted daily closes for the
same trading dates, before and after the split. Needs network access and Tradier credentials
(TRADIER_TEST_ACCESS_TOKEN and TRADIER_TEST_ACCOUNT_NUMBER, a sandbox account is enough).
"""

import os
from datetime import date, datetime

import pandas as pd
import pytest

from lumibot.entities import Asset

NY = "America/New_York"
pytestmark = pytest.mark.apitest


def _yahoo_closes(asset, when, length):
    from lumibot.backtesting import YahooDataBacktesting

    source = YahooDataBacktesting(datetime_start=datetime(2024, 5, 1), datetime_end=datetime(2024, 7, 31))
    source._datetime = pd.Timestamp(when, tz=NY).to_pydatetime()
    bars = source.get_historical_prices(asset, length, "day")
    return {ts.date(): float(close) for ts, close in bars.df["close"].items()}


def _tradier_closes(asset, when, length):
    from lumiwealth_tradier import Tradier

    from lumibot.backtesting.broker_history_backtesting import TradierHistoryBacktesting
    from lumibot.tools import broker_history

    token = os.environ.get("TRADIER_TEST_ACCESS_TOKEN") or os.environ.get("TRADIER_ACCESS_TOKEN")
    account = os.environ.get("TRADIER_TEST_ACCOUNT_NUMBER") or os.environ.get("TRADIER_ACCOUNT_NUMBER")
    if not token or not account:
        pytest.skip("Tradier credentials are not set")
    adapter = broker_history.TradierHistory(Tradier(account, token, True), scope=None)
    source = TradierHistoryBacktesting(pd.Timestamp("2024-05-01", tz=NY).to_pydatetime(),
                                       pd.Timestamp("2024-07-31", tz=NY).to_pydatetime(), adapter=adapter)
    source._datetime = pd.Timestamp(when, tz=NY).to_pydatetime()
    bars = source.get_historical_prices(asset, length, "day")
    return {ts.date(): float(close) for ts, close in bars.df["close"].items()}


def test_tradier_daily_closes_match_yahoo_across_the_nvda_split():
    asset = Asset("NVDA", "stock")
    when = "2024-06-28 15:00"
    tradier = _tradier_closes(asset, when, 25)
    yahoo = _yahoo_closes(asset, when, 25)
    common = sorted(set(tradier) & set(yahoo))
    assert len(common) >= 20, (sorted(tradier), sorted(yahoo))
    assert set(tradier) == set(yahoo)  # same trading dates
    assert min(common) < date(2024, 6, 10) < max(common)  # the window spans the split
    for day in common:
        assert tradier[day] == pytest.approx(yahoo[day], rel=0.005), (day, tradier[day], yahoo[day])
