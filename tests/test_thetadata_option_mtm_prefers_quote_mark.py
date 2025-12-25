from datetime import date
import logging

import pytest

from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy


class _FakeQuote:
    def __init__(self, bid=None, ask=None):
        self.bid = bid
        self.ask = ask


class _FakeThetaSource(ThetaDataBacktestingPandas):
    def __init__(self, snapshot=None, quote=None, last_price=None):
        self._snapshot = snapshot
        self._quote = quote
        self._last_price = last_price
        self.get_quote_calls = 0
        self.get_last_price_calls = 0

    def get_price_snapshot(self, asset, *args, **kwargs):
        return self._snapshot

    def get_quote(self, asset, *args, **kwargs):
        self.get_quote_calls += 1
        return self._quote

    def get_last_price(self, asset, *args, **kwargs):
        self.get_last_price_calls += 1
        return self._last_price


def _make_strategy_stub():
    strat = Strategy.__new__(Strategy)
    strat.logger = logging.getLogger(__name__)
    strat.is_backtesting = True
    strat._get_sleeptime_seconds = lambda: 86400  # daily cadence
    return strat


def test_thetadata_option_mtm_uses_quote_mark_when_snapshot_missing():
    strat = _make_strategy_stub()

    option_asset = Asset(
        "ZZOPT",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 1, 17),
        strike=100.0,
        right="call",
    )

    source = _FakeThetaSource(
        snapshot=None,
        quote=_FakeQuote(bid=10.0, ask=12.0),
        last_price=5.0,  # stale last trade that must not be used for MTM
    )

    result = Strategy._get_price_from_source(strat, source, option_asset)
    assert result == pytest.approx(11.0)
    assert source.get_quote_calls == 1
    assert source.get_last_price_calls == 0


def test_thetadata_option_mtm_returns_none_instead_of_stale_last_trade():
    strat = _make_strategy_stub()

    option_asset = Asset(
        "ZZOPT",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 1, 17),
        strike=100.0,
        right="call",
    )

    source = _FakeThetaSource(
        snapshot=None,
        quote=_FakeQuote(bid=None, ask=None),  # unpriceable quote
        last_price=5.0,  # would be stale last trade
    )

    result = Strategy._get_price_from_source(strat, source, option_asset)
    assert result is None
    assert source.get_quote_calls == 1
    assert source.get_last_price_calls == 0

