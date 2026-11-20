"""The smile estimate uses the expiration's contracts a data source already holds, whatever its store looks like.

An option order with no real quote and no real trade on its fill bar fills at a marked model
estimate. When enough other contracts of the expiration are already in memory, the estimate comes
from a volatility smile fitted across them, which was measured to be far closer to the real
quote midpoint than the contract's own last trade (median error about half). The broker used to
find those contracts only in a store of ``Data`` objects keyed by ``(asset, quote)``. A source that
keeps its option trade bars another way (the Alpaca source keeps plain frames under string keys,
filled by its strike searches) was never read, so its backtests always used the weaker model.

A data source can now list what it holds with ``loaded_option_minute_frames(asset)``:
``(contract, minute trade frame)`` pairs of the same underlying and expiration, never fetching.
"""

from datetime import date, datetime

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from tests.backtest import test_option_chain_estimate_fill as chain
from tests.test_alpaca_backtesting_multitimeframe_unit import _NY_TZ, _alpaca_source
from tests.test_alpaca_option_prefetch import _call, _MultiSymbolClient


class _FramesOutsideTheDataStore(PandasDataBacktesting):
    """A source whose option frames are not ``Data`` objects in ``_data_store``."""

    extra_frames = []

    def loaded_option_minute_frames(self, asset):
        return [
            (data.asset, data.df)
            for data in self.extra_frames
            if data.asset.symbol == asset.symbol and data.asset.expiration == asset.expiration
        ]


def test_broker_reads_the_chain_a_source_lists_outside_its_data_store(monkeypatch):
    truth = chain._true_price(565, "put", chain.FILL_MINUTE)
    own_trade_model = chain._true_price(565, "put", chain.FILL_MINUTE, vol=chain.STALE_VOL)
    alone = chain._fill([chain._target_data()], chain.TARGET)
    assert alone[1] == pytest.approx(own_trade_model, abs=0.02)

    _FramesOutsideTheDataStore.extra_frames = chain._put_chain()
    monkeypatch.setattr(chain, "PandasDataBacktesting", _FramesOutsideTheDataStore)
    minute, price, source = chain._fill([chain._target_data()], chain.TARGET)

    assert (minute, source) == (chain.FILL_MINUTE, "estimated")
    assert price == pytest.approx(truth, abs=0.90)
    assert abs(price - truth) < abs(price - own_trade_model) / 5


def test_broker_ignores_listed_frames_with_trades_after_the_fill_bar(monkeypatch):
    future = [
        chain._data(chain._option(k, "put"), {chain.FILL_MINUTE + 5: 3 * chain._true_price(k, "put", 30)})
        for k in chain.PUT_STRIKES
    ]
    alone = chain._fill([chain._target_data()], chain.TARGET)
    _FramesOutsideTheDataStore.extra_frames = future
    monkeypatch.setattr(chain, "PandasDataBacktesting", _FramesOutsideTheDataStore)
    assert chain._fill([chain._target_data()], chain.TARGET) == alone


def test_alpaca_lists_the_option_frames_it_already_loaded_without_fetching(monkeypatch, tmp_path):
    source = _alpaca_source(monkeypatch, tmp_path)
    source._option_client = _MultiSymbolClient()
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 4, 9, 45))
    source.prefetch_option_bars([_call(k) for k in (640, 645, 650)])
    requests = len(source._option_client.requests)

    listed = source.loaded_option_minute_frames(_call(645))

    assert len(source._option_client.requests) == requests  # nothing fetched
    strikes = sorted(float(contract.strike) for contract, _ in listed)
    assert strikes == [640.0, 645.0]  # 650 never traded: nothing to read
    for contract, frame in listed:
        assert contract.expiration == date(2026, 8, 21)
        assert isinstance(frame.index, pd.DatetimeIndex) and frame.index.tz is not None
        assert {"close", "volume"} <= set(frame.columns)
    other_expiry = _call(645)
    other_expiry.expiration = date(2026, 9, 18)
    assert source.loaded_option_minute_frames(other_expiry) == []
