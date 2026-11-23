"""get_historical_prices_for_assets takes the same quote asset as get_historical_prices.

Its docstring showed ``quote=asset_quote`` but the method rejected the keyword,
so generated forex and crypto strategies crashed with
"got an unexpected keyword argument 'quote'" (seen in a backtest, Nov 15 2026).
"""

import logging
from types import SimpleNamespace

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy


class _RecordingSource:
    IS_BACKTESTING_DATA_SOURCE = False

    def __init__(self):
        self.calls = []

    def get_bars(self, assets, length, **kwargs):
        self.calls.append(kwargs)
        return {}


def _fake_strategy(source):
    return SimpleNamespace(
        logger=logging.getLogger("test"),
        _logged_get_historical_prices_assets=set(),
        _sanitize_user_asset=lambda asset: asset,
        broker=SimpleNamespace(data_source=source),
        is_backtesting=False,
        quote_asset=Asset("USD", asset_type="forex"),
    )


def test_quote_keyword_is_accepted_and_forwarded():
    source = _RecordingSource()
    quote = Asset("USD", asset_type="forex")
    Strategy.get_historical_prices_for_assets(
        _fake_strategy(source), [Asset("EUR", asset_type="forex")], 2, "day", quote=quote
    )
    assert source.calls[0]["quote"] is quote


def test_quote_defaults_to_none_as_before():
    source = _RecordingSource()
    Strategy.get_historical_prices_for_assets(_fake_strategy(source), ["SPY"], 2, "day")
    assert source.calls[0]["quote"] is None
