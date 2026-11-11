"""find_strike_for_delta asks the data source for its candidate strikes in one batch.

The chosen strike must not change: prefetching only loads the same prices earlier.
"""

from datetime import date, datetime

from tests.test_options_helper_log_volume import _Strategy

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset


class _PrefetchingSource:
    option_quote_fallback_allowed = True

    def __init__(self):
        self.calls = []

    def prefetch_option_bars(self, assets, until=None):
        self.calls.append(([float(a.strike) for a in assets], until))
        return len(assets)


def _pick(strategy):
    helper = OptionsHelper(strategy)
    return helper.find_strike_for_delta(Asset("SPY"), 500.0, 0.3, date(2026, 10, 23), "call")


def test_delta_search_prefetches_candidates_once_and_picks_the_same_strike():
    plain = _Strategy()
    expected = _pick(plain)

    batched = _Strategy()
    source = _PrefetchingSource()
    batched.broker.data_source = source
    assert _pick(batched) == expected
    assert len(source.calls) == 1
    strikes, until = source.calls[0]
    assert strikes == [495.0, 500.0, 505.0]
    assert until == datetime(2026, 10, 19, 10, 0)


def test_sources_without_prefetch_are_unaffected():
    strategy = _Strategy()
    assert not hasattr(strategy.broker.data_source, "prefetch_option_bars")
    assert _pick(strategy) is not None


def test_chain_snapshot_matches_strike_by_strike_deltas_with_one_prefetch():
    strategy = _Strategy()
    source = _PrefetchingSource()
    strategy.broker.data_source = source
    helper = OptionsHelper(strategy)
    rows = helper.get_option_chain_snapshot(Asset("SPY"), date(2026, 10, 23), "call", underlying_price=500.0)
    assert [r["strike"] for r in rows] == [495.0, 500.0, 505.0]
    assert len(source.calls) == 1

    reference = OptionsHelper(_Strategy())
    for row in rows:
        assert row["price"] == 2.5
        assert row["delta"] == reference.get_delta_for_strike(Asset("SPY"), 500.0, row["strike"], date(2026, 10, 23), "call")
        assert set(row) == {"strike", "price", "bid", "ask", "implied_volatility", "delta"}


def test_chain_snapshot_keeps_missing_prices_empty():
    strategy = _Strategy(last=None)
    rows = OptionsHelper(strategy).get_option_chain_snapshot(Asset("SPY"), date(2026, 10, 23), "put", strikes=[500])
    assert rows == [{"strike": 500.0, "price": None, "bid": None, "ask": None, "implied_volatility": None, "delta": None}]
