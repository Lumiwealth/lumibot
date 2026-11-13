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


def test_option_contracts_are_built_once_and_reused(monkeypatch):
    """Snapshots and delta picks reuse one Asset per contract instead of rebuilding it.

    Building an Asset costs about 9 microseconds; a warm 51-strike snapshot built about 115
    of them (snapshot row, delta lookup, prefetch), about a fifth of its time.
    """
    built = []
    original = Asset.__init__

    def counting(self, *args, **kwargs):
        built.append(kwargs.get("asset_type") or (args[1] if len(args) > 1 else None))
        original(self, *args, **kwargs)

    strategy = _Strategy()
    strategy.broker.data_source = _PrefetchingSource()
    helper = OptionsHelper(strategy)
    helper.get_option_chain_snapshot(Asset("SPY"), date(2026, 10, 23), "call", underlying_price=500.0)
    monkeypatch.setattr(Asset, "__init__", counting)
    rows = helper.get_option_chain_snapshot(Asset("SPY"), date(2026, 10, 23), "call", underlying_price=500.0)
    helper.find_strike_for_delta(Asset("SPY"), 500.0, 0.3, date(2026, 10, 23), "call")
    assert [r["strike"] for r in rows] == [495.0, 500.0, 505.0]
    assert [t for t in built if str(t).lower().endswith("option")] == []


# --------------------------------------------------------------------------- banded prefetch


class _WideChain(_Strategy):
    """A $1 SPY chain from 470 to 530 whose delta crosses 0.5 at ``center``."""

    def __init__(self, center=500.0, **kwargs):
        super().__init__(**kwargs)
        self.center = center
        self.priced = []

    def get_chains(self, _asset):
        strikes = [float(s) for s in range(470, 531)]
        return {"Multiplier": 100, "Exchange": "SMART",
                "Chains": {"CALL": {"2026-10-23": strikes}, "PUT": {"2026-10-23": strikes}}}

    def get_greeks(self, asset, asset_price=None, underlying_price=None, **_kwargs):
        self.priced.append(float(asset.strike))
        return {"delta": max(0.01, min(0.99, 0.5 - (float(asset.strike) - self.center) / 50.0))}


class _RecordingSource(_PrefetchingSource):
    def __init__(self):
        super().__init__()
        self.loaded = set()

    def prefetch_option_bars(self, assets, until=None):
        self.loaded.update(float(a.strike) for a in assets)
        return super().prefetch_option_bars(assets, until=until)


def _wide_pick(strategy, target=0.3):
    return OptionsHelper(strategy).find_strike_for_delta(Asset("SPY"), 500.0, target, date(2026, 10, 23), "call")


def test_a_delta_pick_loads_only_the_strikes_its_walk_can_reach():
    """51 candidate strikes; the walk prices about a dozen. Loading all 51 decoded four times
    the stored partitions a pick needs (about 1.3 s per entry on a warm 12-month condor)."""
    expected = _wide_pick(_WideChain())
    strategy = _WideChain()
    source = _RecordingSource()
    strategy.broker.data_source = source
    assert _wide_pick(strategy) == expected
    assert len(source.calls) == 1  # still one batched read when the estimate is close
    assert len(source.loaded) <= 21, sorted(source.loaded)
    assert set(strategy.priced) <= source.loaded  # nothing was priced before it was loaded


def test_a_walk_that_travels_past_its_first_band_loads_the_next_band_first():
    # Deltas centred 15 strikes away from the closed-form estimate: the walk must travel.
    expected = _wide_pick(_WideChain(center=515.0))
    strategy = _WideChain(center=515.0)
    source = _RecordingSource()
    strategy.broker.data_source = source
    assert _wide_pick(strategy) == expected
    assert 1 < len(source.calls) <= 4
    assert set(strategy.priced) <= source.loaded
    assert len(source.loaded) < 51
