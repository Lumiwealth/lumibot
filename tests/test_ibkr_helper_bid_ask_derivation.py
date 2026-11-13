from __future__ import annotations

import pandas as pd

from lumibot.tools.ibkr_helper import _derive_bid_ask_from_bid_ask_and_midpoint


def test_derives_bid_and_ask_from_bid_ask_and_midpoint_close():
    idx = pd.date_range("2025-01-01 00:00", periods=3, freq="1min", tz="America/New_York")

    bid_ask = pd.DataFrame({"close": [101.0, 201.0, 301.0]}, index=idx)  # treated as ask_close
    midpoint = pd.DataFrame({"close": [100.0, 200.0, 300.0]}, index=idx)

    derived = _derive_bid_ask_from_bid_ask_and_midpoint(bid_ask, midpoint)

    assert list(derived.columns) == ["bid", "ask"]
    assert derived.loc[idx[0], "ask"] == 101.0
    assert derived.loc[idx[0], "bid"] == 99.0
    assert derived.loc[idx[1], "bid"] == 199.0
    assert derived.loc[idx[2], "ask"] == 301.0


def test_derivation_clamps_inverted_or_invalid_spreads_to_midpoint():
    idx = pd.date_range("2025-01-01 00:00", periods=2, freq="1min", tz="America/New_York")

    # Construct an "inverted" scenario where the implied bid would exceed ask.
    bid_ask = pd.DataFrame({"close": [100.0, 100.0]}, index=idx)
    midpoint = pd.DataFrame({"close": [200.0, -1.0]}, index=idx)

    derived = _derive_bid_ask_from_bid_ask_and_midpoint(bid_ask, midpoint)

    # Row 0: bid would be 300, ask 100 -> inverted; clamp to mid=200.
    assert derived.loc[idx[0], "bid"] == 200.0
    assert derived.loc[idx[0], "ask"] == 200.0
    # Row 1: negative mid is invalid; leave NaN so callers can fall back to trade/mark close.
    assert pd.isna(derived.loc[idx[1], "bid"])
    assert pd.isna(derived.loc[idx[1], "ask"])


def _augment_with_mismatched_quote_index(monkeypatch, augment):
    """Quote bars carry a timestamp the trade bars lack (thinly traded hourly bars)."""
    import lumibot.tools.ibkr_helper as helper
    from lumibot.entities import Asset

    trade_idx = pd.DatetimeIndex(["2025-01-01 00:00", "2025-01-01 02:00"], tz="UTC")
    quote_idx = pd.DatetimeIndex(["2025-01-01 00:00", "2025-01-01 01:00", "2025-01-01 02:00"], tz="UTC")
    trades = pd.DataFrame({"open": [10.0, 12.0], "high": [10.0, 12.0], "low": [10.0, 12.0],
                           "close": [10.0, 12.0], "volume": [1.0, 1.0]}, index=trade_idx)
    sources = {
        "Bid_Ask": pd.DataFrame({"close": [10.5, 11.5, 12.5]}, index=quote_idx),
        "Midpoint": pd.DataFrame({"close": [10.0, 11.0, 12.0]}, index=quote_idx),
    }
    monkeypatch.setattr(helper, "_get_cached_bars_for_source", lambda **kw: sources[kw["source"]])
    monkeypatch.setattr(helper, "_enable_futures_bid_ask_derivation", lambda: True, raising=False)
    asset = Asset("SYN", asset_type="crypto")
    updated, changed = getattr(helper, augment)(
        df_cache=trades, asset=asset, quote=Asset("USD", asset_type="forex"), timestep="hour",
        start_dt=trade_idx[0].to_pydatetime(), end_dt=trade_idx[-1].to_pydatetime(),
        exchange=None, include_after_hours=True,
    )
    return trades, updated, changed


def test_crypto_quote_bars_with_extra_timestamps_do_not_crash_or_add_bars(monkeypatch):
    trades, updated, changed = _augment_with_mismatched_quote_index(monkeypatch, "_maybe_augment_crypto_bid_ask")
    assert changed is True
    assert updated.index.equals(trades.index)  # no invented bar at 01:00
    assert updated.loc[trades.index[0], "bid"] == 9.5
    assert updated.loc[trades.index[1], "ask"] == 12.5


def test_futures_quote_bars_with_extra_timestamps_do_not_crash_or_add_bars(monkeypatch):
    trades, updated, changed = _augment_with_mismatched_quote_index(monkeypatch, "_maybe_augment_futures_bid_ask")
    assert changed is True
    assert updated.index.equals(trades.index)
    assert updated.loc[trades.index[1], "bid"] == 11.5
