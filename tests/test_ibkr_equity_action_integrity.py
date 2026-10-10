"""Market moves and unknown corporate actions must not become invented prices/events."""
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import ibkr_helper as h
from lumibot.tools.yahoo_helper import YahooHelper


@pytest.mark.parametrize("closes", [
    [10, 10.1, 10.2, 10.3, 20.6],
    [10, 10.1, 20.2, 10.3, 10.4],
    [10, 10.1, 5.05, 10.3, 10.4],
])
def test_daily_reader_preserves_large_moves_without_verified_correction(monkeypatch, tmp_path, closes):
    idx = pd.date_range("2026-10-05 16:00", periods=5, freq="B", tz="America/New_York")
    frame = pd.DataFrame({c: closes for c in ["open", "high", "low", "close"]}, index=idx)
    frame["volume"] = 1000.0
    monkeypatch.setattr(h, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(h, "get_backtest_cache", lambda: SimpleNamespace(ensure_local_file=lambda *a, **k: None))
    monkeypatch.setattr(h, "_read_cache_frame", lambda *a: frame.copy())
    monkeypatch.setattr(h, "_write_cache_frame", lambda *a: None)
    monkeypatch.setattr(h, "_fetch_history_between_dates", lambda **k: pd.DataFrame())
    monkeypatch.setattr(h, "_get_cached_equity_actions", lambda *a, **k: pd.DataFrame(columns=["Dividends", "Stock Splits"]))
    monkeypatch.setattr(h, "_RUNTIME_DAILY_GAP_CHECKED_WINDOWS", set())
    result = h.get_price_data(asset=Asset("SPY"), quote=Asset("USD", asset_type="forex"),
                              timestep="day", start_dt=idx[0].to_pydatetime(),
                              end_dt=idx[-1].to_pydatetime(), include_after_hours=False)
    assert list(result.close) == closes


def test_action_provider_failure_is_visible_and_not_cached_as_zero(monkeypatch):
    monkeypatch.setattr(h, "_IBKR_EQUITY_ACTIONS_CACHE", {})
    calls = []
    def history(*a, **k):
        calls.append(1)
        if len(calls) == 1:
            raise TimeoutError("provider unavailable")
        return pd.DataFrame({"Dividends": [1.25], "Stock Splits": [0.0]},
                            index=pd.DatetimeIndex(["2024-06-21"], tz="America/New_York"))
    monkeypatch.setattr(YahooHelper, "get_symbol_data", history)
    with pytest.raises(RuntimeError, match="corporate_actions_unavailable"):
        h._get_cached_equity_actions("SPY")
    assert "SPY" not in h._IBKR_EQUITY_ACTIONS_CACHE
    recovered = h._get_cached_equity_actions("SPY")
    assert recovered.iloc[0]["Dividends"] == 1.25
    assert len(calls) == 2


@pytest.mark.parametrize("payload", [None, pd.DataFrame(), pd.DataFrame({"close": [100.]})])
def test_absent_action_source_is_not_verified_no_events(monkeypatch, payload):
    monkeypatch.setattr(h, "_IBKR_EQUITY_ACTIONS_CACHE", {})
    monkeypatch.setattr(YahooHelper, "get_symbol_data", lambda *a, **k: payload)
    with pytest.raises(RuntimeError, match="corporate_actions_unavailable"):
        h._get_cached_equity_actions("SPY")
    assert not h._IBKR_EQUITY_ACTIONS_CACHE


def test_valid_actionless_history_is_cached(monkeypatch):
    monkeypatch.setattr(h, "_IBKR_EQUITY_ACTIONS_CACHE", {})
    payload = pd.DataFrame({"Dividends": [0.], "Stock Splits": [0.]},
                           index=pd.DatetimeIndex(["2024-06-21"], tz="America/New_York"))
    calls = []
    monkeypatch.setattr(YahooHelper, "get_symbol_data", lambda *a, **k: calls.append(1) or payload)
    assert h._get_cached_equity_actions("TSLA").empty
    assert h._get_cached_equity_actions("TSLA").empty
    assert len(calls) == 1


def test_routed_dividend_fallback_does_not_hide_unknown_actions(monkeypatch):
    from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
    reader = object.__new__(RoutedBacktestingPandas)
    reader._datetime = pd.Timestamp('2024-06-21', tz='America/New_York')
    reader.datetime_end = reader._datetime
    def unavailable(*a, **k):
        raise RuntimeError('corporate_actions_unavailable: SPY')
    monkeypatch.setattr(h, '_get_cached_equity_actions', unavailable)
    with pytest.raises(RuntimeError, match='corporate_actions_unavailable'):
        reader._corporate_action_dividends([Asset('SPY')])
    assert not reader._corporate_action_dividend_cache


def test_ibkr_dividend_reader_does_not_cache_failed_daily_load(monkeypatch):
    from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
    reader = object.__new__(RoutedBacktestingPandas)
    reader._datetime = pd.Timestamp('2024-06-21', tz='America/New_York')
    monkeypatch.setattr(reader, '_provider_spec_for_asset', lambda _: SimpleNamespace(provider='ibkr'))
    monkeypatch.setattr(reader, '_get_backtest_daily_corporate_action_frame', lambda *a, **k: None)
    monkeypatch.setattr(reader, '_backtest_daily_corporate_action_length', lambda: 5)
    def unavailable(*a, **k):
        raise RuntimeError('corporate_actions_unavailable: SPY')
    monkeypatch.setattr(reader, 'get_bars', unavailable)
    with pytest.raises(RuntimeError, match='corporate_actions_unavailable'):
        reader.get_yesterday_dividends([Asset('SPY')])
    assert not reader._routed_dividend_cache


@pytest.mark.parametrize('ratio,prices', [(2.0, [100., 101., 50.5]), (0.2, [10., 10., 50.])])
def test_enrichment_then_split_normalization_has_exact_repeat_dividends(monkeypatch, ratio, prices):
    idx = pd.date_range('2024-06-19 16:00', periods=3, freq='D', tz='America/New_York')
    frame = pd.DataFrame({col: prices for col in ['open', 'high', 'low', 'close']}, index=idx)
    actions = pd.DataFrame({'Dividends': [.2, 0., 0.], 'Stock Splits': [0., 0., ratio]}, index=idx.normalize())
    monkeypatch.setattr(h, '_get_cached_equity_actions', lambda *a, **k: actions)
    def read(value):
        enriched, _ = h._append_equity_corporate_actions_daily(value, Asset('SPY'))
        return h._normalize_equity_daily_prices_for_splits(enriched)[0]
    first = read(frame)
    repeat = read(first.copy())
    pd.testing.assert_frame_equal(first, repeat)
    assert first.dividend.tolist() == [.2, 0., 0.]


def test_action_history_short_of_required_session_is_unknown(monkeypatch):
    monkeypatch.setattr(h, '_IBKR_EQUITY_ACTIONS_CACHE', {})
    frame = pd.DataFrame({'Dividends': [0.], 'Stock Splits': [0.]},
                         index=pd.DatetimeIndex(['2024-06-20'], tz='America/New_York'))
    monkeypatch.setattr(YahooHelper, 'get_symbol_data', lambda *a, **k: frame)
    with pytest.raises(RuntimeError, match='corporate_actions_unavailable'):
        h._get_cached_equity_actions('SPY', pd.Timestamp('2024-06-21 16:00', tz='America/New_York').to_pydatetime())
    assert not h._IBKR_EQUITY_ACTIONS_CACHE


def test_yahoo_transient_failure_can_recover_in_same_process(monkeypatch):
    from lumibot.tools import yahoo_helper as yh
    calls = []
    frame = pd.DataFrame({'Close': [100.], 'Dividends': [0.], 'Stock Splits': [0.]},
                         index=pd.DatetimeIndex(['2024-06-21'], tz='America/New_York'))
    def history(**kwargs):
        calls.append(1)
        if len(calls) <= 3:
            raise TimeoutError('temporary source outage')
        return frame.copy()
    monkeypatch.setattr(yh.yf, 'Ticker', lambda _: SimpleNamespace(history=history))
    monkeypatch.setattr(YahooHelper, 'sleep_and_get_proxy', lambda: None)
    monkeypatch.setattr(YahooHelper, 'get_symbol_info', lambda _: {})
    monkeypatch.setattr(yh.time, 'sleep', lambda _: None)
    if hasattr(yh, 'INVALID_SYMBOLS'):
        monkeypatch.setattr(yh, 'INVALID_SYMBOLS', set())
    assert YahooHelper.download_symbol_data('SPY') is None
    recovered = YahooHelper.download_symbol_data('SPY')
    assert recovered is not None and not recovered.empty
    assert len(calls) == 4
