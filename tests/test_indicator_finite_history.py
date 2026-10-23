"""Finite rolling history preserves causal inputs and recursive/custom contracts."""
from types import SimpleNamespace

import numpy as np
import pandas as pd

from lumibot.entities import Asset
from lumibot.indicators import Indicators


def lazy_history():
    asset = Asset('XYZ')
    frame = pd.DataFrame({'close': np.arange(1., 81.)},
                         index=pd.date_range('2024-01-02', periods=80, freq='B', tz='UTC'))
    source = SimpleNamespace(_data_store={}, consumed=[])
    calls = []

    def history(asset, length, **kwargs):
        calls.append(length)
        result = frame.iloc[-length:].copy()
        source._data_store[(asset, None)] = SimpleNamespace(df=result)
        return SimpleNamespace(df=result)

    source.record_indicator_input = lambda **kwargs: source.consumed.append(kwargs['frame'].copy())
    strategy = SimpleNamespace(broker=SimpleNamespace(data_source=source),
                               get_datetime=lambda: frame.index[-1], get_historical_prices=history)
    return asset, frame, source, calls, Indicators(strategy)


def test_sma_fetches_its_required_window_instead_of_decades():
    asset, frame, source, calls, indicators = lazy_history()
    frame.iloc[0, 0] = np.nan  # Irrelevant old corruption must not become SMA input.
    assert indicators.sma(asset, length=20) == frame.close.iloc[-20:].mean()
    assert calls == [20]
    assert source.consumed[-1].equals(frame.iloc[-20:])


def test_larger_sma_fetches_more_than_an_earlier_small_cache():
    asset, frame, _, calls, indicators = lazy_history()
    indicators.sma(asset, length=20)
    assert indicators.sma(asset, length=50) == frame.close.iloc[-50:].mean()
    assert calls == [20, 50]
    indicators.sma(asset, length=20)
    assert calls == [20, 50]


def test_sma_offset_keeps_the_required_prior_observations():
    asset, frame, _, calls, indicators = lazy_history()
    assert indicators.sma(asset, length=20, offset=2) == frame.close.iloc[-22:-2].mean()
    assert calls == [22]


def test_recursive_and_custom_calculations_keep_existing_full_history():
    asset, frame, source, calls, indicators = lazy_history()
    indicators.ema(asset, length=20)
    assert calls == [10000]
    assert source.consumed[-1].equals(frame)
    seen = []
    indicators.custom('arbitrary', lambda df: seen.append(df.copy()) or df.close.cumsum(), asset)
    assert seen[-1].equals(frame)


def test_required_missing_value_is_preserved_for_validation_not_filled():
    asset, frame, source, _, indicators = lazy_history()
    frame.iloc[-5, 0] = np.nan
    indicators.sma(asset, length=20)
    assert source.consumed[-1].close.isna().sum() == 1


def test_cached_future_rows_do_not_satisfy_current_warmup():
    asset, frame, source, calls, indicators = lazy_history()
    indicators._strategy.get_datetime = lambda: frame.index[30]
    source._data_store[(asset, None)] = SimpleNamespace(df=frame.copy())
    assert indicators.sma(asset, length=20) == frame.close.iloc[11:31].mean()
    assert source.consumed[-1].equals(frame.iloc[11:31])
    assert calls == []
