"""Quote source timestamps survive data adapters without becoming simulation time."""

from datetime import timedelta

import pandas as pd
import polars as pl
import pytest

from lumibot.data_sources import PandasData
from lumibot.data_sources.polars_data import PolarsData
from lumibot.entities import Asset, Data, DataPolars


def _history():
    index = pd.date_range("2024-05-01 09:30", periods=3, freq="min", tz="America/New_York")
    frame = pd.DataFrame(
        {
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 100,
            "bid": 99.5,
            "ask": 100.5,
            "last_bid_time": index - timedelta(seconds=20),
            "last_ask_time": index - timedelta(seconds=10),
        },
        index=index,
    )
    return frame


def _source(engine, frame):
    asset = Asset("SPY")
    if engine == "polars":
        data = DataPolars(asset, pl.from_pandas(frame.rename_axis("datetime").reset_index()))
        source_type = PolarsData
    else:
        data = Data(asset, frame)
        source_type = PandasData
    source = source_type(datetime_start=frame.index[0], datetime_end=frame.index[-1], pandas_data=[data])
    source.load_data()
    source._datetime = frame.index[1]
    return source, asset


@pytest.mark.parametrize("engine", ["pandas", "polars"])
@pytest.mark.parametrize("nanoseconds", [0, 123])
def test_source_quote_preserves_recorded_side_times(engine, nanoseconds, tmp_path):
    frame = _history()
    frame["last_bid_time"] += pd.Timedelta(nanoseconds=nanoseconds)
    path = tmp_path / "quotes.parquet"
    frame.to_parquet(path)
    frame = pd.read_parquet(path)
    source, asset = _source(engine, frame)

    quote = source.get_quote(asset)

    assert quote.bid_time == frame.iloc[1]["last_bid_time"]
    assert quote.ask_time == frame.iloc[1]["last_ask_time"]
    assert quote.bid_time != quote.timestamp
    assert quote.ask_time != quote.timestamp
    assert quote.quote_time is None  # No aggregate quote event exists in this input.
    assert quote.bid == 99.5
    assert quote.ask == 100.5


@pytest.mark.parametrize("engine", ["pandas", "polars"])
@pytest.mark.parametrize("missing", ["absent", "nat"])
def test_missing_source_times_stay_missing(engine, missing):
    frame = _history()
    if missing == "absent":
        frame = frame.drop(columns=["last_bid_time", "last_ask_time"])
    else:
        frame["last_bid_time"] = pd.NaT
        frame["last_ask_time"] = pd.NaT
    source, asset = _source(engine, frame)

    quote = source.get_quote(asset)

    assert quote.bid_time is None
    assert quote.ask_time is None
    assert quote.quote_time is None
    assert quote.bid == 99.5


@pytest.mark.parametrize("engine", ["pandas", "polars"])
@pytest.mark.parametrize("side", ["bid", "ask"])
@pytest.mark.parametrize("following_gap", [False, True])
def test_fresh_side_without_source_time_does_not_inherit_old_timestamp(engine, side, following_gap):
    frame = _history()
    frame.loc[frame.index[1], side] += 1
    frame.loc[frame.index[1], f"last_{side}_time"] = pd.NaT
    if following_gap:
        frame.loc[frame.index[2], side] = float("nan")
        frame.loc[frame.index[2], f"last_{side}_time"] = pd.NaT
    source, asset = _source(engine, frame)
    if following_gap:
        source._datetime = frame.index[2]

    quote = source.get_quote(asset)

    assert getattr(quote, side) == frame.iloc[1][side]
    assert getattr(quote, f"{side}_time") is None


@pytest.mark.parametrize("engine", ["pandas", "polars"])
@pytest.mark.parametrize("side", ["bid", "ask"])
def test_missing_side_carries_its_source_time_with_its_value(engine, side):
    frame = _history()
    frame.loc[frame.index[1], side] = float("nan")
    frame.loc[frame.index[1], f"last_{side}_time"] = pd.NaT
    source, asset = _source(engine, frame)

    quote = source.get_quote(asset)

    assert getattr(quote, side) == frame.iloc[0][side]
    assert getattr(quote, f"{side}_time") == frame.iloc[0][f"last_{side}_time"]


@pytest.mark.parametrize("path", ["cached", "snapshot", "daily"])
def test_thetadata_quote_preserves_recorded_side_times(monkeypatch, path):
    from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr("lumibot.tools.data_downloader_queue_client.set_queue_client_id", lambda *_a, **_k: None)
    monkeypatch.setattr("lumibot.tools.thetadata_helper.reset_theta_terminal_tracking", lambda *_a, **_k: None)
    frame = _history()
    if path == "daily":
        frame.index = pd.date_range("2024-05-01", periods=3, freq="D", tz="America/New_York")
    asset = Asset("SPY")
    data = Data(asset, frame, timestep="day" if path == "daily" else "minute")
    source = ThetaDataBacktestingPandas(datetime_start=frame.index[0], datetime_end=frame.index[-1], pandas_data=[data])
    source._datetime = frame.index[1]
    source._observed_intraday_cadence = True
    if path == "daily":
        monkeypatch.setattr(source, "_update_pandas_data", lambda *_a, **_k: None)
    else:
        monkeypatch.setattr(source, "_update_pandas_data", lambda *_a, **_k: pytest.fail("Cached quote must not fetch"))
    monkeypatch.setattr("lumibot.tools.thetadata_helper.get_historical_data_snapshot_cached", lambda *_a, **_k: frame)

    kwargs = {"snapshot_only": True} if path == "snapshot" else {"timestep": "day" if path == "daily" else "minute"}
    quote = source.get_quote(asset, **kwargs)

    assert quote.bid_time == frame.iloc[1]["last_bid_time"]
    assert quote.ask_time == frame.iloc[1]["last_ask_time"]
    assert source.get_quote(asset, **kwargs) is quote


@pytest.mark.parametrize("value", [None, float("nan"), pd.NaT, "invalid", 1714560640000])
def test_invalid_side_time_does_not_discard_valid_quote(value):
    frame = _history()
    frame["last_bid_time"] = value
    source, asset = _source("pandas", frame)

    quote = source.get_quote(asset)

    assert quote.bid == 99.5
    assert quote.ask == 100.5
    assert quote.bid_time is None
    assert quote.ask_time == frame.iloc[1]["last_ask_time"]


@pytest.mark.parametrize("unit", ["ns", "us", "ms"])
def test_source_times_keep_naive_timestamp_semantics_and_units(unit):
    frame = _history()
    frame["last_bid_time"] = frame["last_bid_time"].dt.tz_localize(None).dt.as_unit(unit)
    source, asset = _source("pandas", frame)

    quote = source.get_quote(asset)

    assert quote.bid_time == frame.iloc[1]["last_bid_time"]
    assert quote.bid_time.tzinfo is None
    assert quote.ask_time.tzinfo is not None
