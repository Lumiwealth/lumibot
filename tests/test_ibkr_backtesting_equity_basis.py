"""Lazy IBKR series must share one immutable split basis per stock and run."""

import pandas as pd
import pytest

from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset, Data
from lumibot.tools import ibkr_helper
from lumibot.tools.ibkr_equity_basis import EquityBasisError, EquityBasisSnapshot, decorate_adjusted_frame


def basis(split=0, dividend=0):
    actions = pd.DataFrame({"Dividends": [dividend], "Stock Splits": [split]},
                           index=pd.DatetimeIndex(["2024-06-10"]))
    return EquityBasisSnapshot.from_verified_actions(actions, as_of="2024-06-12T12:00:00Z", coverage_through="2024-06-11")


def frame(day, snapshot=None):
    result = pd.DataFrame({"open": [100.0], "high": [101.0], "low": [99.0], "close": [100.0], "volume": [10.0]},
                          index=pd.DatetimeIndex([f"2024-06-{day:02d}T14:30:00Z"]))
    return result if snapshot is None else decorate_adjusted_frame(result, snapshot, provider_prices_verified=True)


def source():
    return InteractiveBrokersRESTBacktesting(
        datetime_start=pd.Timestamp("2024-06-07T00:00:00Z").to_pydatetime(),
        datetime_end=pd.Timestamp("2024-06-12T00:00:00Z").to_pydatetime(),
        market="24/7", show_progress_bar=False, log_backtest_progress_to_file=False)


def update(ds, symbol="NVDA", timestep="minute"):
    ds._update_pandas_data(Asset(symbol), Asset("USD", "forex"), timestep=timestep,
                          start_dt=pd.Timestamp("2024-06-07T00:00:00Z").to_pydatetime(),
                          end_dt=pd.Timestamp("2024-06-12T00:00:00Z").to_pydatetime(),
                          exchange="SMART", include_after_hours=True)


def stored(ds, symbol="NVDA", timestep="minute"):
    key, _ = ds._build_dataset_keys(Asset(symbol), Asset("USD", "forex"), timestep, "SMART")
    return ds._data_store[key].df


def test_later_fetch_receives_first_frozen_snapshot(monkeypatch):
    captured = []
    snapshot = basis(10)
    def fetch(**kwargs):
        captured.append(kwargs.get("equity_basis"))
        return frame(7 if len(captured) == 1 else 10, snapshot)
    monkeypatch.setattr(ibkr_helper, "get_price_data", fetch)
    ds = source()
    update(ds)
    update(ds)
    assert captured[0] is None
    assert captured[1] == snapshot
    assert stored(ds)["_ibkr_split_basis"].eq(snapshot.split_fingerprint).all()
    assert stored(ds)["_split_adjusted"].all()


@pytest.mark.parametrize("second", [basis(0.2), None])
def test_changed_or_unknown_basis_cannot_corrupt_existing_memory(monkeypatch, second):
    outputs = iter([frame(7, basis(10)), frame(10, second)])
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: next(outputs))
    ds = source()
    update(ds)
    before = stored(ds).copy(deep=True)
    with pytest.raises(EquityBasisError):
        update(ds)
    pd.testing.assert_frame_equal(stored(ds), before)


def test_one_symbol_snapshot_shared_across_timeframes(monkeypatch):
    snapshots = []
    snapshot = basis(10)
    def fetch(**kwargs):
        snapshots.append(kwargs.get("equity_basis"))
        return frame(7, snapshot)
    monkeypatch.setattr(ibkr_helper, "get_price_data", fetch)
    ds = source()
    update(ds)
    update(ds, timestep="15minute")
    assert snapshots == [None, snapshot]


def test_changed_basis_rejected_on_new_timeframe(monkeypatch):
    outputs = iter([frame(7, basis(10)), frame(10, basis(0.2))])
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: next(outputs))
    ds = source()
    update(ds)
    with pytest.raises(EquityBasisError):
        update(ds, timestep="15minute")


def test_symbols_freeze_independently(monkeypatch):
    captured = []
    def fetch(**kwargs):
        captured.append((kwargs["asset"].symbol, kwargs.get("equity_basis")))
        return frame(7, basis(10 if kwargs["asset"].symbol == "NVDA" else 4))
    monkeypatch.setattr(ibkr_helper, "get_price_data", fetch)
    ds = source()
    update(ds)
    update(ds, symbol="AAPL")
    update(ds)
    assert captured == [("NVDA", None), ("AAPL", None), ("NVDA", basis(10))]


def test_legacy_mock_frames_do_not_trigger_action_fetch(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Legacy mock frames must not trigger Yahoo requests")
    monkeypatch.setattr(ibkr_helper, "_get_cached_equity_actions", unexpected)
    def legacy(*, asset, quote, timestep, start_dt, end_dt, exchange, include_after_hours, source, history_as_of):
        return frame(7)
    monkeypatch.setattr(ibkr_helper, "get_price_data", legacy)
    ds = source()
    update(ds)
    update(ds)
    assert stored(ds)["close"].iloc[0] == 100.0


def test_tampered_event_metadata_rejected(monkeypatch):
    result = frame(7, basis(10))
    result.attrs["ibkr_equity_basis"]["events"] = []
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: result)
    with pytest.raises(EquityBasisError):
        update(source())


def test_metadata_survives_data_repair_attrs_loss(monkeypatch):
    original = Data.repair_times_and_fill
    def repair(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        self.df.attrs = {}
        return result
    monkeypatch.setattr(Data, "repair_times_and_fill", repair)
    snapshot = basis(10)
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: frame(7, snapshot))
    ds = source()
    update(ds)
    assert stored(ds).attrs["ibkr_equity_basis"] == snapshot.to_dict()
    assert stored(ds)["_ibkr_split_basis"].eq(snapshot.split_fingerprint).all()


def test_first_basis_column_without_ledger_is_rejected(monkeypatch):
    result = frame(7, basis(10))
    result.attrs = {}
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: result)
    with pytest.raises(EquityBasisError):
        update(source())


def test_missing_basis_after_data_repair_is_not_swallowed(monkeypatch):
    original = Data.repair_times_and_fill
    def corrupt(self, *args, **kwargs):
        original(self, *args, **kwargs)
        self.df = self.df.drop(columns=["_ibkr_split_basis"])
    monkeypatch.setattr(Data, "repair_times_and_fill", corrupt)
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: frame(7, basis(10)))
    with pytest.raises(EquityBasisError):
        update(source())


def test_snapshots_do_not_leak_between_backtests(monkeypatch):
    seen = []
    def fetch(**kwargs):
        seen.append(kwargs.get("equity_basis"))
        return frame(7, basis(10))
    monkeypatch.setattr(ibkr_helper, "get_price_data", fetch)
    update(source())
    update(source())
    assert seen == [None, None]
