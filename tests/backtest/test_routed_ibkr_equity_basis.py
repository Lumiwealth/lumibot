"""Real routed data-store updates must retain one verified split basis per stock."""

import pandas as pd
import pytest

from lumibot.backtesting.routed_backtesting import ProviderSpec, RoutedBacktestingPandas
from lumibot.entities import Asset, Data
from lumibot.tools import ibkr_helper
from lumibot.tools.ibkr_equity_basis import EquityBasisError, EquityBasisSnapshot, decorate_adjusted_frame


def basis(split=10):
    actions = pd.DataFrame({"Dividends": [0.0], "Stock Splits": [split]},
                           index=pd.DatetimeIndex(["2024-06-10"]))
    return EquityBasisSnapshot.from_verified_actions(
        actions, as_of="2024-06-12T12:00:00Z", coverage_through="2024-06-11")


def frame(day, snapshot):
    result = pd.DataFrame({"open": [100.0], "high": [101.0], "low": [99.0],
                           "close": [100.0], "volume": [10.0]},
                          index=pd.DatetimeIndex([f"2024-06-{day:02d}T14:30:00Z"]))
    return result if snapshot is None else decorate_adjusted_frame(
        result, snapshot, provider_prices_verified=True)


@pytest.fixture
def router(monkeypatch):
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test-key")
    return RoutedBacktestingPandas(
        datetime_start=pd.Timestamp("2024-06-07T00:00:00Z").to_pydatetime(),
        datetime_end=pd.Timestamp("2024-06-12T00:00:00Z").to_pydatetime(),
        market="24/7", show_progress_bar=False, log_backtest_progress_to_file=False,
        config={"backtesting_data_routing": {"default": "ibkr"}})


def update(router, symbol="NVDA", timestep="minute"):
    router._registry._adapters["ibkr"].update_pandas_data(
        asset=Asset(symbol), quote_asset=Asset("USD", "forex"), length=10,
        timestep=timestep, start_dt=pd.Timestamp("2024-06-07T00:00:00Z").to_pydatetime(),
        require_quote_data=False, require_ohlc_data=True, snapshot_only=False,
        provider_spec=ProviderSpec(provider="ibkr"))


def stored(router, symbol="NVDA", timestep="minute"):
    key, _ = router._build_dataset_keys(Asset(symbol), Asset("USD", "forex"), timestep)
    return router._data_store[key].df


def test_routed_later_fetch_receives_first_snapshot(router, monkeypatch):
    snapshot = basis()
    captured = []
    def fetch(**kwargs):
        captured.append(kwargs.get("equity_basis"))
        return frame(7 if len(captured) == 1 else 10, snapshot)
    monkeypatch.setattr(ibkr_helper, "get_price_data", fetch)
    update(router)
    update(router)
    assert captured == [None, snapshot]
    assert stored(router)["_ibkr_split_basis"].eq(snapshot.split_fingerprint).all()


@pytest.mark.parametrize("second", [basis(0.2), None])
def test_routed_changed_or_unknown_basis_cannot_mutate_memory(router, monkeypatch, second):
    outputs = iter([frame(7, basis()), frame(10, second)])
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: next(outputs))
    update(router)
    before = stored(router).copy(deep=True)
    with pytest.raises(EquityBasisError):
        update(router)
    pd.testing.assert_frame_equal(stored(router), before)


def test_routed_snapshot_shared_across_cadences_but_not_symbols(router, monkeypatch):
    captured = []
    def fetch(**kwargs):
        symbol = kwargs["asset"].symbol
        captured.append((symbol, kwargs.get("equity_basis")))
        return frame(7, basis(10 if symbol == "NVDA" else 4))
    monkeypatch.setattr(ibkr_helper, "get_price_data", fetch)
    update(router)
    update(router, timestep="15minute")
    update(router, symbol="AAPL")
    update(router, symbol="AAPL", timestep="day")
    assert captured == [("NVDA", None), ("NVDA", basis()),
                        ("AAPL", None), ("AAPL", basis(4))]


def test_routed_new_cadence_rejects_another_basis(router, monkeypatch):
    outputs = iter([frame(7, basis()), frame(10, basis(0.2))])
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: next(outputs))
    update(router)
    with pytest.raises(EquityBasisError):
        update(router, timestep="15minute")


def test_routed_tampered_ledger_rejected(router, monkeypatch):
    result = frame(7, basis())
    result.attrs["ibkr_equity_basis"]["events"] = []
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: result)
    with pytest.raises(EquityBasisError):
        update(router)


def test_routed_repair_cannot_erase_basis(router, monkeypatch):
    original = Data.repair_times_and_fill
    def corrupt(self, *args, **kwargs):
        original(self, *args, **kwargs)
        self.df = self.df.drop(columns=["_ibkr_split_basis"])
    monkeypatch.setattr(Data, "repair_times_and_fill", corrupt)
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: frame(7, basis()))
    with pytest.raises(EquityBasisError):
        update(router)


def test_routed_ledger_survives_attrs_loss(router, monkeypatch):
    original = Data.repair_times_and_fill
    def repair(self, *args, **kwargs):
        original(self, *args, **kwargs)
        self.df.attrs = {}
    monkeypatch.setattr(Data, "repair_times_and_fill", repair)
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: frame(7, basis()))
    update(router)
    assert stored(router).attrs["ibkr_equity_basis"] == basis().to_dict()


def test_routed_first_basis_requires_ledger(router, monkeypatch):
    result = frame(7, basis())
    result.attrs = {}
    monkeypatch.setattr(ibkr_helper, "get_price_data", lambda **kwargs: result)
    with pytest.raises(EquityBasisError):
        update(router)
