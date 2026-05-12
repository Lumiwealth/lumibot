from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas, RoutingProviderError
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset


def _make_ds(*, routing: dict[str, str], monkeypatch) -> RoutedBacktestingPandas:
    import lumibot.tools.thetadata_helper as thetadata_helper

    # Avoid side effects from ThetaTerminal process management in unit tests.
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    return RoutedBacktestingPandas(
        datetime_start=datetime(2025, 1, 1, tzinfo=UTC),
        datetime_end=datetime(2025, 1, 2, tzinfo=UTC),
        config={"backtesting_data_routing": routing},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )


@pytest.mark.parametrize(
    ("raw", "expected_provider"),
    [
        ("ThetaData", "thetadata"),
        ("theta data", "thetadata"),
        ("THETA_DATA", "thetadata"),
        ("ibkr", "ibkr"),
        ("Interactive_Brokers_REST", "ibkr"),
        ("polygon", "polygon"),
        ("poly", "polygon"),
        ("alpaca", "alpaca"),
        ("CCXT", "ccxt"),
    ],
)
def test_provider_alias_normalization(raw: str, expected_provider: str, monkeypatch):
    ds = _make_ds(routing={"default": raw}, monkeypatch=monkeypatch)
    spec = ds._provider_spec_for_asset(Asset(symbol="SPY", asset_type="stock"))
    assert spec.provider == expected_provider


def test_unknown_provider_raises_loudly(monkeypatch):
    with pytest.raises(RoutingProviderError):
        _make_ds(routing={"default": "does-not-exist"}, monkeypatch=monkeypatch)


@pytest.mark.parametrize("exchange_id", ["coinbase", "kraken"])
def test_ccxt_exchange_id_alias_routes_without_network(exchange_id: str, monkeypatch):
    pytest.importorskip("ccxt")

    ds = _make_ds(routing={"crypto": exchange_id, "default": "thetadata"}, monkeypatch=monkeypatch)

    calls: list[str] = []

    def fake_fetch_df(
        *,
        asset,
        quote_asset,
        ts_unit,
        start_datetime,
        end_dt,
        length,
        canonical_key,
        provider_spec,
        require_quote_data,
        require_ohlc_data,
    ):
        assert provider_spec.provider == "ccxt"
        assert provider_spec.ccxt_exchange_id == exchange_id
        calls.append(provider_spec.ccxt_exchange_id)
        idx = pd.DatetimeIndex(
            [
                datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2025, 1, 1, 0, 1, tzinfo=UTC),
            ]
        ).tz_convert("America/New_York")
        return pd.DataFrame(
            {"open": [1.0, 2.0], "high": [1.1, 2.1], "low": [0.9, 1.9], "close": [1.0, 2.0], "volume": [10, 11]},
            index=idx,
        )

    monkeypatch.setattr(ds._registry._adapters["ccxt"], "_fetch_df", fake_fetch_df)

    asset = Asset(symbol="BTC", asset_type="crypto")
    quote = Asset(symbol="USD", asset_type="forex")
    ds._update_pandas_data(
        asset, quote, length=2, timestep="minute", start_dt=datetime(2025, 1, 2, tzinfo=UTC)
    )

    assert calls == [exchange_id]
    assert ds._data_store


def test_routed_get_last_price_aligns_1d_source_timestep_to_day_for_non_theta(monkeypatch):
    ds = _make_ds(routing={"stock": "ibkr", "default": "thetadata"}, monkeypatch=monkeypatch)
    ds._timestep = "1D"

    seen: list[str] = []

    def _fake_super_get_last_price(self, asset, timestep="minute", quote=None, exchange=None, **kwargs):
        seen.append(str(timestep))
        return 123.45

    monkeypatch.setattr(ThetaDataBacktestingPandas, "get_last_price", _fake_super_get_last_price)

    price = ds.get_last_price(Asset("AAPL", asset_type="stock"))
    assert price == 123.45
    assert seen == ["day"]


def test_routed_get_quote_aligns_1d_source_timestep_to_day_for_non_theta(monkeypatch):
    ds = _make_ds(routing={"stock": "ibkr", "default": "thetadata"}, monkeypatch=monkeypatch)
    ds._timestep = "1D"

    seen: list[str] = []

    def _fake_super_get_quote(self, asset, quote=None, exchange=None, timestep="minute", **kwargs):
        seen.append(str(timestep))
        return None

    monkeypatch.setattr(ThetaDataBacktestingPandas, "get_quote", _fake_super_get_quote)

    _ = ds.get_quote(Asset("AAPL", asset_type="stock"))
    assert seen == ["day"]
