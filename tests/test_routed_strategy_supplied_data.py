"""Data a strategy supplies itself (pandas_data) wins over the routed providers for those assets.

Why: a hosted runner can set BACKTESTING_DATA_SOURCE to a routing map. LumiBot then swaps the
strategy's PandasDataBacktesting for RoutedBacktestingPandas but keeps its pandas_data. Before this
fix the router still sent every lookup for those assets to the routed provider (for stocks:
Interactive Brokers), so a strategy that loaded its own CSV minute bars got prices from the
provider (or None for symbols the provider does not know) instead of from its own files.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset, Data


def _own_minutes(symbol: str, first_price: float, step: float) -> Data:
    index = pd.date_range("2026-11-20 14:30", periods=390, freq="1min", tz="UTC")
    close = [round(first_price + step * i, 4) for i in range(len(index))]
    frame = pd.DataFrame(
        {"open": close, "high": close, "low": close, "close": close, "volume": 1000}, index=index
    )
    return Data(Asset(symbol), frame, timestep="minute")


@pytest.fixture
def routed(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_a, **_k: None)
    provider_calls = []

    def fake_get_price_data(*, asset, **_kwargs):
        provider_calls.append(asset.symbol)
        return pd.DataFrame()

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    own = {
        Asset("ZQXA"): _own_minutes("ZQXA", 7.77, 0.0001),
        Asset("ZQXB"): _own_minutes("ZQXB", 4321.0, 0.01),
    }
    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2026, 11, 20, 14, 30, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 11, 20, 21, 0, tzinfo=timezone.utc),
        pandas_data=own,
        config={"backtesting_data_routing": {"default": "ibkr", "stock": "ibkr"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    ds.load_data()
    # 9:45 AM New York = 14:45 UTC, the 16th minute of the supplied file.
    ds._datetime = datetime(2026, 11, 20, 14, 45, tzinfo=timezone.utc).astimezone(ds.tzinfo)
    return ds, provider_calls


def test_last_price_comes_from_strategy_supplied_minutes(routed):
    ds, provider_calls = routed

    price_a = ds.get_last_price(Asset("ZQXA"))
    price_b = ds.get_last_price(Asset("ZQXB"))

    # Supplied prices only: 7.77xx and 4,321.xx exist nowhere else.
    assert price_a is not None and 7.77 <= float(price_a) <= 7.7716
    assert price_b is not None and 4321.0 <= float(price_b) <= 4321.16
    assert provider_calls == []


def test_history_comes_from_strategy_supplied_minutes(routed):
    ds, provider_calls = routed

    bars = ds.get_historical_prices(Asset("ZQXA"), 3, "minute")

    assert bars is not None and len(bars.df) == 3
    assert all(7.77 <= float(value) <= 7.7716 for value in bars.df["close"])
    assert provider_calls == []


def test_assets_the_strategy_did_not_supply_still_route(routed):
    ds, provider_calls = routed

    ds._update_pandas_data(Asset("SPY"), None, 2, "day", start_dt=ds.get_datetime())

    assert provider_calls == ["SPY"]


def test_strategy_supplied_data_is_reported_as_its_own_source(routed):
    ds, _ = routed

    ds.get_last_price(Asset("ZQXA"))

    providers = {route.get("vendor") for route in ds._observed_data_routes.values()}
    assert "your_own_data" in providers
    assert "ibkr" not in providers


def test_backtest_with_router_env_fills_at_strategy_supplied_prices(monkeypatch, tmp_path):
    """End to end: the env routing map is set, the strategy runs PandasDataBacktesting on its own
    minute files, and every fill is at a price that exists only in those files."""
    import json

    import lumibot.tools.ibkr_helper as ibkr_helper
    import lumibot.tools.thetadata_helper as thetadata_helper
    from lumibot.backtesting import PandasDataBacktesting
    from lumibot.strategies import Strategy

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_a, **_k: None)
    provider_calls = []
    monkeypatch.setattr(
        ibkr_helper, "get_price_data", lambda *, asset, **_k: provider_calls.append(asset.symbol) or pd.DataFrame()
    )
    monkeypatch.setenv(
        "BACKTESTING_DATA_SOURCE", json.dumps({"default": "ibkr", "stock": "ibkr", "crypto": "ibkr"})
    )
    monkeypatch.chdir(tmp_path)

    class BuyThenFlat(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.vars.done = False

        def on_trading_iteration(self):
            now = self.get_datetime()
            if not self.vars.done and now.hour == 9 and now.minute >= 45:
                self.vars.done = True
                self.submit_order(self.create_order("ZQXA", 10, "buy"))
                self.submit_order(self.create_order("ZQXB", 1, "buy"))

    own = {
        Asset("ZQXA"): _own_minutes("ZQXA", 7.77, 0.0001),
        Asset("ZQXB"): _own_minutes("ZQXB", 4321.0, 0.01),
    }
    result, strategy = BuyThenFlat.run_backtest(
        PandasDataBacktesting,
        datetime(2026, 11, 20, 9, 30),
        datetime(2026, 11, 20, 15, 0),
        pandas_data=own,
        benchmark_asset=None,
        budget=100000,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
        return_strategy=True,
    )

    fills = {}
    for order in strategy.broker.get_all_orders():
        if getattr(order, "avg_fill_price", None):
            fills[order.asset.symbol] = float(order.avg_fill_price)
    assert set(fills) == {"ZQXA", "ZQXB"}
    assert 7.77 <= fills["ZQXA"] <= 7.7720
    assert 4321.0 <= fills["ZQXB"] <= 4321.20
    assert provider_calls == []
