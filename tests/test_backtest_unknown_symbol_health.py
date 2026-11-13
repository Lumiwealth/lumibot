"""A backtest that never gets any data for an asset it asked about says so in data_health.

The host marks such a run failed instead of "completed with no trades". An ordinary run
that simply made no trades, with data for everything it asked about, is not affected.
Nothing is fabricated: the record only says which asset had no data, from which source, why.
"""

import json
from datetime import datetime

import numpy as np
import pandas as pd

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy

QUOTE = Asset("USD", Asset.AssetType.FOREX)


def _data():
    asset = Asset("SPY")
    index = pd.date_range("2026-08-03 09:30", "2026-08-05 16:00", freq="1min", tz="America/New_York")
    close = 100 + np.cumsum(np.random.default_rng(1).normal(0, 0.05, len(index)))
    frame = pd.DataFrame({"open": close, "high": close, "low": close, "close": close, "volume": 1.0}, index=index)
    return {(asset, QUOTE): Data(asset, frame, timestep="minute", quote=QUOTE)}


def _run(strategy_class, tmp_path, monkeypatch):
    tmp_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "none")
    _results, strategy = strategy_class.run_backtest(
        PandasDataBacktesting, datetime(2026, 8, 3), datetime(2026, 8, 5), pandas_data=_data(),
        benchmark_asset=None, show_plot=False, show_tearsheet=False, save_tearsheet=False,
        show_indicators=False, quiet_logs=True, show_progress_bar=False, budget=10_000)
    settings = tmp_path / "settings.json"
    strategy.write_backtest_settings(str(settings))
    return json.loads(settings.read_text()).get("data_health")


class _UnknownSymbol(Strategy):
    def initialize(self):
        self.sleeptime = "30M"

    def on_trading_iteration(self):
        price = self.get_last_price(Asset("LANCX"))
        if price is not None:
            self.submit_order(self.create_order(Asset("LANCX"), 1, "buy"))
        self.get_last_price(Asset("SPY"))


class _UnknownHistory(Strategy):
    def initialize(self):
        self.sleeptime = "30M"

    def on_trading_iteration(self):
        self.get_historical_prices(Asset("LANCX"), 5, "day")


class _NoTrades(Strategy):
    def initialize(self):
        self.sleeptime = "30M"

    def on_trading_iteration(self):
        self.get_last_price(Asset("SPY"))
        self.get_historical_prices(Asset("SPY"), 5, "minute")


def test_an_asset_with_no_data_for_the_whole_run_is_a_required_failure(tmp_path, monkeypatch):
    health = _run(_UnknownSymbol, tmp_path, monkeypatch)
    assert health is not None and health["required_complete"] is False
    [failure] = [f for f in health["required_failures"] if f["symbol"] == "LANCX"]
    assert failure["asset_type"] == "stock"
    assert failure["reason"] in {"symbol_not_found", "no_data_for_backtest"}
    assert failure["returned_bars"] == 0 and failure["requested_bars"] >= 1
    assert failure["retryable"] is False
    assert failure["provider"]
    assert failure["occurrences"] >= 1 and failure["first_observed_at"]
    assert not [f for f in health["required_failures"] if f["symbol"] == "SPY"]


def test_history_requests_count_too(tmp_path, monkeypatch):
    health = _run(_UnknownHistory, tmp_path, monkeypatch)
    assert health is not None and health["required_complete"] is False
    assert [f["symbol"] for f in health["required_failures"]] == ["LANCX"]


def test_a_run_with_data_and_no_trades_is_not_a_failure(tmp_path, monkeypatch):
    health = _run(_NoTrades, tmp_path, monkeypatch)
    assert health is None or (health["required_complete"] is True and not health["required_failures"])


def test_option_contracts_without_prints_are_not_failures(tmp_path, monkeypatch):
    """Strike searches probe contracts that never traded; that is normal, not a data failure."""

    class Probe(Strategy):
        def initialize(self):
            self.sleeptime = "30M"

        def on_trading_iteration(self):
            self.get_last_price(Asset("SPY", asset_type="option", expiration=datetime(2026, 8, 21).date(),
                                      strike=999, right="CALL", underlying_asset=Asset("SPY")))
            self.get_last_price(Asset("SPY"))

    health = _run(Probe, tmp_path, monkeypatch)
    assert health is None or not health["required_failures"]


def test_ibkr_reports_symbol_not_found_after_a_failed_contract_lookup(monkeypatch):
    from lumibot.tools import ibkr_helper

    monkeypatch.setattr(ibkr_helper, "_NEGATIVE_CONID_CACHE", {})
    monkeypatch.setattr(ibkr_helper, "_NEGATIVE_CONID_CACHE_LOADED", True)
    lancx = Asset("LANCX")
    assert ibkr_helper.symbol_not_found(lancx) is False
    key = ibkr_helper._conid_key(asset=lancx, quote=QUOTE, exchange=None).to_key()
    ibkr_helper._NEGATIVE_CONID_CACHE[key] = {"ts": 0, "reason": "no_conid", "message": "x"}
    assert ibkr_helper.symbol_not_found(lancx) is True
    assert ibkr_helper.symbol_not_found(Asset("SPY")) is False


def test_the_failure_names_the_provider_and_the_not_found_reason(tmp_path, monkeypatch):
    monkeypatch.setattr(PandasDataBacktesting, "symbol_not_found", lambda self, asset: asset.symbol == "LANCX",
                        raising=False)
    health = _run(_UnknownSymbol, tmp_path, monkeypatch)
    [failure] = health["required_failures"]
    assert failure["reason"] == "symbol_not_found"
    assert failure["provider"]
