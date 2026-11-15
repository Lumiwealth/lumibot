"""Explicit starting-capital units through the ordinary entrypoint and synthetic bars."""

import json
from copy import deepcopy

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, ContractSpec, Data
from lumibot.strategies import Strategy


class _BudgetProbe(Strategy):
    initialized = []

    def initialize(self):
        self.set_market("24/7")
        self.sleeptime = "1M"
        self.start_cash = self.cash
        self.start_budget = self.initial_budget
        type(self).initialized.append(self)

    def on_trading_iteration(self):
        pass


@pytest.fixture(autouse=True)
def _isolated_budget_inputs(monkeypatch, tmp_path):
    monkeypatch.delenv("BACKTESTING_BUDGET", raising=False)
    monkeypatch.delenv("BACKTESTING_BUDGET_CONVERSION", raising=False)
    monkeypatch.chdir(tmp_path)
    _BudgetProbe.initialized = []


def _synthetic_data(wallet):
    dates = pd.date_range("2026-03-02 09:30", periods=3, freq="min", tz="America/New_York")
    if wallet == "BTC":
        spec = ContractSpec("BTC", "USD", "BTC", 1, True, "contracts")
        asset = Asset("BTC/USD:BTC", "crypto_future", contract_spec=spec)
        quote, prices = Asset("USD", "crypto"), [50_000, 100_000, 100_000]
    else:
        asset = Asset("EUR", "forex")
        quote, prices = Asset("USD", "forex"), [1 / .9, 2, 2]
    series = [(asset, quote, prices),
              (Asset("ETH", "crypto"), Asset("BTC", "crypto"), [.06, .12, .12]),
              (Asset("USDT", "crypto"), Asset("USD", "crypto"), [.98, 1.03, 1.03])]
    data = []
    for asset, quote, prices in series:
        frame = pd.DataFrame({key: prices for key in ("open", "high", "low", "close")}, index=dates)
        frame["volume"] = 1000
        data.append(Data(asset, frame, quote=quote, timestep="minute"))
    return dates, data


def _backtest(tmp_path, *, wallet="BTC", budget=10_000, conversion=None):
    dates, data = _synthetic_data(wallet)
    kwargs = {} if conversion is None else {"budget_conversion": conversion}
    _BudgetProbe.backtest(
        PandasDataBacktesting, dates[0], dates[-1],
        quote_asset=Asset(wallet, "forex" if wallet in {"USD", "EUR"} else "crypto"),
        budget=budget, pandas_data=data, benchmark_asset=None, risk_free_rate=0,
        analyze_backtest=False, show_plot=False, show_tearsheet=False,
        show_indicators=False, show_progress_bar=False, save_tearsheet=False,
        save_logfile=False, save_stats_file=False, **kwargs,
    )
    strategy = _BudgetProbe.initialized[-1]
    settings = tmp_path / "settings.json"
    strategy.write_backtest_settings(str(settings))
    return strategy, json.loads(settings.read_text())


def _conversion(currency="USD", wallet="BTC", rate="0.00002", **provenance):
    return {"budget_currency": currency, "quote_currency": wallet,
            "quote_per_budget": rate, **provenance}


def _assert_start_value(strategy, expected):
    assert strategy.start_cash == pytest.approx(expected, abs=1e-12)
    assert strategy.start_budget == pytest.approx(expected, abs=1e-12)


@pytest.mark.parametrize("currency,wallet,amount,rate,expected", [
    ("USD", "BTC", 10_000, "0.00002", .2),
    # A synthetic USDT/USD leg of .98, not an assumed stablecoin peg.
    ("USDT", "BTC", 10_000, "0.0000196", .196),
    ("ETH", "BTC", 2, "0.06", .12),
    ("USD", "EUR", 10_000, "0.9", 9000),
], ids=["usd-btc-start-mark", "usdt-btc-perturbed-dollar-leg", "eth-btc-spot", "usd-eur-fx"])
def test_public_backtest_initializes_declared_budget_units(tmp_path, currency, wallet, amount, rate, expected):
    supplied = _conversion(currency, wallet, rate,
        as_of="2026-03-02T09:30:00-05:00", source="synthetic starting conversion", note="test observation")
    original = deepcopy(supplied)
    strategy, settings = _backtest(tmp_path, wallet=wallet, budget=amount, conversion=supplied)

    _assert_start_value(strategy, expected)
    assert strategy.cash == pytest.approx(expected)  # Later prices cannot change starting cash.
    receipt = settings["budget_conversion"]
    assert receipt["input_amount"] == str(amount)
    assert receipt["budget_currency"] == currency and receipt["quote_currency"] == wallet
    assert receipt["quote_per_budget"] == rate
    assert float(receipt["quote_amount"]) == pytest.approx(expected)
    assert receipt["as_of"] == supplied["as_of"] and receipt["source"] == supplied["source"]
    assert receipt["note"] == supplied["note"] and receipt["time_relation"] == "at_start"
    assert supplied == original


def test_environment_budget_conversion_replaces_argument_atomically(tmp_path, monkeypatch):
    monkeypatch.setenv("BACKTESTING_BUDGET", "12k")
    environmental = _conversion("USDT", "BTC", "0.0000196", source="synthetic env rate")
    monkeypatch.setenv("BACKTESTING_BUDGET_CONVERSION", json.dumps(environmental))
    strategy, settings = _backtest(tmp_path, budget=2,
        conversion=_conversion("USD", "EUR", "12", source="unselected argument"))
    _assert_start_value(strategy, .2352)
    assert settings["budget_conversion"]["input_amount"] == "12000.0"
    assert settings["budget_conversion"]["source"] == "synthetic env rate"
    assert settings["budget_conversion"]["supplied_by"] == "environment"


@pytest.mark.parametrize("provenance", [{}, {"as_of": "around early March", "note": "approximate"}],
                         ids=["unspecified", "approximate-date"])
def test_optional_conversion_provenance_does_not_block_useful_initialization(tmp_path, provenance):
    strategy, settings = _backtest(tmp_path, conversion=_conversion(**provenance))
    _assert_start_value(strategy, .2)
    receipt = settings["budget_conversion"]
    assert receipt["source"] is None
    assert receipt["as_of"] == provenance.get("as_of")
    assert receipt["time_relation"] == "unspecified"


@pytest.mark.parametrize("wallet,budget,env_budget,expected", [
    ("USD", None, None, 100_000), ("BTC", .2, None, .2), ("BTC", .2, "5k", 5000),
], ids=["default-usd", "native-btc", "scalar-env-still-quote-units"])
def test_legacy_budget_units_and_defaults_stay_unchanged(tmp_path, monkeypatch, wallet, budget, env_budget, expected):
    if env_budget is not None:
        monkeypatch.setenv("BACKTESTING_BUDGET", env_budget)
    strategy, settings = _backtest(tmp_path, wallet=wallet, budget=budget)
    _assert_start_value(strategy, expected)
    assert "budget_conversion" not in settings


def test_unchanged_numeric_wallet_fails_recorded_start_value_contract(tmp_path):
    strategy, _ = _backtest(tmp_path, budget=10_000)
    assert strategy.start_cash == 10_000  # Valid legacy units, wrong for this declared conversion.
    with pytest.raises(AssertionError):
        _assert_start_value(strategy, .2)


def test_later_rate_is_disclosed_and_fails_start_value_contract(tmp_path):
    strategy, settings = _backtest(tmp_path, conversion=_conversion(rate="0.00001",
        as_of="2026-03-02T09:31:00-05:00", source="synthetic later observation", note="approximation"))
    _assert_start_value(strategy, .1)  # The generic API honors the caller's explicit assumption.
    assert settings["budget_conversion"]["time_relation"] == "after_start"
    with pytest.raises(AssertionError):
        _assert_start_value(strategy, .2)  # It cannot qualify the stricter recorded-start requirement.


@pytest.mark.parametrize("budget,conversion,error", [
    (10_000, _conversion(wallet="EUR"), "quote currency"),
    (10_000, _conversion(rate="0"), "positive finite"),
    (10_000, _conversion(rate="NaN"), "positive finite"),
    (None, _conversion(), "explicit budget"),
], ids=["wrong-target", "zero-rate", "nonfinite-rate", "missing-amount"])
def test_explicit_conversion_rejects_only_invalid_unit_contracts(tmp_path, budget, conversion, error):
    with pytest.raises(ValueError, match=error):
        _backtest(tmp_path, budget=budget, conversion=conversion)
    assert _BudgetProbe.initialized == []


def test_malformed_environment_conversion_is_visible(tmp_path, monkeypatch):
    monkeypatch.setenv("BACKTESTING_BUDGET_CONVERSION", "{not json}")
    with pytest.raises(ValueError, match="BACKTESTING_BUDGET_CONVERSION"):
        _backtest(tmp_path, conversion=_conversion())
