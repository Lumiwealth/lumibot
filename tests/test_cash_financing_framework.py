from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Position
from lumibot.strategies._strategy import _Strategy
from lumibot.strategies.strategy import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor
from lumibot.tools.indicators import create_tearsheet
from tests.fixtures import pandas_data_fixture


class _CashFrameworkStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        pass


@pytest.fixture
def strategy():
    data_source = PandasDataBacktesting(
        datetime_start=datetime(2025, 1, 2),
        datetime_end=datetime(2025, 1, 20),
    )
    broker = BacktestingBroker(data_source=data_source)
    return _CashFrameworkStrategy(broker=broker, budget=100_000, name="CashFrameworkStrategy")


def test_cash_mutation_api_updates_cash_and_ledger_totals(strategy):
    starting_cash = strategy.cash

    updated_cash = strategy.withdraw_cash(1_500, reason="monthly_income")
    assert updated_cash == pytest.approx(starting_cash - 1_500)
    assert strategy._cash_withdrawals_total == pytest.approx(1_500)

    updated_cash = strategy.deposit_cash(250, reason="rebate")
    assert updated_cash == pytest.approx(starting_cash - 1_250)
    assert strategy._cash_deposits_total == pytest.approx(250)

    updated_cash = strategy.adjust_cash(-100, reason="manual_adjustment")
    assert updated_cash == pytest.approx(starting_cash - 1_350)
    assert strategy._cash_adjustments_net_total == pytest.approx(-1_350)

    events = strategy.broker._trade_event_log_df
    cash_events = events.loc[events["event_kind"] == "cash_event"]
    assert cash_events["cash_event_type"].tolist() == ["withdrawal", "deposit", "adjustment"]
    assert cash_events["cash_event_reason"].tolist() == ["monthly_income", "rebate", "manual_adjustment"]


def test_backtest_dividends_emit_cash_events(strategy, monkeypatch):
    asset = Asset("SPY")
    strategy.broker._filled_positions.append(Position(strategy._name, asset, 10))
    strategy.broker._update_datetime(timedelta(days=1))
    starting_cash = strategy.cash

    monkeypatch.setattr(strategy, "get_yesterday_dividends", lambda assets: {asset: 1.25})

    updated_cash = strategy._update_cash_with_dividends()

    assert updated_cash == pytest.approx(starting_cash + 12.5)

    cash_events = strategy.broker._trade_event_log_df.loc[
        strategy.broker._trade_event_log_df["event_kind"] == "cash_event"
    ]
    assert not cash_events.empty
    dividend_event = cash_events.iloc[-1]
    assert dividend_event["cash_event_type"] == "dividend"
    assert float(dividend_event["cash_event_amount"]) == pytest.approx(12.5)
    assert dividend_event["cash_event_raw_type"] == "dividend"
    assert dividend_event["cash_event_raw_subtype"] == "SPY"
    assert "SPY dividend" in str(dividend_event["cash_event_description"])

    strategy._update_cash_with_dividends()
    cash_events_after_repeat = strategy.broker._trade_event_log_df.loc[
        strategy.broker._trade_event_log_df["event_kind"] == "cash_event"
    ]
    assert len(cash_events_after_repeat) == len(cash_events)


def test_cash_account_mode_blocks_negative_withdrawals(strategy):
    strategy.configure_cash_financing(account_mode="cash")

    with pytest.raises(ValueError, match="account_mode='cash'"):
        strategy.withdraw_cash(strategy.cash + 1.0, reason="oversized_withdrawal")


def test_set_cash_financing_rates_supports_one_sided_updates_and_none(strategy):
    strategy.configure_cash_financing()
    strategy.set_cash_financing_rates(credit_rate_annual=0.03, debit_rate_annual=0.09)

    strategy.set_cash_financing_rates(debit_rate_annual=0.11)
    assert strategy._cash_financing_credit_rate_annual == pytest.approx(0.03)
    assert strategy._cash_financing_debit_rate_annual == pytest.approx(0.11)

    strategy.set_cash_financing_rates(credit_rate_annual=None, debit_rate_annual=None)
    assert strategy._cash_financing_credit_rate_annual == pytest.approx(0.03)
    assert strategy._cash_financing_debit_rate_annual == pytest.approx(0.11)


def test_set_cash_financing_rates_zero_explicitly_overwrites_prior_rate(strategy):
    strategy.configure_cash_financing()
    strategy.set_cash_financing_rates(credit_rate_annual=0.03, debit_rate_annual=0.09)

    strategy.set_cash_financing_rates(credit_rate_annual=0.0)

    assert strategy._cash_financing_credit_rate_annual == pytest.approx(0.0)
    assert strategy._cash_financing_last_valid_credit_rate_annual == pytest.approx(0.0)
    assert strategy._cash_financing_debit_rate_annual == pytest.approx(0.09)


def test_strategy_public_api_no_longer_exposes_cash_financing_hook():
    assert not hasattr(Strategy, "cash_financing_rates")


def test_daily_financing_carry_forward_rates_across_missing_days(strategy):
    strategy.configure_cash_financing(
        enabled=True,
        account_mode="margin",
        day_count_basis=360,
        missing_rate_policy="carry_forward",
    )
    strategy.set_cash_financing_rates(credit_rate_annual=0.036, debit_rate_annual=0.09)

    day_rate = 0.036 / 360.0
    expected_cash = strategy.cash * (1.0 + day_rate)
    strategy._apply_daily_cash_financing_if_needed()
    assert strategy.cash == pytest.approx(expected_cash)

    # Simulate no fresh rates for later dates; framework should carry forward.
    strategy._cash_financing_credit_rate_annual = None
    strategy._cash_financing_debit_rate_annual = None
    strategy.broker._update_datetime(3 * 24 * 60 * 60)

    expected_cash *= (1.0 + day_rate) ** 3
    strategy._apply_daily_cash_financing_if_needed()
    assert strategy.cash == pytest.approx(expected_cash)
    assert strategy._cash_financing_days_accrued == 4


def test_negative_cash_accrues_debit_interest(strategy):
    strategy._set_cash_position(-10_000.0)
    strategy.configure_cash_financing(
        enabled=True,
        account_mode="margin",
        day_count_basis=360,
        missing_rate_policy="carry_forward",
    )
    strategy.set_cash_financing_rates(credit_rate_annual=0.02, debit_rate_annual=0.09)

    expected_cash = -10_000.0 * (1.0 + (0.09 / 360.0))
    strategy._apply_daily_cash_financing_if_needed()

    assert strategy.cash == pytest.approx(expected_cash)
    assert strategy._cash_financing_debit_total == pytest.approx(abs(expected_cash + 10_000.0))
    assert strategy._cash_financing_credit_total == pytest.approx(0.0)
    assert strategy._cash_financing_net_total < 0.0

    events = strategy.broker._trade_event_log_df
    financing_events = events.loc[events["cash_event_raw_type"] == "cash_financing_debit"]
    assert not financing_events.empty
    assert financing_events["cash_event_type"].tolist() == ["interest"]


class _InitializeConfigStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.configure_cash_financing(
            enabled=True,
            account_mode="margin",
            day_count_basis=360,
            missing_rate_policy="carry_forward",
        )
        self.set_cash_financing_rates(credit_rate_annual=0.02, debit_rate_annual=0.08)

    def on_trading_iteration(self):
        pass


def test_cash_financing_can_be_configured_from_initialize():
    data_source = PandasDataBacktesting(
        datetime_start=datetime(2025, 1, 2),
        datetime_end=datetime(2025, 1, 20),
    )
    broker = BacktestingBroker(data_source=data_source)
    strategy = _InitializeConfigStrategy(broker=broker, budget=100_000, name="InitializeConfigStrategy")
    strategy.initialize()

    assert strategy._cash_financing_enabled is True
    assert strategy._cash_financing_credit_rate_annual == pytest.approx(0.02)
    assert strategy._cash_financing_debit_rate_annual == pytest.approx(0.08)


class _IterationRateStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.configure_cash_financing(enabled=True, account_mode="margin", day_count_basis=360)
        self.set_cash_financing_rates(credit_rate_annual=0.0, debit_rate_annual=0.0)

    def on_trading_iteration(self):
        self.set_cash_financing_rates(credit_rate_annual=0.036, debit_rate_annual=0.09)


def test_cash_financing_rates_set_inside_on_trading_iteration_apply_that_day():
    data_source = PandasDataBacktesting(
        datetime_start=datetime(2025, 1, 2),
        datetime_end=datetime(2025, 1, 20),
    )
    broker = BacktestingBroker(data_source=data_source)
    strategy = _IterationRateStrategy(broker=broker, budget=100_000, name="IterationRateStrategy")
    strategy.initialize()

    strategy.on_trading_iteration()
    expected_cash = strategy.cash * (1.0 + (0.036 / 360.0))
    strategy._apply_daily_cash_financing_if_needed()

    assert strategy.cash == pytest.approx(expected_cash)
    assert strategy._cash_financing_last_credit_rate_used == pytest.approx(0.036)
    assert strategy._cash_financing_last_debit_rate_used == pytest.approx(0.09)


class _FilledOrderCashStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.configure_cash_financing(enabled=True, account_mode="margin", day_count_basis=360)
        self.set_cash_financing_rates(credit_rate_annual=0.01, debit_rate_annual=0.08)

    def on_trading_iteration(self):
        pass

    def on_filled_order(self, position, order, price, quantity, multiplier):  # noqa: ANN001
        self.deposit_cash(500, reason="rebate")
        self.withdraw_cash(200, reason="distribution")
        self.set_cash_financing_rates(debit_rate_annual=0.11)


def test_cash_api_can_be_used_from_on_filled_order_callback():
    data_source = PandasDataBacktesting(
        datetime_start=datetime(2025, 1, 2),
        datetime_end=datetime(2025, 1, 20),
    )
    broker = BacktestingBroker(data_source=data_source)
    strategy = _FilledOrderCashStrategy(broker=broker, budget=100_000, name="FilledOrderCashStrategy")
    strategy.initialize()
    starting_cash = strategy.cash

    strategy.on_filled_order(None, None, 0.0, 0.0, 1.0)

    assert strategy.cash == pytest.approx(starting_cash + 300.0)
    assert strategy._cash_deposits_total == pytest.approx(500.0)
    assert strategy._cash_withdrawals_total == pytest.approx(200.0)
    assert strategy._cash_financing_debit_rate_annual == pytest.approx(0.11)


class _TraceStatsCashStubStrategy:
    __test__ = False

    def __init__(self):
        self._rows: list[dict] = []
        self.portfolio_value = 101.0
        self.cash = 77.0
        self._cash_deposits_total = 25.0
        self._cash_withdrawals_total = 10.0
        self._cash_adjustments_net_total = 15.0
        self._cash_financing_enabled = True
        self._cash_financing_account_mode = "margin"
        self._cash_financing_credit_total = 2.0
        self._cash_financing_debit_total = 0.5
        self._cash_financing_net_total = 1.5
        self._cash_financing_days_accrued = 4
        self._cash_financing_events = 2
        self._cash_financing_last_credit_rate_used = 0.05
        self._cash_financing_last_debit_rate_used = 0.06

    def trace_stats(self, context, snapshot_before):  # noqa: ANN001
        return {}

    def get_datetime(self):
        return datetime(2026, 1, 1, tzinfo=timezone.utc)

    def get_positions(self):
        return [
            Position(strategy="test", asset=Asset("SPY"), quantity=1),
        ]

    def _append_row(self, row: dict) -> None:
        self._rows.append(row)


def test_trace_stats_includes_cash_flow_and_financing_fields():
    strategy = _TraceStatsCashStubStrategy()
    dummy_executor = type("_DummyExecutor", (), {"strategy": strategy})()

    StrategyExecutor._trace_stats(dummy_executor, context=None, snapshot_before={})

    row = strategy._rows[-1]
    assert row["cash_deposits_total"] == pytest.approx(25.0)
    assert row["cash_withdrawals_total"] == pytest.approx(10.0)
    assert row["cash_adjustments_net_total"] == pytest.approx(15.0)
    assert row["cash_financing_credit_total"] == pytest.approx(2.0)
    assert row["cash_financing_debit_total"] == pytest.approx(0.5)
    assert row["cash_financing_net_total"] == pytest.approx(1.5)
    assert row["cash_financing_days_accrued"] == 4
    assert row["cash_financing_events"] == 2


def test_default_cash_tearsheet_metrics_include_framework_cash_fields():
    strategy = _TraceStatsCashStubStrategy()

    metrics = _Strategy._default_cash_tearsheet_metrics(strategy)

    assert metrics["Cash Deposits Total"] == pytest.approx(25.0)
    assert metrics["Cash Withdrawals Total"] == pytest.approx(10.0)
    assert metrics["Cash Adjustments Net Total"] == pytest.approx(15.0)
    assert metrics["Cash Financing Credit Total"] == pytest.approx(2.0)
    assert metrics["Cash Financing Debit Total"] == pytest.approx(0.5)
    assert metrics["Cash Financing Net Total"] == pytest.approx(1.5)
    assert metrics["Cash Financing Days Accrued"] == 4
    assert metrics["Cash Financing Events"] == 2


class _StatsReturnAdjustmentStubStrategy:
    __test__ = False

    def __init__(self):
        self._stats_dirty = True
        self._stats = None
        self._stats_list = [
            {
                "datetime": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "portfolio_value": 100_000.0,
                "cash_adjustments_net_total": 0.0,
            },
            {
                "datetime": datetime(2026, 1, 2, tzinfo=timezone.utc),
                "portfolio_value": 104_000.0,
                "cash_adjustments_net_total": 0.0,
            },
            {
                "datetime": datetime(2026, 1, 3, tzinfo=timezone.utc),
                "portfolio_value": 103_000.0,
                "cash_adjustments_net_total": -1_000.0,
            },
        ]


def test_format_stats_subtracts_external_cash_flows_from_return_series():
    strategy = _StatsReturnAdjustmentStubStrategy()

    stats = _Strategy._format_stats(strategy)

    assert "cash_adjustments_net_period" in stats.columns
    assert float(stats.iloc[0]["cash_adjustments_net_period"]) == pytest.approx(0.0)
    assert float(stats.iloc[1]["return"]) == pytest.approx(0.04)
    assert float(stats.iloc[2]["cash_adjustments_net_period"]) == pytest.approx(-1_000.0)
    assert float(stats.iloc[2]["return"]) == pytest.approx(0.0)


def test_create_tearsheet_uses_cash_flow_adjusted_strategy_returns(monkeypatch, tmp_path):
    import quantstats_lumi as qs

    captured: dict[str, object] = {}

    def fake_html(returns, benchmark=None, title=None, output=None, download_filename=None, **kwargs):
        captured["returns"] = returns.copy()
        captured["benchmark"] = benchmark.copy() if benchmark is not None else None
        if output:
            with open(output, "w", encoding="utf-8") as handle:
                handle.write("<html><body>ok</body></html>")
        return pd.DataFrame({"Metric": ["Total Return"], "Strategy": ["4.00%"]})

    def fake_metrics_json(returns, benchmark=None, rf=0.0, output=None, summary_only=False, **kwargs):
        payload = {
            "metadata": {"summary_only": bool(summary_only)},
            "scalar_metrics": {"Total Return": float((1.0 + returns).prod() - 1.0)},
        }
        if output:
            with open(output, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
        return payload

    monkeypatch.setattr(qs.reports, "html", fake_html)
    monkeypatch.setattr(qs.reports, "metrics_json", fake_metrics_json, raising=False)

    idx = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"])
    strategy_df = pd.DataFrame(
        {
            "portfolio_value": [100_000.0, 104_000.0, 103_000.0],
            "cash_adjustments_net_total": [0.0, 0.0, -1_000.0],
            "return": [0.0, 0.04, 0.0],
        },
        index=idx,
    )
    benchmark_df = pd.DataFrame({"symbol_cumprod": [1.0, 1.01, 1.02]}, index=idx)

    tearsheet_file = tmp_path / "tearsheet.html"
    metrics_file = tmp_path / "tearsheet_metrics.json"

    create_tearsheet(
        strategy_df=strategy_df,
        strat_name="CashflowAdjustedStrategy",
        tearsheet_file=str(tearsheet_file),
        benchmark_df=benchmark_df,
        benchmark_asset="SPY",
        show_tearsheet=False,
        save_tearsheet=True,
        risk_free_rate=0.0,
        tearsheet_metrics_file=str(metrics_file),
    )

    assert tearsheet_file.exists()
    assert metrics_file.exists()

    returns = captured["returns"]
    assert float(returns.loc[pd.Timestamp("2026-01-02")]) == pytest.approx(0.04)
    assert float(returns.loc[pd.Timestamp("2026-01-03")]) == pytest.approx(0.0)

    payload = json.loads(metrics_file.read_text(encoding="utf-8"))
    assert payload["scalar_metrics"]["Total Return"] == pytest.approx(0.04)


SPY = Asset(symbol="SPY", asset_type="stock")


class _CashArtifactsBacktestStrategy(Strategy):
    def initialize(self):
        self.asset = SPY
        self.sleeptime = "1D"
        self.vars.bought = False
        self.vars.withdrew = False
        self.configure_cash_financing(
            enabled=True,
            account_mode="margin",
            day_count_basis=360,
            missing_rate_policy="carry_forward",
        )
        self.set_cash_financing_rates(credit_rate_annual=0.036, debit_rate_annual=0.09)
        self.deposit_cash(20_000.0, reason="starting_credit")

    def on_trading_iteration(self):
        if not self.vars.bought:
            self.submit_order(self.create_order(self.asset, 1, "buy"))
            self.vars.bought = True
        else:
            self.set_cash_financing_rates(debit_rate_annual=0.10)

    def on_filled_order(self, position, order, price, quantity, multiplier):  # noqa: ANN001
        if not self.vars.withdrew:
            self.withdraw_cash(33_000.0, reason="distribution")
            self.vars.withdrew = True


def test_cash_financing_backtest_artifacts_include_stats_and_tearsheet_metrics(pandas_data_fixture, tmp_path):
    stats_file = tmp_path / "stats.csv"
    tearsheet_file = tmp_path / "tearsheet.html"
    metrics_file = tmp_path / "tearsheet_metrics.json"

    _CashArtifactsBacktestStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2019, 1, 2),
        backtesting_end=datetime(2019, 1, 11),
        pandas_data=pandas_data_fixture,
        benchmark_asset="SPY",
        budget=100_000,
        risk_free_rate=0.0369,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=True,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
        plot_file_html=str(tmp_path / "trades.html"),
        stats_file=str(stats_file),
        tearsheet_file=str(tearsheet_file),
        indicators_file=str(tmp_path / "indicators.html"),
        tearsheet_metrics_file=str(metrics_file),
    )

    assert stats_file.exists()
    assert tearsheet_file.exists()
    assert metrics_file.exists()

    stats_df = pd.read_csv(stats_file)
    assert "cash_deposits_total" in stats_df.columns
    assert "cash_withdrawals_total" in stats_df.columns
    assert "cash_adjustments_net_period" in stats_df.columns
    assert "cash_financing_credit_total" in stats_df.columns
    assert "cash_financing_last_debit_rate_used" in stats_df.columns

    last_row = stats_df.iloc[-1]
    assert float(last_row["cash_deposits_total"]) == pytest.approx(20_000.0)
    assert float(last_row["cash_withdrawals_total"]) == pytest.approx(33_000.0)
    assert float(last_row["cash_adjustments_net_total"]) == pytest.approx(-13_000.0)
    assert int(last_row["cash_financing_events"]) >= 1
    assert int(last_row["cash_financing_days_accrued"]) >= 1
    assert float(last_row["cash_financing_last_credit_rate_used"]) == pytest.approx(0.036)
    assert float(last_row["cash_financing_last_debit_rate_used"]) == pytest.approx(0.10)
    assert "cash_adjusted_portfolio_value" in stats_df.columns

    payload = json.loads(metrics_file.read_text(encoding="utf-8"))
    scalar_metrics = payload.get("scalar_metrics", {})
    metadata = payload.get("metadata", {})
    if scalar_metrics:
        assert scalar_metrics["Cash Deposits Total"] == pytest.approx(20_000.0)
        assert scalar_metrics["Cash Withdrawals Total"] == pytest.approx(33_000.0)
        assert scalar_metrics["Cash Adjustments Net Total"] == pytest.approx(-13_000.0)
        assert scalar_metrics["Cash Financing Events"] >= 1
    else:
        assert metadata.get("status") == "unavailable"
        assert str(metadata.get("reason", "")).startswith("metrics_json_error:")


def test_backtest_artifacts_write_cash_events_into_trades_csv_and_plot(pandas_data_fixture, tmp_path):
    stats_file = tmp_path / "stats.csv"
    trades_file = tmp_path / "trades.csv"
    plot_file = tmp_path / "trades.html"

    _CashArtifactsBacktestStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2019, 1, 2),
        backtesting_end=datetime(2019, 1, 11),
        pandas_data=pandas_data_fixture,
        benchmark_asset="SPY",
        budget=100_000,
        risk_free_rate=0.0369,
        show_plot=True,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
        plot_file_html=str(plot_file),
        trades_file=str(trades_file),
        stats_file=str(stats_file),
    )

    assert stats_file.exists()
    assert trades_file.exists()
    assert plot_file.exists()

    trades_df = pd.read_csv(trades_file)
    cash_events = trades_df.loc[trades_df["event_kind"] == "cash_event"]
    assert not cash_events.empty
    assert {"deposit", "withdrawal", "interest"}.issubset(set(cash_events["cash_event_type"].dropna()))

    html = plot_file.read_text(encoding="utf-8")
    assert "Cash-Adjusted Portfolio Value" in html
    assert "Portfolio Value" in html
    assert "Deposit" in html
    assert "Withdrawal" in html
    assert "Financing Credit" in html or "Financing Debit" in html


@pytest.mark.parametrize(
    ("initial_adjustment", "expected_total"),
    [
        (-1_000.0, -1_000.0),
        (2_500.0, 2_500.0),
    ],
    ids=["withdrawal", "deposit"],
)
def test_no_trade_external_cash_flows_do_not_count_as_strategy_return(
    pandas_data_fixture,
    tmp_path,
    initial_adjustment,
    expected_total,
):
    class _NoTradeCashFlowStrategy(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.vars.did_adjust = False

        def on_trading_iteration(self):
            if self.vars.did_adjust:
                return
            if initial_adjustment >= 0:
                self.deposit_cash(initial_adjustment, reason="capital_inflow")
            else:
                self.withdraw_cash(abs(initial_adjustment), reason="capital_outflow")
            self.vars.did_adjust = True

    stats_file = tmp_path / "stats.csv"
    results, strategy = _NoTradeCashFlowStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2019, 1, 2),
        backtesting_end=datetime(2019, 1, 9),
        pandas_data=pandas_data_fixture,
        benchmark_asset="SPY",
        budget=100_000,
        risk_free_rate=0.0,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
        stats_file=str(stats_file),
    )

    assert stats_file.exists()
    stats_df = pd.read_csv(stats_file)
    assert "cash_adjustments_net_period" in stats_df.columns
    assert "return" in stats_df.columns
    assert float(stats_df["cash_adjustments_net_total"].iloc[-1]) == pytest.approx(expected_total)
    assert float(stats_df["cash_adjustments_net_period"].abs().sum()) == pytest.approx(abs(initial_adjustment))
    assert stats_df["return"].fillna(0.0).abs().max() == pytest.approx(0.0, abs=1e-12)
    assert float(results["total_return"]) == pytest.approx(0.0, abs=1e-12)
    assert float(strategy._analysis["total_return"]) == pytest.approx(0.0, abs=1e-12)
