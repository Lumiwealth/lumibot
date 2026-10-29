"""Timestamped funding and liquidation use real backtest state transitions."""

import pandas as pd
import pytest

from tests.test_backtesting_perpetuals import account, fill


def event(strategy, minute, rate, mark=50000):
    timestamp = strategy.broker.data_source.datetime_start + pd.Timedelta(minutes=minute)
    return {"timestamp": int(timestamp.timestamp()*1000), "fundingRate": rate, "markPrice": mark}


def advance(strategy, minutes):
    source = strategy.broker.data_source
    source._update_datetime(source.datetime_start + pd.Timedelta(minutes=minutes))
    strategy.broker.process_pending_orders(strategy)


def test_funding_at_actual_times_is_applied_once_across_clock_jump(account):
    strategy, broker, asset = account()
    broker.data_source.funding_rates = {asset.symbol: [event(strategy, 1, .01), event(strategy, 3, -.02, 55000)]}
    fill(strategy, broker, asset, 20, "buy")
    advance(strategy, 3)
    assert strategy.cash == pytest.approx(1012)  # -1000*.01 + 1100*.02
    advance(strategy, 3)
    assert strategy.cash == pytest.approx(1012)
    fill(strategy, broker, asset, 20, "sell", reduce_only=True)
    advance(strategy, 4)
    assert len([row for row in broker.perpetual_events if row["type"] == "funding"]) == 2


def test_hedge_funding_and_inverse_settlement(account):
    strategy, broker, asset = account(inverse=True, budget=.1)
    broker.data_source.funding_rates = {asset: [event(strategy, 1, .01, 40000)]}
    fill(strategy, broker, asset, 1000, "buy", position_side="long")
    fill(strategy, broker, asset, 500, "sell", position_side="short")
    advance(strategy, 2)
    assert strategy.cash == pytest.approx(.1 - .00025 + .000125)
    rows = [row for row in broker.perpetual_events if row["type"] == "funding"]
    assert {row["position_side"] for row in rows} == {"long", "short"}
    assert {row["currency"] for row in rows} == {"BTC"}


def test_opening_at_funding_boundary_does_not_pay_prior_interval(account):
    strategy, broker, asset = account()
    broker.data_source.funding_rates = {asset: [event(strategy, 1, .01)]}
    advance(strategy, 1)
    fill(strategy, broker, asset, 20, "buy")
    advance(strategy, 2)
    assert strategy.cash == 1000


def test_isolated_liquidation_preserves_other_hedge_leg_and_wallet_cash(account):
    strategy, broker, asset = account()
    source = broker.data_source
    source.funding_rates = {asset: []}
    source.perpetual_risk = {asset: {"maintenance_margin_rate": .005, "liquidation_fee_rate": .002}}
    source.mark_prices = {asset: [
        {"timestamp": event(strategy, 0, 0)["timestamp"], "markPrice": 50000},
        {"timestamp": event(strategy, 1, 0)["timestamp"], "markPrice": 45200},
    ]}
    fill(strategy, broker, asset, 20, "buy", position_side="long", custom_params={"marginMode": "isolated"})
    fill(strategy, broker, asset, 20, "sell", position_side="short", custom_params={"marginMode": "isolated"})
    advance(strategy, 1)
    assert broker.get_tracked_position(strategy.name, asset, position_side="long") is None
    assert broker.get_tracked_position(strategy.name, asset, position_side="short").quantity == -20
    assert strategy.cash == pytest.approx(902.192)  # 96 loss + 904*.002 fee
    assert any(row["type"] == "liquidation" for row in broker.perpetual_events)


def test_missing_funding_and_mark_history_remain_visible_assumptions(account):
    strategy, broker, asset = account()
    fill(strategy, broker, asset, 20, "buy")
    advance(strategy, 1)
    assumptions = " ".join(broker.backtest_assumptions)
    assert "funding" in assumptions and "mark" in assumptions and "maintenance" in assumptions


def test_past_funding_uses_closed_past_price_not_current_or_future_mark(account):
    strategy, broker, asset = account()
    row = event(strategy, 1, .01)
    row.pop("markPrice")
    broker.data_source.funding_rates = {asset: [row]}
    broker.data_source.mark_prices = {asset: [{"timestamp": event(strategy, 5, 0)["timestamp"], "markPrice": 999999}]}
    fill(strategy, broker, asset, 20, "buy")
    advance(strategy, 3)
    assert strategy.cash == 990


def test_late_opening_does_not_liquidate_against_marks_before_ownership(account):
    strategy, broker, asset = account()
    source = broker.data_source
    source.mark_prices = {asset: [{"timestamp": event(strategy, 1, 0)["timestamp"], "markPrice": 1},
                                 {"timestamp": event(strategy, 3, 0)["timestamp"], "markPrice": 50000}]}
    source.funding_rates = {asset: []}
    advance(strategy, 3)
    fill(strategy, broker, asset, 20, "buy", custom_params={"marginMode": "isolated"})
    advance(strategy, 4)
    assert broker.get_tracked_position(strategy.name, asset).quantity == 20


def test_isolated_gap_loss_cannot_spend_unallocated_wallet_collateral(account):
    strategy, broker, asset = account()
    source = broker.data_source
    source.funding_rates = {asset: []}
    source.mark_prices = {asset: [{"timestamp": event(strategy, 1, 0)["timestamp"], "markPrice": 40000}]}
    source.perpetual_risk = {asset: {"maintenance_margin_rate": .005, "liquidation_fee_rate": .002}}
    fill(strategy, broker, asset, 20, "buy", custom_params={"marginMode": "isolated"})
    advance(strategy, 1)
    assert broker.get_tracked_position(strategy.name, asset) is None
    assert strategy.cash == 900
    assert float(broker.perpetual_events[-1]["insurance_credit"]) > 0


def test_funding_and_assumptions_are_in_normal_analyzed_results():
    from lumibot.backtesting import PandasDataBacktesting
    from lumibot.entities import Asset, ContractSpec, Data
    from tests.test_backtesting_perpetuals import RoundTrip

    asset = Asset("BTC/USDT:USDT", "crypto_future", leverage=10,
                  contract_spec=ContractSpec("BTC", "USDT", "USDT", ".001"))
    quote = Asset("USDT", "crypto")
    dates = pd.date_range("2026-01-01", periods=7, freq="min", tz="UTC")
    df = pd.DataFrame({column: 50000 for column in ("open", "high", "low", "close")}, index=dates)
    df["volume"] = 1000
    data = Data(asset, df, quote=quote, timestep="minute")
    result, strategy = RoundTrip.run_backtest(
        PandasDataBacktesting, dates[0], dates[-1], pandas_data=[data], quote_asset=quote,
        budget=2000, parameters={"asset": asset, "quantity": 20},
        funding_rates={asset: [{"timestamp": dates[2], "fundingRate": .01, "markPrice": 50000}]},
        benchmark_asset=None, analyze_backtest=True, show_plot=False, show_tearsheet=False,
        save_tearsheet=False, show_indicators=False, show_progress_bar=False,
        save_logfile=False, save_stats_file=False,
    )
    assert strategy.cash == 1990
    assert float(result["perpetual_events"][0]["amount"]) == -10
    assert result["backtest_assumptions"]
