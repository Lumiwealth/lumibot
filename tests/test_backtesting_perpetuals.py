"""Financial accounting through actual broker fills, positions and valuation."""

from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, ContractSpec, Data, Order, Position, TradingFee
from tests.test_backtesting_multileg_unit import _StubStrategy


@pytest.fixture
def account():
    brokers = []

    def create(*, inverse=False, settle="USDT", unit="contracts", budget=1000, fees=False, report=None):
        spec = ContractSpec("BTC", "USD" if inverse else settle, "BTC" if inverse else settle,
                            1 if inverse or unit == "base" else "0.001", inverse, unit)
        asset = Asset(f"BTC/{spec.quote}:{spec.settle}", "crypto_future", leverage=10, contract_spec=spec)
        quote = Asset(report or spec.settle, "crypto")
        price_quote = Asset(spec.quote, "crypto")
        dates = pd.date_range("2026-01-01 00:00", periods=6, freq="min", tz="UTC")
        df = pd.DataFrame({"open": 50000, "high": 55000, "low": 49000, "close": 50000, "volume": 1000}, index=dates)
        data = Data(asset, df, quote=price_quote, timestep="minute")
        datasets = [data]
        if report and report != spec.settle:
            fx = df.copy()
            fx[["open", "high", "low", "close"]] = 1
            datasets.append(Data(Asset(spec.settle, "crypto"), fx, quote=quote, timestep="minute"))
        source = PandasDataBacktesting(datetime_start=dates[0], datetime_end=dates[-1],
                                      pandas_data=datasets, show_progress_bar=False)
        source.load_data()
        broker = BacktestingBroker(source)
        brokers.append(broker)
        strategy = _StubStrategy(broker=broker, quote_asset=quote, budget=budget, analyze_backtest=False,
                                 buy_trading_fees=[TradingFee(percent_fee="0.001")] if fees else [],
                                 sell_trading_fees=[TradingFee(percent_fee="0.001")] if fees else [])
        return strategy, broker, asset

    yield create
    for broker in brokers:
        broker.cleanup_streams()


def fill(strategy, broker, asset, quantity, side, price=50000, **kwargs):
    order = strategy.create_order(asset, Decimal(str(quantity)), side, **kwargs)
    broker._process_new_order(order)
    broker._execute_filled_order(order, price, Decimal(str(quantity)), strategy)
    return order


@pytest.mark.parametrize("settle,unit,quantity", [("USDT", "contracts", 20), ("USDC", "contracts", 20),
                                                ("USDT", "base", ".02")])
def test_linear_cash_equity_and_realized_pnl(account, settle, unit, quantity):
    strategy, broker, asset = account(settle=settle, unit=unit)
    assert broker.data_source.get_last_price(asset) == 50000
    fill(strategy, broker, asset, quantity, "buy")
    assert strategy.cash == pytest.approx(1000)
    assert strategy._update_portfolio_value() == pytest.approx(1000)
    fill(strategy, broker, asset, quantity, "sell", 55000, reduce_only=True)
    assert broker.get_tracked_position(strategy.name, asset) is None
    assert strategy.cash == pytest.approx(1100)


def test_inverse_pnl_and_percentage_fees_settle_in_coin(account):
    strategy, broker, asset = account(inverse=True, budget="0.1", fees=True)
    fill(strategy, broker, asset, 1000, "sell")
    assert strategy.cash == pytest.approx(.1 - 1/50000)
    fill(strategy, broker, asset, 1000, "buy", 40000, reduce_only=True)
    assert strategy.cash == pytest.approx(.1 + 1000*(1/40000 - 1/50000) - 1/50000 - 1/40000)


def test_hedge_legs_keep_separate_basis_and_close_ownership(account):
    strategy, broker, asset = account()
    fill(strategy, broker, asset, 20, "buy", position_side="long")
    fill(strategy, broker, asset, 20, "sell", 51000, position_side="short")
    assert broker.get_tracked_position(strategy.name, asset, position_side="long").quantity == 20
    assert broker.get_tracked_position(strategy.name, asset, position_side="short").quantity == -20
    fill(strategy, broker, asset, 20, "sell", 55000, position_side="long", reduce_only=True)
    assert broker.get_tracked_position(strategy.name, asset, position_side="long") is None
    assert broker.get_tracked_position(strategy.name, asset, position_side="short").quantity == -20
    assert strategy.cash == pytest.approx(1100)


def test_margin_reserves_and_reduce_only_cannot_reverse(account):
    strategy, broker, asset = account(budget=150)
    fill(strategy, broker, asset, 20, "buy")
    rejected = fill(strategy, broker, asset, 20, "buy")
    assert rejected.status == Order.OrderStatus.ERROR
    assert broker.get_tracked_position(strategy.name, asset).quantity == 20
    assert strategy.cash == pytest.approx(150)
    fill(strategy, broker, asset, 30, "sell", 55000, reduce_only=True)
    assert broker.get_tracked_position(strategy.name, asset) is None
    assert strategy.cash == pytest.approx(250)


def test_reversal_realizes_only_closed_contracts(account):
    strategy, broker, asset = account()
    fill(strategy, broker, asset, 20, "buy")
    fill(strategy, broker, asset, 30, "sell", 55000)
    assert strategy.cash == pytest.approx(1100)
    assert broker.get_tracked_position(strategy.name, asset).quantity == -10
    fill(strategy, broker, asset, 10, "buy", 54000, reduce_only=True)
    assert strategy.cash == pytest.approx(1110)


def test_unrealized_value_is_profit_not_leveraged_notional(account):
    strategy, broker, asset = account()
    fill(strategy, broker, asset, 20, "buy", 45000)
    assert strategy._update_portfolio_value() == pytest.approx(1100)


def test_fifo_partial_close_retains_remaining_lot_basis(account):
    strategy, broker, asset = account()
    fill(strategy, broker, asset, 10, "buy", 40000)
    fill(strategy, broker, asset, 10, "buy", 50000)
    fill(strategy, broker, asset, 10, "sell", 55000, reduce_only=True)
    position = broker.get_tracked_position(strategy.name, asset)
    assert strategy.cash == pytest.approx(1150)
    assert position.avg_fill_price == 50000
    assert strategy._update_portfolio_value() == pytest.approx(1150)


def test_settlement_wallet_is_distinct_from_reporting_cash(account):
    strategy, broker, asset = account(settle="USDC", report="USDT")
    broker._filled_positions.append(Position(strategy.name, Asset("USDC", "crypto"), 600))
    fill(strategy, broker, asset, 20, "buy")
    assert strategy._update_portfolio_value() == pytest.approx(1600)
    fill(strategy, broker, asset, 20, "sell", 55000, reduce_only=True)
    assert strategy.cash == pytest.approx(1000)
    assert broker.get_tracked_position(strategy.name, Asset("USDC", "crypto")).quantity == 700
    assert strategy._update_portfolio_value() == pytest.approx(1700)


def test_reduce_only_and_terms_survive_order_serialization(account):
    strategy, _, asset = account()
    order = strategy.create_order(asset, 20, "sell", reduce_only=True, position_side="long")
    loaded = Order.from_dict(order.to_dict())
    assert loaded.reduce_only is True
    assert loaded.position_side == "long"
    assert loaded.asset.contract_spec == asset.contract_spec


def test_two_strategies_cannot_change_each_others_collateral(account):
    strategy, broker, asset = account()
    other = _StubStrategy(broker=broker, name="IndependentAccount", quote_asset=strategy.quote_asset,
                          budget=200, analyze_backtest=False)
    assert strategy.cash == 1000
    fill(other, broker, asset, 20, "buy")
    fill(other, broker, asset, 20, "sell", 55000, reduce_only=True)
    assert other.cash == 300
    assert strategy.cash == 1000


class RoundTrip(_StubStrategy):
    def initialize(self, asset, quantity):
        self.asset, self.size = asset, quantity
        self.steps = 0
        self.fills = []
        self.sleeptime = "1M"
        self.set_market("24/7")

    def on_trading_iteration(self):
        if self.steps == 0:
            self.submit_order(self.create_order(self.asset, self.size, "buy"))
        elif self.steps == 3:
            self.submit_order(self.create_order(self.asset, self.size, "sell",
                                               reduce_only=self.asset.asset_type == "crypto_future"))
        self.steps += 1

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.fills.append((order.side, quantity, price))


@pytest.mark.parametrize("kind", ["spot", "linear-usdt", "linear-usdc", "inverse", "base-units"])
def test_normal_strategy_backtest_round_trip(kind):
    inverse = kind == "inverse"
    settle = "BTC" if inverse else "USDC" if kind == "linear-usdc" else "USDT"
    unit = "base" if kind == "base-units" else "contracts"
    spec = ContractSpec("BTC", "USD" if inverse else settle, settle,
                        1 if inverse or unit == "base" else ".001", inverse, unit)
    asset = (Asset("BTC", "crypto") if kind == "spot" else
             Asset(f"BTC/{spec.quote}:{settle}", "crypto_future", leverage=10, contract_spec=spec))
    quantity = .02 if kind in ("spot", "base-units") else 1000 if inverse else 20
    quote = Asset(settle, "crypto")
    dates = pd.date_range("2026-01-01 00:00", periods=8, freq="min", tz="UTC")
    prices = [50000]*3 + [55000]*5
    df = pd.DataFrame({column: prices for column in ("open", "high", "low", "close")}, index=dates)
    df["volume"] = 1000
    data = Data(asset, df, quote=Asset(spec.quote, "crypto"), timestep="minute")
    _, strategy = RoundTrip.run_backtest(
        PandasDataBacktesting, dates[0], dates[-1], quote_asset=quote,
        budget=.1 if inverse else 2000, pandas_data=[data],
        parameters={"asset": asset, "quantity": quantity},
        benchmark_asset=None, analyze_backtest=False, show_plot=False,
        show_tearsheet=False, show_indicators=False, show_progress_bar=False,
        save_logfile=False, save_stats_file=False,
    )
    assert len(strategy.fills) == 2
    assert strategy.broker.get_tracked_position(strategy.name, asset) is None
    expected = .1 + 1000*(1/50000 - 1/55000) if inverse else 2100
    assert strategy.cash == pytest.approx(expected)
    assert strategy._update_portfolio_value() == pytest.approx(expected)
