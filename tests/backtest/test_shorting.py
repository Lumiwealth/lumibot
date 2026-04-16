import datetime as dt

import pytest

from decimal import Decimal

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.backtesting import AlpacaBacktesting
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.components.drift_rebalancer_logic import DriftType

# Skip these tests if Alpaca test credentials are not available
if not ALPACA_TEST_CONFIG.get('API_KEY') or ALPACA_TEST_CONFIG.get('API_KEY') == '<your key here>':
    pytest.skip("These tests require an Alpaca API key", allow_module_level=True)


class _ShortOnStartStock(Strategy):
    parameters = {"symbol": "SPY", "qty": 1}

    def initialize(self):
        self._did_short = False
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        if not self._did_short:
            sym = self.parameters["symbol"]
            qty = self.parameters["qty"]
            order = self.create_order(sym, qty, side="sell_short")
            self.submit_order(order)
            self._did_short = True


class _ShortOnStartCrypto(Strategy):
    parameters = {"pair": (Asset("BTC", asset_type="crypto"), Asset("USD", asset_type="forex")), "qty": 0.01}

    def initialize(self):
        self._did_short = False
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        if not self._did_short:
            base, quote = self.parameters["pair"]
            qty = self.parameters["qty"]
            order = self.create_order((base, quote), qty, side="sell_short")
            self.submit_order(order)
            self._did_short = True


def _five_day_window(start_date: dt.date):
    # Return a 7-day span that should include 5 market days for stocks
    start = dt.datetime.combine(start_date, dt.time(0, 0))
    end = start + dt.timedelta(days=7)
    return start, end


def _assert_trade_and_state(strategy: Strategy, target_asset):
    # 1) A trade was made
    filled_orders = strategy.broker._filled_orders.get_list()
    assert len(filled_orders) > 0, "Expected at least one filled order"

    # 2) Position exists with negative quantity
    positions = strategy.get_positions(include_cash_positions=False)
    neg_pos = None
    for p in positions:
        if p.asset == target_asset:
            neg_pos = p
            break
    assert neg_pos is not None, f"Expected a position for {target_asset}"
    assert float(neg_pos.quantity) < 0, "Expected a short position (negative qty)"

    # 3) Cash greater than starting cash
    assert strategy.cash > strategy.initial_budget, (
        f"Expected cash to increase from short proceeds: cash={strategy.cash}, initial={strategy.initial_budget}"
    )


def test_short_stock_on_start():
    # Use a recent period expected to have data; run long enough to cover 5 market days
    start, end = _five_day_window(dt.date(2024, 1, 3))

    results, strat = _ShortOnStartStock.run_backtest(
        datasource_class=AlpacaBacktesting,
        config=ALPACA_TEST_CONFIG,
        backtesting_start=start,
        backtesting_end=end,
        benchmark_asset="SPY",
        analyze_backtest=False,
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
    )

    target = Asset("SPY", asset_type="stock")
    _assert_trade_and_state(strat, target)


def test_short_crypto_on_start():
    start, end = _five_day_window(dt.date(2024, 1, 3))

    results, strat = _ShortOnStartCrypto.run_backtest(
        datasource_class=AlpacaBacktesting,
        config=ALPACA_TEST_CONFIG,
        backtesting_start=start,
        backtesting_end=end,
        benchmark_asset=(Asset("BTC", asset_type="crypto"), Asset("USD", asset_type="forex")),
        analyze_backtest=False,
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
    )

    target = Asset("BTC", asset_type="crypto")
    _assert_trade_and_state(strat, target)


def test_short_stock_with_drift_rebalancer():
    # Keep a 50% short position in SPY using the DriftRebalancer
    start, end = _five_day_window(dt.date(2024, 1, 3))

    params = {
        "sleeptime": "1D",
        "drift_type": DriftType.RELATIVE,
        "drift_threshold": "0.10",
        "acceptable_slippage": "0.005",
        "fill_sleeptime": 1,
        "portfolio_weights": [
            {"base_asset": Asset("SPY", asset_type="stock"), "weight": Decimal("-0.5")},
        ],
        "shorting": True,
        "fractional_shares": False,
        "only_rebalance_drifted_assets": False,
    }

    results, strat = DriftRebalancer.run_backtest(
        parameters=params,
        datasource_class=AlpacaBacktesting,
        config=ALPACA_TEST_CONFIG,
        backtesting_start=start,
        backtesting_end=end,
        benchmark_asset="SPY",
        analyze_backtest=False,
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
    )

    target = Asset("SPY", asset_type="stock")
    _assert_trade_and_state(strat, target)


def test_short_crypto_with_drift_rebalancer():
    # Keep a 50% short position in BTC (quoted in USD) using the DriftRebalancer
    start, end = _five_day_window(dt.date(2024, 1, 3))

    params = {
        "sleeptime": "1D",
        "drift_type": DriftType.RELATIVE,
        "drift_threshold": "0.10",
        "acceptable_slippage": "0.005",
        "fill_sleeptime": 1,
        "portfolio_weights": [
            {"base_asset": Asset("BTC", asset_type="crypto"), "weight": Decimal("-0.5")},
        ],
        "shorting": True,
        "fractional_shares": True,  # allow fractional crypto
        "only_rebalance_drifted_assets": False,
    }

    results, strat = DriftRebalancer.run_backtest(
        parameters=params,
        datasource_class=AlpacaBacktesting,
        config=ALPACA_TEST_CONFIG,
        backtesting_start=start,
        backtesting_end=end,
        benchmark_asset=(Asset("BTC", asset_type="crypto"), Asset("USD", asset_type="forex")),
        analyze_backtest=False,
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
    )

    target = Asset("BTC", asset_type="crypto")
    _assert_trade_and_state(strat, target)
