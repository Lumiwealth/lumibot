import datetime
from decimal import Decimal
import pytest

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.components.drift_rebalancer_logic import DriftType
from lumibot.entities import Order, Asset
from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.traders import Trader


class TestIntegrationTests:
    """
    Run "integration tests" which serve to ensure that important functionality
    remains intact. If these tests fail, that means something changed and lumibot users
    might be counting on whatever it was that changed.
    """

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_yahoo(self):

        backtesting_start = datetime.datetime(2019, 1, 1)
        backtesting_end = datetime.datetime(2025, 1, 1)

        data_source = YahooDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            benchmark_asset=None
        )

        broker = BacktestingBroker(data_source=data_source)

        parameters = {
            "market": "NYSE",
            "sleeptime": "30D",
            "drift_type": DriftType.ABSOLUTE,
            "drift_threshold": "0.05",
            "order_type": Order.OrderType.LIMIT,
            "acceptable_slippage": "0.005",  # 50 BPS
            "fill_sleeptime": 15,
            "portfolio_weights": [
                {
                    "base_asset": Asset(symbol='SPY', asset_type='stock'),
                    "weight": Decimal("0.6")
                },
                {
                    "base_asset": Asset(symbol='TLT', asset_type='stock'),
                    "weight": Decimal("0.3")
                },
                {
                    "base_asset": Asset(symbol='GLD', asset_type='stock'),
                    "weight": Decimal("0.1")
                },
            ],
            "shorting": False,
            "fractional_shares": False,
            "only_rebalance_drifted_assets": False,
        }

        strategy = DriftRebalancer(
            broker=broker,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            parameters=parameters,
            include_cash_positions=True
        )

        trader = Trader(logfile="", backtest=True, quiet_logs=True)
        trader.add_strategy(strategy)
        results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False, tearsheet_file="")
        assert results
        result = list(results.values())[0]

        print(
            f"Results:\n"
            f"CAGR: {result['cagr']:.2%}\n"
            f"MaxDD: {result['max_drawdown']['drawdown']:.2%}\n"
            f"Vol: {result['volatility']:.2%}\n"
            f"Sharpe: {result['sharpe']:.2f}"
        )

        assert round(result['cagr'], 2) == 0.09