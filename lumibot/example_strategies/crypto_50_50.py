from datetime import datetime
import logging
from decimal import Decimal

from lumibot.credentials import broker
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.components.drift_rebalancer_logic import DriftType
from lumibot.entities import Order, Asset

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    is_live = False

    parameters = {
        "market": "24/7",
        "sleeptime": "1D",
        "drift_type": DriftType.ABSOLUTE,
        "drift_threshold": "0.05",  # 5%
        "order_type": Order.OrderType.LIMIT,
        "acceptable_slippage": "0.005",  # 50 BPS
        "fill_sleeptime": 15,
        "shorting": False,
        "fractional_shares": True
    }

    if not is_live:
        # Backtest this strategy
        backtesting_start = datetime(2023, 1, 1)
        backtesting_end = datetime(2023, 8, 1)

        # Backtesting crypto using yahoo means we need to use stock assets.
        parameters["portfolio_weights"] = [
            {
                "base_asset": Asset(symbol='BTC-USD', asset_type='stock'),
                "weight": Decimal("0.5")
            },
            {
                "base_asset": Asset(symbol='ETH-USD', asset_type='stock'),
                "weight": Decimal("0.5")
            }
        ]
        results = DriftRebalancer.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            parameters=parameters,
            show_plot=True,
            show_tearsheet=True,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=True
        )

    elif isinstance(broker, Alpaca):
        # Run the strategy live
        logger.info("Running the strategy live with alpaca.")

        # Trading crypto live means we need to use crypto assets.
        parameters["portfolio_weights"] = [
            {
                "base_asset": Asset(symbol='BTC', asset_type='crypto'),
                "weight": Decimal("0.5")
            },
            {
                "base_asset": Asset(symbol='ETH', asset_type='crypto'),
                "weight": Decimal("0.5")
            }
        ]
        strategy = DriftRebalancer(broker, parameters=parameters)
        strategy.run_live()
