from datetime import datetime
import logging
from decimal import Decimal

from lumibot.credentials import broker
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.components.drift_rebalancer_logic import DriftType
from lumibot.entities import Order, Asset

parameters = {
    "market": "24/7",
    "sleeptime": "60M",
    "drift_type": DriftType.ABSOLUTE,
    "drift_threshold": "0.05",  # 5%
    "order_type": Order.OrderType.LIMIT,
    "acceptable_slippage": "0.005",  # 50 BPS
    "fill_sleeptime": 15,
    "portfolio_weights": [
        {
            "base_asset": Asset(symbol='BTC', asset_type='crypto'),
            "weight": Decimal("0.5")
        },
        {
            "base_asset": Asset(symbol='ETH', asset_type='crypto'),
            "weight": Decimal("0.5")
        }
    ],
    "shorting": False,
    "fractional_shares": True,
    "only_rebalance_drifted_assets": False,
}
