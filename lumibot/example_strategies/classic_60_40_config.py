from decimal import Decimal

from lumibot.components.drift_rebalancer_logic import DriftType
from lumibot.entities import Order, Asset


parameters = {
    "market": "NYSE",
    "sleeptime": "1D",
    "drift_type": DriftType.RELATIVE,
    "drift_threshold": "0.1",
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
            "weight": Decimal("0.4")
        }
    ],
    "shorting": False,
    "fractional_shares": False,
    "only_rebalance_drifted_assets": False,
}
