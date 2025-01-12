from lumibot.components.drift_rebalancer_logic import DriftType
from lumibot.entities import Order

parameters = {
    "market": "NYSE",
    "sleeptime": "1D",

    # Pro tip: In live trading rebalance multiple times a day, more buys will be placed after the sells fill.
    # This will make it really likely that you will complete the rebalance in a single day.
    # "sleeptime": 60,

    "drift_type": DriftType.RELATIVE,
    "drift_threshold": "0.1",
    "order_type": Order.OrderType.MARKET,
    "acceptable_slippage": "0.005",  # 50 BPS
    "fill_sleeptime": 15,
    "target_weights": {
        "SPY": "0.60",
        "TLT": "0.40"
    },
    "shorting": False
}