import pandas as pd
from typing import Any
from decimal import Decimal

from lumibot.strategies.strategy import Strategy
from lumibot.components.drift_rebalancer_logic import DriftRebalancerLogic, DriftType
from lumibot.entities import Order

"""
The DriftRebalancer strategy is designed to maintain a portfolio's target asset allocation by 
rebalancing assets based on their drift from target weights. The strategy calculates the 
drift of each asset in the portfolio and triggers a rebalance if the drift exceeds a predefined 
threshold. Then it places buy or sell assets to bring the portfolio back to its target allocation.

Note: If you run this strategy in a live trading environment, be sure to not make manual trades in the same account.
This strategy will sell other positions in order to get the account to the target weights. 
"""


class DriftRebalancer(Strategy):
    """The DriftRebalancer strategy rebalances a portfolio based on drift from target weights.

    The strategy calculates the drift of each asset in the portfolio and triggers a rebalance if the drift exceeds
    the drift_threshold. The strategy will sell assets that have drifted above the threshold and
    buy assets that have drifted below the threshold.

    The current version of the DriftRebalancer strategy only supports whole share quantities.
    Submit an issue if you need fractional shares. It should be pretty easy to add.

    Example parameters:

    parameters = {

        ### Standard lumibot strategy parameters
        "market": "NYSE",
        "sleeptime": "1D",

        ### DriftRebalancer parameters

        "strategy": Strategy,
        # The strategy object that will be used to get the current positions and submit orders.

        "drift_type": DriftType.RELATIVE,  # optional
        # The type of drift calculation to use. Can be "absolute" or "relative". The default is DriftType.ABSOLUTE.
        # If the drift_type is "absolute", the drift is calculated as the difference between the target_weight
        # and the current_weight. For example, if the target_weight is 0.20 and the current_weight is 0.23, the
        # absolute drift would be 0.03.
        # If the drift_type is "relative", the drift is calculated as the difference between the target_weight
        # and the current_weight divided by the target_weight. For example, if the target_weight is 0.20 and the
        # current_weight is 0.23, the relative drift would be (0.20 - 0.23) / 0.20 = -0.15.
        # Absolute drift is better if you have assets with small weights but don't want changes in small positions to
        # trigger a rebalance in your portfolio. If your target weights were like below, an absolute drift of 0.05 would
        # only trigger a rebalance when asset3 or asset4 drifted by 0.05 or more.
        # {
        #     "asset1": Decimal("0.025"),
        #     "asset2": Decimal("0.025"),
        #     "asset3": Decimal("0.40"),
        #     "asset4": Decimal("0.55"),
        # }
        # Relative drift can be useful when the target_weights are small or very different from each other, and you do
        # want changes in small positions to trigger a rebalance. If your target weights were like above, a relative drift
        # of 0.20 would trigger a rebalance when asset1 or asset2 drifted by 0.005 or more.

        "drift_threshold": Decimal("0.05"),  # optional
        # The drift threshold that will trigger a rebalance. The default is Decimal("0.05").
        # If the drift_type is absolute, the target_weight of an asset is 0.30 and the drift_threshold is 0.05,
        # then a rebalance will be triggered when the asset's current_weight is less than 0.25 or greater than 0.35.
        # If the drift_type is relative, the target_weight of an asset is 0.30 and the drift_threshold is 0.05,
        # then a rebalance will be triggered when the asset's current_weight is less than -0.285 or greater than 0.315.

        "order_type": Order.OrderType.LIMIT,  # optional
        # The type of order to use. Can be Order.OrderType.LIMIT or Order.OrderType.MARKET. The default is Order.OrderType.LIMIT.

        "fill_sleeptime": 15,  # optional
        # The amount of time to sleep between the sells and buys to give enough time for the orders to fill. The default is 15.

        "acceptable_slippage": Decimal("0.005"),  # optional
        # The acceptable slippage that will be used when calculating the number of shares to buy or sell. The default is Decimal("0.005") (50 BPS).

        "shorting": False,  # optional
        # If you want to allow shorting, set this to True. The default is False.

    }
    """

    # noinspection PyAttributeOutsideInit
    def initialize(self, parameters: Any = None) -> None:
        self.set_market(self.parameters.get("market", "NYSE"))
        self.sleeptime = self.parameters.get("sleeptime", "1D")
        self.drift_type = self.parameters.get("drift_type", DriftType.RELATIVE)
        self.drift_threshold = Decimal(self.parameters.get("drift_threshold", "0.10"))
        self.order_type = self.parameters.get("order_type", Order.OrderType.MARKET)
        self.acceptable_slippage = Decimal(self.parameters.get("acceptable_slippage", "0.005"))
        self.fill_sleeptime = self.parameters.get("fill_sleeptime", 15)
        self.target_weights = {k: Decimal(v) for k, v in self.parameters["target_weights"].items()}
        self.shorting = self.parameters.get("shorting", False)
        self.verbose = self.parameters.get("verbose", False)
        self.drift_df = pd.DataFrame()
        self.drift_rebalancer_logic = DriftRebalancerLogic(
            strategy=self,
            drift_type=self.drift_type,
            drift_threshold=self.drift_threshold,
            order_type=self.order_type,
            acceptable_slippage=self.acceptable_slippage,
            fill_sleeptime=self.fill_sleeptime,
            shorting=self.shorting,
        )

    # noinspection PyAttributeOutsideInit
    def on_trading_iteration(self) -> None:
        dt = self.get_datetime()
        self.logger.info(f"{dt} on_trading_iteration called")
        self.cancel_open_orders()

        if self.cash < 0:
            self.logger.error(
                f"Negative cash: {self.cash} "
                f"but DriftRebalancer does not support margin yet."
            )

        self.drift_df = self.drift_rebalancer_logic.calculate(target_weights=self.target_weights)
        self.drift_rebalancer_logic.rebalance(drift_df=self.drift_df)
        