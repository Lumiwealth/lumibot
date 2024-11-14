import pandas as pd
from typing import Dict, Any
from decimal import Decimal, ROUND_DOWN
import time

from lumibot.strategies.strategy import Strategy
from lumibot.components import DriftCalculationLogic, LimitOrderDriftRebalancerLogic

"""
The DriftRebalancer strategy is designed to maintain a portfolio's target asset allocation by 
rebalancing assets based on their drift from target weights. The strategy calculates the 
drift of each asset in the portfolio and triggers a rebalance if the drift exceeds a predefined 
threshold. It uses limit orders to buy or sell assets to bring the portfolio back to its target allocation.

It basically does the following:
Calculate Drift: Determine the difference between the current and target weights of each asset in the portfolio.
Trigger Rebalance: Initiate buy or sell orders when the drift exceeds the threshold.
Execute Orders: Place limit orders to buy or sell assets based on the calculated drift.

Note: If you run this strategy in a live trading environment, be sure to not make manual trades in the same account.
This strategy will sell other positions in order to get the account to the target weights. 
"""


class DriftRebalancer(Strategy):
    """The DriftRebalancer strategy rebalances a portfolio based on drift from target weights.

    The strategy calculates the drift of each asset in the portfolio and triggers a rebalance if the drift exceeds
    the drift_threshold. The strategy will sell assets that have drifted above the threshold and
    buy assets that have drifted below the threshold.

    The current version of the DriftRebalancer strategy only supports limit orders and whole share quantities.
    Submit an issue if you need market orders or fractional shares. It should be pretty easy to add.

    Example parameters:

    parameters = {

        ### Standard lumibot strategy parameters
        "market": "NYSE",
        "sleeptime": "1D",

        ### DriftRebalancer parameters

        # This is the drift threshold that will trigger a rebalance. If the target_weight is 0.30 and the
        # drift_threshold is 0.05, then the rebalance will be triggered when the assets current_weight
        # is less than 0.25 or greater than 0.35.
        "drift_threshold": "0.05",

        # This is the acceptable slippage that will be used when calculating the number of shares to buy or sell.
        # The default is 0.005 (50 BPS)
        "acceptable_slippage": "0.005",  # 50 BPS

         # The amount of time to sleep between the sells and buys to give enough time for the orders to fill
        "fill_sleeptime": 15,

        # The target weights for each asset in the portfolio. You can put the quote asset in here (or not).
        "target_weights": {
            "SPY": "0.60",
            "TLT": "0.40",
            "USD": "0.00",
        }

        # If you want to allow shorting, set this to True.
        shorting: False
    }
    """

    # noinspection PyAttributeOutsideInit
    def initialize(self, parameters: Any = None) -> None:
        self.set_market(self.parameters.get("market", "NYSE"))
        self.sleeptime = self.parameters.get("sleeptime", "1D")
        self.drift_threshold = Decimal(self.parameters.get("drift_threshold", "0.05"))
        self.acceptable_slippage = Decimal(self.parameters.get("acceptable_slippage", "0.005"))
        self.fill_sleeptime = self.parameters.get("fill_sleeptime", 15)
        self.target_weights = {k: Decimal(v) for k, v in self.parameters["target_weights"].items()}
        self.shorting = self.parameters.get("shorting", False)
        self.drift_df = pd.DataFrame()

        # Load the components
        self.drift_calculation_logic = DriftCalculationLogic(self)
        self.rebalancer_logic = LimitOrderDriftRebalancerLogic(
            strategy=self,
            drift_threshold=self.drift_threshold,
            fill_sleeptime=self.fill_sleeptime,
            acceptable_slippage=self.acceptable_slippage,
            shorting=self.shorting
        )

    # noinspection PyAttributeOutsideInit
    def on_trading_iteration(self) -> None:
        dt = self.get_datetime()
        msg = f"{dt} on_trading_iteration called"
        self.logger.info(msg)
        self.log_message(msg, broadcast=True)
        self.cancel_open_orders()

        if self.cash < 0:
            self.logger.error(
                f"Negative cash: {self.cash} "
                f"but DriftRebalancer does not support margin yet."
            )

        self.drift_df = self.drift_calculation_logic.calculate(target_weights=self.target_weights)
        rebalance_needed = self.rebalancer_logic.rebalance(drift_df=self.drift_df)

        if rebalance_needed:
            msg = f"Rebalancing portfolio."
            self.logger.info(msg)
            self.log_message(msg, broadcast=True)

    def on_abrupt_closing(self):
        dt = self.get_datetime()
        self.logger.info(f"{dt} on_abrupt_closing called")
        self.log_message("On abrupt closing called.", broadcast=True)
        self.cancel_open_orders()

    def on_bot_crash(self, error):
        dt = self.get_datetime()
        self.logger.info(f"{dt} on_bot_crash called")
        self.log_message(f"Bot crashed with error: {error}", broadcast=True)
        self.cancel_open_orders()

