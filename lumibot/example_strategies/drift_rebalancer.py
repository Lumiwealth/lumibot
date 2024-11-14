import pandas as pd
from typing import Dict, Any
from decimal import Decimal, ROUND_DOWN
import time

from lumibot.strategies.strategy import Strategy
from lumibot.components import DriftCalculationLogic

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
        self.drift_threshold = Decimal(self.parameters.get("drift_threshold", "0.20"))
        self.acceptable_slippage = Decimal(self.parameters.get("acceptable_slippage", "0.005"))
        self.fill_sleeptime = self.parameters.get("fill_sleeptime", 15)
        self.target_weights = {k: Decimal(v) for k, v in self.parameters["target_weights"].items()}
        self.shorting = self.parameters.get("shorting", False)
        self.drift_df = pd.DataFrame()

        # Sanity checks
        if self.acceptable_slippage >= self.drift_threshold:
            raise ValueError("acceptable_slippage must be less than drift_threshold")
        if self.drift_threshold >= Decimal("1.0"):
            raise ValueError("drift_threshold must be less than 1.0")
        for key, target_weight in self.target_weights.items():
            if self.drift_threshold >= target_weight:
                self.logger.warning(
                    f"drift_threshold of {self.drift_threshold} is "
                    f">= target_weight of {key}: {target_weight}. Drift in this asset will never trigger a rebalance."
                )

        # Load the components
        self.drift_calculation_logic = DriftCalculationLogic(self)

    # noinspection PyAttributeOutsideInit
    def on_trading_iteration(self) -> None:
        dt = self.get_datetime()
        msg = f"{dt} on_trading_iteration called"
        self.logger.info(msg)
        self.log_message(msg, broadcast=True)
        self.cancel_open_orders()

        if self.cash < 0:
            self.logger.error(f"Negative cash: {self.cash} but DriftRebalancer does not support short sales or margin yet.")

        self.drift_df = self.drift_calculation_logic.calculate(target_weights=self.target_weights)

        # Check if the absolute value of any drift is greater than the threshold
        rebalance_needed = False
        for index, row in self.drift_df.iterrows():
            msg = (
                f"Symbol: {row['symbol']} current_weight: {row['current_weight']:.2%} "
                f"target_weight: {row['target_weight']:.2%} drift: {row['drift']:.2%}"
            )
            if abs(row["drift"]) > self.drift_threshold:
                rebalance_needed = True
                msg += (
                    f" Absolute drift exceeds threshold of {self.drift_threshold:.2%}. Rebalance needed."
                )
            self.logger.info(msg)
            self.log_message(msg, broadcast=True)

        if rebalance_needed:
            msg = f"Rebalancing portfolio."
            self.logger.info(msg)
            self.log_message(msg, broadcast=True)
            rebalance_logic = LimitOrderRebalanceLogic(
                strategy=self,
                df=self.drift_df,
                fill_sleeptime=self.fill_sleeptime,
                acceptable_slippage=self.acceptable_slippage,
                shorting=self.shorting
            )
            rebalance_logic.rebalance()

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


class LimitOrderRebalanceLogic:
    def __init__(
            self,
            *,
            strategy: Strategy,
            df: pd.DataFrame,
            fill_sleeptime: int = 15,
            acceptable_slippage: Decimal = Decimal("0.005"),
            shorting: bool = False
    ) -> None:
        self.strategy = strategy
        self.df = df
        self.fill_sleeptime = fill_sleeptime
        self.acceptable_slippage = acceptable_slippage
        self.shorting = shorting

    def rebalance(self) -> None:
        # Execute sells first
        sell_orders = []
        buy_orders = []
        for index, row in self.df.iterrows():
            if row["drift"] == -1:
                # Sell everything
                symbol = row["symbol"]
                quantity = row["current_quantity"]
                last_price = Decimal(self.strategy.get_last_price(symbol))
                limit_price = self.calculate_limit_price(last_price=last_price, side="sell")
                if quantity > 0 or (quantity == 0 and self.shorting):
                    order = self.place_limit_order(
                        symbol=symbol,
                        quantity=quantity,
                        limit_price=limit_price,
                        side="sell"
                    )
                    sell_orders.append(order)

            elif row["drift"] < 0:
                symbol = row["symbol"]
                last_price = Decimal(self.strategy.get_last_price(symbol))
                limit_price = self.calculate_limit_price(last_price=last_price, side="sell")
                quantity = ((row["current_value"] - row["target_value"]) / limit_price).quantize(Decimal('1'), rounding=ROUND_DOWN)
                if quantity > 0 and (quantity < row["current_quantity"] or self.shorting):
                    order = self.place_limit_order(
                        symbol=symbol,
                        quantity=quantity,
                        limit_price=limit_price,
                        side="sell"
                    )
                    sell_orders.append(order)

        for order in sell_orders:
            self.strategy.logger.info(f"Submitted sell order: {order}")

        if not self.strategy.is_backtesting:
            # Sleep to allow sell orders to fill
            time.sleep(self.fill_sleeptime)
            orders = self.strategy.broker._pull_all_orders(self.strategy.name, self.strategy)
            for order in orders:
                msg = f"Submitted order status: {order}"
                self.strategy.logger.info(msg)
                self.strategy.log_message(msg, broadcast=True)

        # Get current cash position from the broker
        cash_position = self.get_current_cash_position()

        # Execute buys
        for index, row in self.df.iterrows():
            if row["drift"] > 0:
                symbol = row["symbol"]
                last_price = Decimal(self.strategy.get_last_price(symbol))
                limit_price = self.calculate_limit_price(last_price=last_price, side="buy")
                order_value = row["target_value"] - row["current_value"]
                quantity = (min(order_value, cash_position) / limit_price).quantize(Decimal('1'), rounding=ROUND_DOWN)
                if quantity > 0:
                    order = self.place_limit_order(symbol=symbol, quantity=quantity, limit_price=limit_price, side="buy")
                    buy_orders.append(order)
                    cash_position -= min(order_value, cash_position)
                else:
                    self.strategy.logger.info(f"Ran out of cash to buy {symbol}. Cash: {cash_position} and limit_price: {limit_price:.2f}")

        for order in buy_orders:
            self.strategy.logger.info(f"Submitted buy order: {order}")

        if not self.strategy.is_backtesting:
            # Sleep to allow orders to fill
            time.sleep(self.fill_sleeptime)
            orders = self.strategy.broker._pull_all_orders(self.strategy.name, self.strategy)
            for order in orders:
                msg = f"Submitted order status: {order}"
                self.strategy.logger.info(msg)
                self.strategy.log_message(msg, broadcast=True)

    def calculate_limit_price(self, *, last_price: Decimal, side: str) -> Decimal:
        if side == "sell":
            return last_price * (1 - self.acceptable_slippage)
        elif side == "buy":
            return last_price * (1 + self.acceptable_slippage)

    def get_current_cash_position(self) -> Decimal:
        self.strategy.update_broker_balances(force_update=True)
        return Decimal(self.strategy.cash)

    def place_limit_order(self, *, symbol: str, quantity: Decimal, limit_price: Decimal, side: str) -> Any:
        limit_order = self.strategy.create_order(
            asset=symbol,
            quantity=quantity,
            side=side,
            limit_price=float(limit_price)
        )
        return self.strategy.submit_order(limit_order)
