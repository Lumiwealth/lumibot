import pandas as pd
from typing import Dict, Any
from decimal import Decimal, ROUND_DOWN
import time

from lumibot.strategies.strategy import Strategy
from lumibot.components.drift_rebalancer_logic_base import DriftRebalancerLogicBase


class LimitOrderDriftRebalancerLogic(DriftRebalancerLogicBase):
    
    def __init__(
            self,
            *,
            strategy: Strategy,
            drift_threshold: Decimal = Decimal("0.05"),
            fill_sleeptime: int = 15,
            acceptable_slippage: Decimal = Decimal("0.005"),
            shorting: bool = False
    ) -> None:
        super().__init__(
            strategy=strategy,
            drift_threshold=drift_threshold,
            fill_sleeptime=fill_sleeptime,
            acceptable_slippage=acceptable_slippage,
            shorting=shorting
        )

    def _rebalance(self, df: pd.DataFrame = None) -> None:
        if df is None:
            raise ValueError("You must pass in a DataFrame to LimitOrderDriftRebalancerLogic.rebalance()")
        
        # Execute sells first
        sell_orders = []
        buy_orders = []
        for index, row in df.iterrows():
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
            try:
                for order in sell_orders:
                    pulled_order = self.strategy.broker._pull_order(order.identifier, self.strategy.name, self.strategy)
                    msg = f"Submitted order status: {pulled_order}"
                    self.strategy.logger.info(msg)
                    self.strategy.log_message(msg, broadcast=True)
            except Exception as e:
                self.strategy.logger.error(f"Error pulling order: {e}")

        # Get current cash position from the broker
        cash_position = self.get_current_cash_position()

        # Execute buys
        for index, row in df.iterrows():
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
            # Sleep to allow sell orders to fill
            time.sleep(self.fill_sleeptime)
            try:
                for order in buy_orders:
                    pulled_order = self.strategy.broker._pull_order(order.identifier, self.strategy.name, self.strategy)
                    msg = f"Submitted order status: {pulled_order}"
                    self.strategy.logger.info(msg)
                    self.strategy.log_message(msg, broadcast=True)
            except Exception as e:
                self.strategy.logger.error(f"Error pulling order: {e}")

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
