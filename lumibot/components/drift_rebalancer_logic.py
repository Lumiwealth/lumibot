from abc import ABC, abstractmethod
from typing import Dict, Any
from decimal import Decimal, ROUND_DOWN
import time

import pandas as pd

from lumibot.strategies.strategy import Strategy


class DriftRebalancerLogic:

    def __init__(
            self,
            *,
            strategy: Strategy,
            drift_threshold: Decimal = Decimal("0.05"),
            fill_sleeptime: int = 15,
            acceptable_slippage: Decimal = Decimal("0.005"),
            shorting: bool = False
    ) -> None:
        self.strategy = strategy
        self.drift_threshold = drift_threshold
        self.fill_sleeptime = fill_sleeptime
        self.acceptable_slippage = acceptable_slippage
        self.shorting = shorting

        # Load the components
        self.drift_calculation_logic = DriftCalculationLogic(strategy=strategy)
        self.rebalancer_logic = LimitOrderDriftRebalancerLogic(
            strategy=strategy,
            drift_threshold=self.drift_threshold,
            fill_sleeptime=self.fill_sleeptime,
            acceptable_slippage=self.acceptable_slippage,
            shorting=self.shorting
        )

    def calculate(self, target_weights: Dict[str, Decimal]) -> pd.DataFrame:
        return self.drift_calculation_logic.calculate(target_weights)

    def rebalance(self, drift_df: pd.DataFrame = None) -> bool:
        return self.rebalancer_logic.rebalance(drift_df)


class DriftCalculationLogic:

    def __init__(self, strategy: Strategy) -> None:
        self.strategy = strategy
        self.df = pd.DataFrame()

    def calculate(self, target_weights: Dict[str, Decimal]) -> pd.DataFrame:
        self.df = pd.DataFrame({
            "symbol": target_weights.keys(),
            "is_quote_asset": False,
            "current_quantity": Decimal(0),
            "current_value": Decimal(0),
            "current_weight": Decimal(0),
            "target_weight": [Decimal(weight) for weight in target_weights.values()],
            "target_value": Decimal(0),
            "drift": Decimal(0)
        })

        self._add_positions()
        return self._calculate_drift().copy()

    def _add_positions(self) -> None:
        # Get all positions and add them to the calculator
        positions = self.strategy.get_positions()
        for position in positions:
            symbol = position.symbol
            current_quantity = Decimal(position.quantity)
            if position.asset == self.strategy.quote_asset:
                is_quote_asset = True
                current_value = Decimal(position.quantity)
            else:
                is_quote_asset = False
                current_value = Decimal(self.strategy.get_last_price(symbol)) * current_quantity
            self._add_position(
                symbol=symbol,
                is_quote_asset=is_quote_asset,
                current_quantity=current_quantity,
                current_value=current_value
            )

    def _add_position(
            self,
            *,
            symbol: str,
            is_quote_asset: bool,
            current_quantity: Decimal,
            current_value: Decimal
    ) -> None:
        if symbol in self.df["symbol"].values:
            self.df.loc[self.df["symbol"] == symbol, "is_quote_asset"] = is_quote_asset
            self.df.loc[self.df["symbol"] == symbol, "current_quantity"] = current_quantity
            self.df.loc[self.df["symbol"] == symbol, "current_value"] = current_value
        else:
            new_row = {
                "symbol": symbol,
                "is_quote_asset": is_quote_asset,
                "current_quantity": current_quantity,
                "current_value": current_value,
                "current_weight": Decimal(0),
                "target_weight": Decimal(0),
                "target_value": Decimal(0),
                "drift": Decimal(0)
            }
            # Convert the dictionary to a DataFrame
            new_row_df = pd.DataFrame([new_row])

            # Concatenate the new row to the existing DataFrame
            self.df = pd.concat([self.df, new_row_df], ignore_index=True)

    def _calculate_drift(self) -> pd.DataFrame:
        """
        A positive drift means we need to buy more of the asset,
        a negative drift means we need to sell some of the asset.
        """
        total_value = self.df["current_value"].sum()
        self.df["current_weight"] = self.df["current_value"] / total_value
        self.df["target_value"] = self.df["target_weight"] * total_value

        def calculate_drift_row(row: pd.Series) -> Decimal:
            if row["is_quote_asset"]:
                # We can never buy or sell the quote asset
                return Decimal(0)

            # Check if we should sell everything
            elif row["current_quantity"] > Decimal(0) and row["target_weight"] == Decimal(0):
                return Decimal(-1)

            # Check if we need to buy for the first time
            elif row["current_quantity"] == Decimal(0) and row["target_weight"] > Decimal(0):
                return Decimal(1)

            # Otherwise we just need to adjust our holding
            else:
                return row["target_weight"] - row["current_weight"]

        self.df["drift"] = self.df.apply(calculate_drift_row, axis=1)
        return self.df.copy()


class DriftRebalancerLogicBase(ABC):

    def __init__(
            self,
            *,
            strategy: Strategy,
            drift_threshold: Decimal = Decimal("0.05"),
            fill_sleeptime: int = 15,
            acceptable_slippage: Decimal = Decimal("0.005"),
            shorting: bool = False
    ) -> None:
        self.strategy = strategy
        self.drift_threshold = drift_threshold
        self.fill_sleeptime = fill_sleeptime
        self.acceptable_slippage = acceptable_slippage
        self.shorting = shorting

        # Sanity checks
        if self.acceptable_slippage >= self.drift_threshold:
            raise ValueError("acceptable_slippage must be less than drift_threshold")
        if self.drift_threshold >= Decimal("1.0"):
            raise ValueError("drift_threshold must be less than 1.0")

    def rebalance(self, drift_df: pd.DataFrame = None) -> bool:
        if drift_df is None:
            raise ValueError("You must pass in a DataFrame to DriftRebalancerLogicBase.rebalance()")

        # Get the target weights and make sure they are all less than the drift threshold
        target_weights = {k: Decimal(v) for k, v in self.strategy.target_weights.items()}
        for key, target_weight in target_weights.items():
            if self.drift_threshold >= target_weight:
                self.strategy.logger.warning(
                    f"drift_threshold of {self.drift_threshold} is "
                    f">= target_weight of {key}: {target_weight}. Drift in this asset will never trigger a rebalance."
                )

        rebalance_needed = self._check_if_rebalance_needed(drift_df)
        if rebalance_needed:
            self._rebalance(drift_df)
        return rebalance_needed

    @abstractmethod
    def _rebalance(self, drift_df: pd.DataFrame = None) -> None:
        raise NotImplementedError("You must implement _rebalance() in your subclass.")

    def _check_if_rebalance_needed(self, drift_df: pd.DataFrame) -> bool:
        # Check if the absolute value of any drift is greater than the threshold
        rebalance_needed = False
        for index, row in drift_df.iterrows():
            msg = (
                f"Symbol: {row['symbol']} current_weight: {row['current_weight']:.2%} "
                f"target_weight: {row['target_weight']:.2%} drift: {row['drift']:.2%}"
            )
            if abs(row["drift"]) > self.drift_threshold:
                rebalance_needed = True
                msg += (
                    f" Absolute drift exceeds threshold of {self.drift_threshold:.2%}. Rebalance needed."
                )
            self.strategy.logger.info(msg)
            self.strategy.log_message(msg, broadcast=True)
        return rebalance_needed


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
                quantity = ((row["current_value"] - row["target_value"]) / limit_price).quantize(Decimal('1'),
                                                                                                 rounding=ROUND_DOWN)
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
                    order = self.place_limit_order(symbol=symbol, quantity=quantity, limit_price=limit_price,
                                                   side="buy")
                    buy_orders.append(order)
                    cash_position -= min(order_value, cash_position)
                else:
                    self.strategy.logger.info(
                        f"Ran out of cash to buy {symbol}. Cash: {cash_position} and limit_price: {limit_price:.2f}")

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
