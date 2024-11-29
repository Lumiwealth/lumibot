from abc import ABC, abstractmethod
from typing import Dict, Any
from decimal import Decimal, ROUND_DOWN
import time

import pandas as pd

from lumibot.strategies.strategy import Strategy
from lumibot.entities.order import Order


class DriftType:
    ABSOLUTE = "absolute"
    RELATIVE = "relative"


class DriftRebalancerLogic:
    """ DriftRebalancerLogic calculates the drift of each asset in a portfolio and rebalances the portfolio.

    The strategy calculates the drift of each asset in the portfolio and triggers a rebalance if the drift exceeds
    the drift_threshold. The strategy will sell assets if their weights have drifted above the threshold and
    buy assets whose weights have drifted below the threshold.

    The current version of the DriftRebalancer strategy only supports market and limit orders.
    The current version of the DriftRebalancer strategy only supports whole share quantities.
    Upvote an issue if you need fractional shares.

    Parameters
    ----------

    strategy : Strategy
        The strategy object that will be used to get the current positions and submit orders.

    drift_type : DriftType, optional
        The type of drift calculation to use. Can be "absolute" or "relative". The default is DriftType.ABSOLUTE.

        If the drift_type is "absolute", the drift is calculated as the difference between the target_weight
        and the current_weight. For example, if the target_weight is 0.20 and the current_weight is 0.23, the
        absolute drift would be 0.03.

        If the drift_type is "relative", the drift is calculated as the difference between the target_weight
        and the current_weight divided by the target_weight. For example, if the target_weight is 0.20 and the
        current_weight is 0.23, the relative drift would be (0.20 - 0.23) / 0.20 = -0.15.

        Absolute drift is better if you have assets with small weights but don't want changes in small positions to
        trigger a rebalance in your portfolio. If your target weights were like below, an absolute drift of 0.05 would
        only trigger a rebalance when asset3 or asset4 drifted by 0.05 or more.
        {
            "asset1": Decimal("0.025"),
            "asset2": Decimal("0.025"),
            "asset3": Decimal("0.40"),
            "asset4": Decimal("0.55"),
        }

        Relative drift can be useful when the target_weights are small or very different from each other, and you do
        want changes in small positions to trigger a rebalance. If your target weights were like above, a relative drift
        of 0.20 would trigger a rebalance when asset1 or asset2 drifted by 0.005 or more.

    drift_threshold : Decimal, optional
        The drift threshold that will trigger a rebalance.
        The default is Decimal("0.05").

        If the drift_type is absolute, the target_weight of an asset is 0.30 and the drift_threshold is 0.05,
        then a rebalance will be triggered when the asset's current_weight is less than 0.25 or greater than 0.35.

        If the drift_type is relative, the target_weight of an asset is 0.30 and the drift_threshold is 0.05,
        then a rebalance will be triggered when the asset's current_weight is less than -0.285 or greater than 0.315.

    order_type : Order.OrderType, optional
        The type of order to use. Can be Order.OrderType.LIMIT or Order.OrderType.MARKET.
        The default is Order.OrderType.LIMIT.

    fill_sleeptime : int, optional
        The amount of time to sleep between the sells and buys to give enough time for the orders to fill.
        The default is 15.

    acceptable_slippage : Decimal, optional
        The acceptable slippage that will be used when calculating the number of shares to buy or sell.
        The default is Decimal("0.005") (50 BPS).

    shorting : bool, optional
        If you want to allow shorting, set this to True. The default is False.

    """

    def __init__(
            self,
            *,
            strategy: Strategy,
            drift_type: DriftType = DriftType.ABSOLUTE,
            drift_threshold: Decimal = Decimal("0.1"),
            order_type: Order.OrderType = Order.OrderType.LIMIT,
            acceptable_slippage: Decimal = Decimal("0.005"),
            fill_sleeptime: int = 15,
            shorting: bool = False
    ) -> None:
        self.strategy = strategy
        self.calculation_logic = DriftCalculationLogic(
            strategy=strategy,
            drift_type=drift_type,
            drift_threshold=drift_threshold
        )
        self.order_logic = DriftOrderLogic(
            strategy=strategy,
            drift_threshold=drift_threshold,
            fill_sleeptime=fill_sleeptime,
            acceptable_slippage=acceptable_slippage,
            shorting=shorting,
            order_type=order_type
        )

    def calculate(self, target_weights: Dict[str, Decimal]) -> pd.DataFrame:
        return self.calculation_logic.calculate(target_weights)

    def rebalance(self, drift_df: pd.DataFrame = None) -> bool:
        return self.order_logic.rebalance(drift_df)


class DriftCalculationLogic:

    def __init__(
            self,
            *,
            strategy: Strategy,
            drift_type: DriftType = DriftType.ABSOLUTE,
            drift_threshold: Decimal = Decimal("0.05")
    ) -> None:
        self.strategy = strategy
        self.drift_type = drift_type
        self.drift_threshold = drift_threshold
        self.df = pd.DataFrame()

    def calculate(self, target_weights: Dict[str, Decimal]) -> pd.DataFrame:

        if self.drift_type == DriftType.ABSOLUTE:
            # Make sure the target_weights are all less than the drift threshold
            for key, target_weight in target_weights.items():
                if self.drift_threshold >= target_weight:
                    self.strategy.logger.warning(
                        f"drift_threshold of {self.drift_threshold} is "
                        f">= target_weight of {key}: {target_weight}. Drift in this asset will never trigger a rebalance."
                    )

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
        self.df["drift"] = self.df.apply(self._calculate_drift_row, axis=1)
        return self.df.copy()

    def _calculate_drift_row(self, row: pd.Series) -> Decimal:

        if row["is_quote_asset"]:
            # We can never buy or sell the quote asset
            return Decimal(0)

        elif row["current_weight"] == Decimal(0) and row["target_weight"] == Decimal(0):
            # Should nothing change?
            return Decimal(0)

        elif row["current_quantity"] > Decimal(0) and row["target_weight"] == Decimal(0):
            # Should we sell everything
            return Decimal(-1)

        elif row["current_quantity"] == Decimal(0) and row["target_weight"] > Decimal(0):
            # We don't have any of this asset but we wanna buy some.
            return Decimal(1)

        elif row["current_quantity"] == Decimal(0) and row["target_weight"] == Decimal(-1):
            # Should we short everything we have
            return Decimal(-1)

        elif row["current_quantity"] == Decimal(0) and row["target_weight"] < Decimal(0):
            # We don't have any of this asset but we wanna short some.
            return Decimal(-1)

        # Otherwise we just need to adjust our holding. Calculate the drift.
        else:
            if self.drift_type == DriftType.ABSOLUTE:
                return row["target_weight"] - row["current_weight"]
            elif self.drift_type == DriftType.RELATIVE:
                # Relative drift is calculated by: difference / target_weight.
                # Example: target_weight=0.20 and current_weight=0.23
                # The drift is (0.20 - 0.23) / 0.20 = -0.15
                return (row["target_weight"] - row["current_weight"]) / row["target_weight"]
            else:
                raise ValueError(f"Invalid drift_type: {self.drift_type}")


class DriftOrderLogic:

    def __init__(
            self,
            *,
            strategy: Strategy,
            drift_threshold: Decimal = Decimal("0.05"),
            fill_sleeptime: int = 15,
            acceptable_slippage: Decimal = Decimal("0.005"),
            shorting: bool = False,
            order_type: Order.OrderType = Order.OrderType.LIMIT
    ) -> None:
        self.strategy = strategy
        self.drift_threshold = drift_threshold
        self.fill_sleeptime = fill_sleeptime
        self.acceptable_slippage = acceptable_slippage
        self.shorting = shorting
        self.order_type = order_type

        # Sanity checks
        if self.acceptable_slippage >= self.drift_threshold:
            raise ValueError("acceptable_slippage must be less than drift_threshold")
        if self.drift_threshold >= Decimal("1.0"):
            raise ValueError("drift_threshold must be less than 1.0")
        if self.order_type not in [Order.OrderType.LIMIT, Order.OrderType.MARKET]:
            raise ValueError(f"Invalid order_type: {self.order_type}")

    def rebalance(self, drift_df: pd.DataFrame = None) -> bool:
        if drift_df is None:
            raise ValueError("You must pass in a DataFrame to DriftOrderLogic.rebalance()")

        rebalance_needed = self._check_if_rebalance_needed(drift_df)
        if rebalance_needed:
            self._rebalance(drift_df)
        return rebalance_needed

    def _rebalance(self, df: pd.DataFrame = None) -> None:
        if df is None:
            raise ValueError("You must pass in a DataFrame to DriftOrderLogic.rebalance()")

        # Execute sells first
        sell_orders = []
        buy_orders = []
        for index, row in df.iterrows():
            if row["drift"] == -1:
                # Sell everything (or create 100% short position)
                symbol = row["symbol"]
                quantity = row["current_quantity"]
                last_price = Decimal(self.strategy.get_last_price(symbol))
                limit_price = self.calculate_limit_price(last_price=last_price, side="sell")
                if quantity == 0 and self.shorting:
                    total_value = df["current_value"].sum()
                    quantity = total_value // limit_price
                if quantity > 0:
                    order = self.place_order(
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
                quantity = (
                        (row["current_value"] - row["target_value"]) / limit_price
                ).quantize(Decimal('1'), rounding=ROUND_DOWN)
                if (0 < quantity < row["current_quantity"]) or (quantity > 0 and self.shorting):
                    # If we are not shorting, we can only sell what we have.
                    order = self.place_order(
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
                    pulled_order = self.strategy.broker._pull_order(order.identifier, self.strategy.name)
                    msg = f"Status of submitted sell order: {pulled_order}"
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
                    order = self.place_order(symbol=symbol, quantity=quantity, limit_price=limit_price,
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
                    pulled_order = self.strategy.broker._pull_order(order.identifier, self.strategy.name)
                    msg = f"Status of submitted buy order: {pulled_order}"
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

    def place_order(self, *, symbol: str, quantity: Decimal, limit_price: Decimal, side: str) -> Any:
        if self.order_type == Order.OrderType.LIMIT:
            order = self.strategy.create_order(
                asset=symbol,
                quantity=quantity,
                side=side,
                limit_price=float(limit_price)
            )
        else:
            order = self.strategy.create_order(
                asset=symbol,
                quantity=quantity,
                side=side
            )
        return self.strategy.submit_order(order)

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
