from typing import Dict, Any, List
from decimal import Decimal, ROUND_DOWN
import time

import pandas as pd

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.entities.order import Order
from lumibot.tools.pandas import prettify_dataframe_with_decimals


class DriftType:
    ABSOLUTE = "absolute"
    RELATIVE = "relative"


class DriftRebalancerLogic:
    """ DriftRebalancerLogic calculates the drift of each asset in a portfolio and rebalances the portfolio.

    The strategy calculates the drift of each asset in the portfolio and triggers a rebalance if the drift exceeds
    the drift_threshold. The strategy will sell assets if their weights have drifted above the threshold and
    buy assets whose weights have drifted below the threshold.

    The current version of the DriftRebalancer strategy only supports market and limit orders.

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

    fractional_shares : bool, optional
        When set to True, the strategy will only use fractional shares. The default is False, which means
        the strategy will only use whole shares.

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
            shorting: bool = False,
            fractional_shares: bool = False
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
            order_type=order_type,
            fractional_shares=fractional_shares
        )

    def calculate(self, portfolio_weights: List[Dict[str, Any]]) -> pd.DataFrame:
        """Return a dataframe with the drift of each asset in the portfolio.

        Parameters
        ----------

        portfolio_weights : List[Dict[str, Any]]
            A list of dictionaries with the target weights for each asset in the portfolio.
            Each dictionary should have the following keys:
            - base_asset: Asset
            - weight: Decimal

        Returns
        -------

        pd.DataFrame
            A DataFrame with the drift of each asset in the portfolio.


        Examples
        --------

        # Stock example
        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL"), "weight": Decimal("0.20")},
            {"base_asset": Asset(symbol="MSFT"), "weight": Decimal("0.30")},
            {"base_asset": Asset(symbol="GOOGL"), "weight": Decimal("0.50")}
        ]

        # Crypto example
        portfolio_weights = [
            {"base_asset": Asset(symbol="BTC", asset_type="crypto"), "weight": Decimal("0.60")},
            {"base_asset": Asset(symbol="ETH", asset_type="crypto"), "weight": Decimal("0.40")}
        ]

        """
        return self.calculation_logic.calculate(portfolio_weights)

    def rebalance(self, drift_df: pd.DataFrame = None) -> bool:
        return self.order_logic.rebalance(drift_df)


class DriftCalculationLogic:

    def __init__(
            self,
            *,
            strategy: Strategy,
            drift_type: DriftType = DriftType.ABSOLUTE,
            drift_threshold: Decimal = Decimal("0.05"),
    ) -> None:
        self.strategy = strategy
        self.drift_type = drift_type
        self.drift_threshold = drift_threshold
        self.df = pd.DataFrame()

    def calculate(self, portfolio_weights: List[Dict[str, Any]]) -> pd.DataFrame:

        if self.drift_type == DriftType.ABSOLUTE:
            # The absolute value of all the weights are less than the drift_threshold
            # then we will never trigger a rebalance.
            if all([abs(item['weight']) < self.drift_threshold for item in portfolio_weights]):
                self.strategy.logger.warning(
                    f"All target weights are less than the drift_threshold: {self.drift_threshold}. "
                    f"No rebalance will be triggered."
                )

        self.df = pd.DataFrame({
            "symbol": [item['base_asset'].symbol for item in portfolio_weights],
            "base_asset": [item['base_asset'] for item in portfolio_weights],
            "is_quote_asset": False,
            "current_quantity": Decimal(0),
            "current_value": Decimal(0),
            "current_weight": Decimal(0),
            "target_weight": [Decimal(item['weight']) for item in portfolio_weights],
            "target_value": Decimal(0),
            "drift": Decimal(0)
        })

        self._add_positions()
        return self._calculate_drift().copy()

    def _add_positions(self) -> None:
        positions = self.strategy.get_positions()
        for position in positions:
            symbol = position.symbol
            current_quantity = Decimal(position.quantity)
            if position.asset == self.strategy.quote_asset:
                is_quote_asset = True
                current_value = Decimal(position.quantity)
            else:
                is_quote_asset = False
                last_price = Decimal(self.strategy.get_last_price(position.asset))
                current_value = current_quantity * last_price
            self._add_position(
                symbol=symbol,
                base_asset=position.asset,
                is_quote_asset=is_quote_asset,
                current_quantity=current_quantity,
                current_value=current_value
            )

    def _add_position(
            self,
            *,
            symbol: str,
            base_asset: Asset,
            is_quote_asset: bool,
            current_quantity: Decimal,
            current_value: Decimal
    ) -> None:
        if symbol in self.df["symbol"].values:
            self.df.loc[self.df["symbol"] == symbol, "base_asset"] = base_asset
            self.df.loc[self.df["symbol"] == symbol, "is_quote_asset"] = is_quote_asset
            self.df.loc[self.df["symbol"] == symbol, "current_quantity"] = current_quantity
            self.df.loc[self.df["symbol"] == symbol, "current_value"] = current_value
        else:
            new_row = {
                "symbol": symbol,
                "base_asset": base_asset,
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
            # Do nothing
            return Decimal(0)

        elif row["current_quantity"] > Decimal(0) and row["target_weight"] == Decimal(0):
            # Sell everything
            return Decimal(-1)

        elif row["current_quantity"] < Decimal(0) and row["target_weight"] == Decimal(0):
            # Cover our short position
            return Decimal(1)

        elif row["current_quantity"] == Decimal(0) and row["target_weight"] > Decimal(0):
            # We don't have any of this asset, but we want to buy some.
            return Decimal(1)

        elif row["current_quantity"] == Decimal(0) and row["target_weight"] == Decimal(-1):
            # Short everything we have
            return Decimal(-1)

        elif row["current_quantity"] == Decimal(0) and row["target_weight"] < Decimal(0):
            # We don't have any of this asset, but we want to short some.
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
            order_type: Order.OrderType = Order.OrderType.LIMIT,
            fractional_shares: bool = False
    ) -> None:
        self.strategy = strategy
        self.drift_threshold = drift_threshold
        self.fill_sleeptime = fill_sleeptime
        self.acceptable_slippage = acceptable_slippage
        self.shorting = shorting
        self.order_type = order_type
        self.fractional_shares = fractional_shares

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

        # Just print the drift_df to the log but sort it by symbol column
        drift_df = drift_df.sort_values(by='symbol')
        self.strategy.logger.info(f"drift_df:\n{prettify_dataframe_with_decimals(df=drift_df)}")

        rebalance_needed = self._check_if_rebalance_needed(drift_df)
        if rebalance_needed:
            self._rebalance(drift_df)
        return rebalance_needed

    def _rebalance(self, df: pd.DataFrame = None) -> None:
        if df is None:
            raise ValueError("You must pass in a DataFrame to DriftOrderLogic.rebalance()")

        # sort dataframe by the largest absolute value drift first
        df = df.reindex(df["drift"].abs().sort_values(ascending=False).index)

        # Execute sells first
        sell_orders = []
        buy_orders = []
        for index, row in df.iterrows():
            if row["drift"] == -1:
                # Sell everything (or create 100% short position)
                base_asset = row["base_asset"]
                quantity = row["current_quantity"]
                last_price = Decimal(self.strategy.get_last_price(base_asset))
                limit_price = self.calculate_limit_price(last_price=last_price, side="sell")
                if quantity == 0 and self.shorting:
                    # Create a 100% short position.
                    total_value = df["current_value"].sum()
                    if self.fractional_shares:
                        quantity = total_value / limit_price
                        quantity = quantity.quantize(Decimal('1.000000000'))
                    else:
                        quantity = total_value // limit_price
                if quantity > 0:
                    order = self.place_order(
                        base_asset=base_asset,
                        quantity=quantity,
                        limit_price=limit_price,
                        side="sell"
                    )
                    sell_orders.append(order)

            elif row["drift"] < 0:
                base_asset = row["base_asset"]
                last_price = Decimal(self.strategy.get_last_price(base_asset))
                limit_price = self.calculate_limit_price(last_price=last_price, side="sell")
                quantity = (row["current_value"] - row["target_value"]) / limit_price
                if self.fractional_shares:
                    quantity = quantity.quantize(Decimal('1.000000000'))
                else:
                    quantity = quantity.quantize(Decimal('1'), rounding=ROUND_DOWN)

                if (0 < quantity < row["current_quantity"]) or (quantity > 0 and self.shorting):
                    # If we are not shorting, we can only sell what we have.
                    order = self.place_order(
                        base_asset=base_asset,
                        quantity=quantity,
                        limit_price=limit_price,
                        side="sell"
                    )
                    sell_orders.append(order)

        if not self.strategy.is_backtesting:
            # Sleep to allow sell orders to fill
            time.sleep(self.fill_sleeptime)

        for order in sell_orders:
            self.strategy.logger.info(f"Submitted sell order: {order}")

        # Get current cash position from the broker
        cash_position = self.get_current_cash_position()

        # Execute buys
        for index, row in df.iterrows():
            if row["drift"] == 1 and row['current_quantity'] < 0 and self.shorting:
                # Cover our short position
                base_asset = row["base_asset"]
                quantity = abs(row["current_quantity"])
                last_price = Decimal(self.strategy.get_last_price(base_asset))
                limit_price = self.calculate_limit_price(last_price=last_price, side="buy")
                order = self.place_order(
                    base_asset=base_asset,
                    quantity=quantity,
                    limit_price=limit_price,
                    side="buy"
                )
                buy_orders.append(order)
                cash_position -= quantity * limit_price

            elif row["drift"] > 0:
                base_asset = row["base_asset"]
                last_price = Decimal(self.strategy.get_last_price(base_asset))
                limit_price = self.calculate_limit_price(last_price=last_price, side="buy")
                order_value = row["target_value"] - row["current_value"]
                quantity = min(order_value, cash_position) / limit_price
                if self.fractional_shares:
                    quantity = quantity.quantize(Decimal('1.000000000'))
                else:
                    quantity = quantity.quantize(Decimal('1'), rounding=ROUND_DOWN)

                if quantity > 0:
                    order = self.place_order(
                        base_asset=base_asset,
                        quantity=quantity,
                        limit_price=limit_price,
                        side="buy"
                    )
                    buy_orders.append(order)
                    cash_position -= quantity * limit_price
                else:
                    self.strategy.logger.info(
                        f"Ran out of cash to buy {quantity} of {base_asset.symbol}. "
                        f"Cash: {cash_position} and limit_price: {limit_price:.2f}"
                    )

        for order in buy_orders:
            self.strategy.logger.info(f"Submitted buy order: {order}")

    def calculate_limit_price(self, *, last_price: Decimal, side: str) -> Decimal:
        if side == "sell":
            return last_price * (1 - self.acceptable_slippage)
        elif side == "buy":
            return last_price * (1 + self.acceptable_slippage)

    def get_current_cash_position(self) -> Decimal:
        self.strategy.update_broker_balances(force_update=True)
        return Decimal(self.strategy.cash)

    def place_order(
            self,
            base_asset: Asset,
            quantity: Decimal,
            limit_price: Decimal,
            side: str
    ) -> Order:
        quote_asset = self.strategy.quote_asset or Asset(symbol="USD", asset_type="forex")
        if self.order_type == Order.OrderType.LIMIT:
            order = self.strategy.create_order(
                asset=base_asset,
                quantity=quantity,
                side=side,
                limit_price=float(limit_price),
                quote=quote_asset
            )
        else:
            order = self.strategy.create_order(
                asset=base_asset,
                quantity=quantity,
                side=side,
                quote=quote_asset
            )

        self.strategy.submit_order(order)
        return order

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
                    f" Drift exceeds threshold."
                )
            self.strategy.logger.info(msg)

        return rebalance_needed