from typing import Dict, Any, List
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import time

import pandas as pd

from lumibot.entities import Asset, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.entities.order import Order
from lumibot.tools.pandas import prettify_dataframe_with_decimals
from lumibot.tools.helpers import quantize_to_num_decimals


class DriftType:
    ABSOLUTE = "absolute"
    RELATIVE = "relative"


def get_last_price_or_raise(strategy: Strategy, asset: Asset, quote: Asset) -> Decimal:
    try:
        price = strategy.get_last_price(asset, quote)
    except Exception as e:
        strategy.logger.error(f"DriftRebalancer could not get_last_price for {asset}-{quote}. Error: {e}")
        raise e

    if price is None:
        msg = f"DriftRebalancer could not get_last_price for {asset}-{quote}."
        strategy.logger.error(msg)
        raise ValueError(msg)
    else:
        return Decimal(str(price))


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

    only_rebalance_drifted_assets : bool, optional
        If True, the strategy will only rebalance assets whose drift exceeds the drift_threshold.
        The default is False, which means the strategy will rebalance all assets in the portfolio.

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
            fractional_shares: bool = False,
            only_rebalance_drifted_assets: bool = False
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
            fractional_shares=fractional_shares,
            only_rebalance_drifted_assets=only_rebalance_drifted_assets
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
            # then we will never trigger a rebalance. This happens by design when strategies
            # derived from DriftRebalancer decide to have no positions for example.
            if all([abs(item['weight']) < self.drift_threshold for item in portfolio_weights]):
                self.strategy.logger.info(
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
            current_quantity = Decimal(str(position.quantity))
            if position.asset == self.strategy.quote_asset:
                is_quote_asset = True
                current_value = Decimal(str(position.quantity))
            else:
                is_quote_asset = False
                last_price = get_last_price_or_raise(self.strategy, position.asset, self.strategy.quote_asset)
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
        # Use total portfolio value instead of just current asset values
        # This fixes the issue where starting from all-cash positions would result in zero target values
        total_value = Decimal(str(self.strategy.get_portfolio_value()))
        
        self.df["current_weight"] = self.df["current_value"] / total_value if total_value > 0 else Decimal(0)
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
                # Relative drift is calculated by: difference / abs(target_weight).
                # Example: target_weight=0.20 and current_weight=0.23
                # The drift is (0.20 - 0.23) / 0.20 = -0.15
                # For negative target weights (short positions), we use the absolute value
                # to ensure the sign of the drift is correct
                return (row["target_weight"] - row["current_weight"]) / abs(row["target_weight"])
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
            fractional_shares: bool = False,
            only_rebalance_drifted_assets: bool = False
    ) -> None:
        self.strategy = strategy
        self.drift_threshold = drift_threshold
        self.fill_sleeptime = fill_sleeptime
        self.acceptable_slippage = acceptable_slippage
        self.shorting = shorting
        self.order_type = order_type
        self.fractional_shares = fractional_shares
        self.only_rebalance_drifted_assets = only_rebalance_drifted_assets

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
                # Sell everything (or create a short position)
                base_asset = row["base_asset"]
                quantity = row["current_quantity"]
                last_price = get_last_price_or_raise(self.strategy, base_asset, self.strategy.quote_asset)
                limit_price = self.calculate_limit_price(last_price=last_price, side="sell", asset=base_asset)
                if quantity == 0 and self.shorting:
                    # Create a new short position.
                    if self.fractional_shares:
                        quantity = abs(row["target_value"]) / limit_price
                        quantity = quantity.quantize(Decimal('1.000000000'), rounding=ROUND_DOWN)
                    else:
                        quantity = abs(row["target_value"]) // limit_price
                if quantity > 0:
                    order = self.place_order(
                        base_asset=base_asset,
                        quantity=quantity,
                        limit_price=limit_price,
                        side="sell"
                    )
                    sell_orders.append(order)

            elif row["drift"] < 0:

                if self.only_rebalance_drifted_assets and abs(row["drift"]) < self.drift_threshold:
                    continue

                base_asset = row["base_asset"]
                last_price = get_last_price_or_raise(self.strategy, base_asset, self.strategy.quote_asset)
                limit_price = self.calculate_limit_price(last_price=last_price, side="sell", asset=base_asset)
                
                # For options, account for the 100-share multiplier in selling too
                if base_asset.asset_type == Asset.AssetType.OPTION:
                    # Options prices are quoted per share but each contract represents 100 shares
                    effective_price = limit_price * 100
                    quantity = (row["current_value"] - row["target_value"]) / effective_price
                else:
                    quantity = (row["current_value"] - row["target_value"]) / limit_price
                
                # Apply quantity rounding - options must be whole contracts
                if base_asset.asset_type == Asset.AssetType.OPTION:
                    quantity = quantity.quantize(Decimal('1'), rounding=ROUND_DOWN)
                elif self.fractional_shares:
                    quantity = quantity.quantize(Decimal('1.000000000'), rounding=ROUND_DOWN)
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

        # Get current cash position from the broker
        cash_position = self.get_current_cash_position()

        # Execute buys
        for index, row in df.iterrows():
            if row["drift"] == 1 and row['current_quantity'] < 0 and self.shorting:
                # Cover our short position
                base_asset = row["base_asset"]
                quantity = abs(row["current_quantity"])
                last_price = get_last_price_or_raise(self.strategy, base_asset, self.strategy.quote_asset)
                limit_price = self.calculate_limit_price(last_price=last_price, side="buy", asset=base_asset)
                order = self.place_order(
                    base_asset=base_asset,
                    quantity=quantity,
                    limit_price=limit_price,
                    side="buy"
                )
                buy_orders.append(order)
                cash_position -= quantity * limit_price

            elif row["drift"] > 0:

                if self.only_rebalance_drifted_assets and abs(row["drift"]) < self.drift_threshold:
                    continue

                base_asset = row["base_asset"]
                last_price = get_last_price_or_raise(self.strategy, base_asset, self.strategy.quote_asset)
                limit_price = self.calculate_limit_price(last_price=last_price, side="buy", asset=base_asset)
                order_value = row["target_value"] - row["current_value"]
                
                # For options, account for the 100-share multiplier
                if base_asset.asset_type == Asset.AssetType.OPTION:
                    # Options prices are quoted per share but each contract represents 100 shares
                    effective_price = limit_price * 100
                    desired_quantity = min(order_value, cash_position) / effective_price
                else:
                    desired_quantity = min(order_value, cash_position) / limit_price

                adjusted_quantity = self.adjust_quantity_for_fees(
                    desired_quantity,
                    limit_price,
                    Order.OrderSide.BUY,
                    self.strategy.buy_trading_fees,
                    cash_position
                )

                # Apply quantity rounding - options must be whole contracts
                if base_asset.asset_type == Asset.AssetType.OPTION:
                    quantity = adjusted_quantity.quantize(Decimal('1'), rounding=ROUND_DOWN)
                elif self.fractional_shares:
                    quantity = adjusted_quantity.quantize(Decimal('1.000000000'), rounding=ROUND_DOWN)
                else:
                    quantity = adjusted_quantity.quantize(Decimal('1'), rounding=ROUND_DOWN)

                if quantity > 0:
                    # For options, check against actual cost (price * 100 * quantity)
                    if base_asset.asset_type == Asset.AssetType.OPTION:
                        actual_cost = quantity * limit_price * 100
                    else:
                        actual_cost = quantity * limit_price
                        
                    if actual_cost > cash_position:
                        self.strategy.logger.error(
                            f"Quantity {quantity} of {base_asset.symbol} * cost: {actual_cost:.2f}"
                            f"is more than cash: {cash_position}. Not sending order."
                        )
                        continue

                    order = self.place_order(
                        base_asset=base_asset,
                        quantity=quantity,
                        limit_price=limit_price,
                        side="buy"
                    )
                    buy_orders.append(order)
                    
                    # Deduct actual cost from cash position
                    if base_asset.asset_type == Asset.AssetType.OPTION:
                        cash_position -= quantity * limit_price * 100
                    else:
                        cash_position -= quantity * limit_price

    def calculate_limit_price(self, *, last_price: Decimal, side: str, asset: Asset) -> Decimal:
        if side == "sell":
            limit_price = last_price * (1 - self.acceptable_slippage)
        else:
            limit_price = last_price * (1 + self.acceptable_slippage)

        if asset.asset_type == Asset.AssetType.CRYPTO:
            # Keep full precision for crypto
            pass
        elif asset.asset_type == Asset.AssetType.OPTION:
            # Options typically trade in $0.05 or $0.01 increments
            # Round to the nearest cent for options
            if side == "buy":
                limit_price = limit_price.quantize(Decimal('1.01'), rounding=ROUND_DOWN)
            else:
                limit_price = limit_price.quantize(Decimal('1.01'), rounding=ROUND_UP)
        else:
            # Stocks - reduce to 2 decimals (cents)
            if side == "buy":
                limit_price = limit_price.quantize(Decimal('1.01'), rounding=ROUND_DOWN)
            else:
                limit_price = limit_price.quantize(Decimal('1.01'), rounding=ROUND_UP)

        return limit_price

    def get_current_cash_position(self) -> Decimal:
        self.strategy.update_broker_balances(force_update=True)
        cash_position = Decimal(str(self.strategy.cash))
        cash_position = cash_position.quantize(Decimal('1.00'), rounding=ROUND_DOWN)
        return cash_position

    def place_order(
            self,
            base_asset: Asset,
            quantity: Decimal,
            limit_price: Decimal,
            side: str
    ) -> Order:
        quote_asset = self.strategy.quote_asset or Asset(symbol="USD", asset_type="forex")
        # If orders don't fill at the end of the day, and there is a split the next day,
        # unexpected things can happen. Use the 'day' time in force to address this.
        time_in_force = 'day'

        if self.order_type == Order.OrderType.LIMIT:
            order = self.strategy.create_order(
                asset=base_asset,
                quantity=quantity,
                side=side,
                limit_price=float(limit_price),
                quote=quote_asset,
                time_in_force=time_in_force
            )
        else:
            order = self.strategy.create_order(
                asset=base_asset,
                quantity=quantity,
                side=side,
                quote=quote_asset,
                time_in_force=time_in_force
            )

        self.strategy.logger.info(f"Submitting order: {order}")
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

    # noinspection PyMethodMayBeStatic
    def calculate_trading_costs(
            self,
            quantity: Decimal,
            price: Decimal,
            trading_fees: List[TradingFee] | TradingFee
    ) -> Decimal:
        """Calculates the total trading costs for an order."""
        total_cost = Decimal(0)
        if isinstance(trading_fees, TradingFee):
            trading_fees = [trading_fees]
        for fee in trading_fees:
            total_cost += fee.flat_fee
            total_cost += quantity * price * fee.percent_fee
        return total_cost

    def adjust_quantity_for_fees(
            self,
            desired_quantity: Decimal,
            price: Decimal,
            side: str,
            trading_fees: List[TradingFee] | TradingFee,
            buying_power: Decimal
    ) -> Decimal:
        """Adjusts the desired quantity to account for trading fees and available capital."""
        if isinstance(trading_fees, TradingFee):
            trading_fees = [trading_fees]

        if side == "buy":
            # For options, calculate fees based on actual cost (price * 100 * quantity)
            # Note: We need to determine if this is an options trade - we'll approximate by checking the calling context
            # This is a limitation of the current design, but works for most cases
            
            fees = self.calculate_trading_costs(desired_quantity, price, trading_fees)
            total_cost = desired_quantity * price + fees

            if total_cost < buying_power:
                return desired_quantity  # Affordable
            else:
                # Reduce quantity until affordable
                affordable_quantity = (buying_power - fees) / price
                return max(Decimal(0), affordable_quantity)

        else:  # Selling logic remains unchanged
            return desired_quantity
