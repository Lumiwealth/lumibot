import pandas as pd
from typing import Dict, Any
from decimal import Decimal


from lumibot.strategies.strategy import Strategy


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

