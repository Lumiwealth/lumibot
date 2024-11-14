import pandas as pd
from typing import Dict, Any
from decimal import Decimal, ROUND_DOWN
import time
from abc import ABC, abstractmethod

from lumibot.strategies import Strategy


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
