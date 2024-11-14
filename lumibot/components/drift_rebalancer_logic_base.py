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
            fill_sleeptime: int = 15,
            acceptable_slippage: Decimal = Decimal("0.005"),
            shorting: bool = False
    ) -> None:
        self.strategy = strategy
        self.fill_sleeptime = fill_sleeptime
        self.acceptable_slippage = acceptable_slippage
        self.shorting = shorting

    def rebalance(self, drift_df: pd.DataFrame = None) -> bool:
        if drift_df is None:
            raise ValueError("You must pass in a DataFrame to DriftRebalancerLogicBase.rebalance()")
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
            if abs(row["drift"]) > self.strategy.drift_threshold:
                rebalance_needed = True
                msg += (
                    f" Absolute drift exceeds threshold of {self.strategy.drift_threshold:.2%}. Rebalance needed."
                )
            self.strategy.logger.info(msg)
            self.strategy.log_message(msg, broadcast=True)
        return rebalance_needed
