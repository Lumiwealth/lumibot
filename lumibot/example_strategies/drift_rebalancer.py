import pandas as pd
from typing import Any, Union
from decimal import Decimal

from lumibot.strategies.strategy import Strategy
from lumibot.components.drift_rebalancer_logic import DriftRebalancerLogic, DriftType
from lumibot.entities import Order, Asset


class DriftRebalancer(Strategy):
    """The DriftRebalancer strategy rebalances a portfolio based on drift from target weights.

    The strategy calculates the drift of each asset in the portfolio and triggers a rebalance if the drift exceeds
    the drift_threshold. The strategy will sell assets that have drifted above the threshold and
    buy assets that have drifted below the threshold.

    Notes:

    1. If you run this strategy in a live trading environment, be sure to not make manual trades in the same account.
    This strategy will sell other positions in order to get the account to the target weights.
    2. The quote asset of the strategy must be USD. Other quote assets are untested (though might work).
    3. Trading crypto is supported so long as the quote asset for each pair is USD.

    Example parameters:

    parameters = {
        "market": "NYSE",
        "sleeptime": "1D",
        "drift_type": DriftType.RELATIVE,
        "drift_threshold": "0.1",
        "order_type": Order.OrderType.MARKET,
        "acceptable_slippage": "0.005",  # 50 BPS
        "fill_sleeptime": 15,
        "portfolio_weights": [
            {
                "base_asset": Asset(symbol='SPY', asset_type='stock'),
                "weight": Decimal("0.6")
            },
            {
                "base_asset": Asset(symbol='TLT', asset_type='stock'),
                "weight": Decimal("0.4")
            }
        ],
        "shorting": False,
        "fractional_shares": False,
        "only_rebalance_drifted_assets": False,
    }

    Description of parameters:


    - market: The market to trade in. Default is "NYSE".
    - sleeptime: The time to sleep between trading iterations. Default is "1D".
    - drift_type: The type of drift calculation to use. Can be "absolute" or "relative". Default is DriftType.ABSOLUTE.
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
    - drift_threshold: The drift threshold that will trigger a rebalance. Default is Decimal("0.05").
        If the drift_type is absolute, the target_weight of an asset is 0.30 and the drift_threshold is 0.05,
        then a rebalance will be triggered when the asset's current_weight is less than 0.25 or greater than 0.35.
        If the drift_type is relative, the target_weight of an asset is 0.30 and the drift_threshold is 0.05,
        then a rebalance will be triggered when the asset's current_weight is less than -0.285 or greater than 0.315.
    - order_type: The type of order to use. Can be Order.OrderType.LIMIT or Order.OrderType.MARKET. Default is Order.OrderType.LIMIT.
    - acceptable_slippage: The acceptable slippage that will be used when calculating the number of shares to buy or sell. Default is Decimal("0.005") (50 BPS).
    - fill_sleeptime: The amount of time to sleep between the sells and buys to give enough time for the orders to fill. Default is 15.
    - portfolio_weights: A list of dictionaries containing the base_asset and weight of each asset in the portfolio.
    - shorting: If you want to allow shorting, set this to True. Default is False.
    - fractional_shares: If you want to allow fractional shares, set this to True. Default is False.
    - only_rebalance_drifted_assets: If you want to only rebalance assets that have drifted, set this to True. Default is False.

    """

    # noinspection PyAttributeOutsideInit
    def initialize(self, parameters: Any = None) -> None:
        self.set_market(self.parameters.get("market", "NYSE"))
        self.sleeptime = self.parameters.get("sleeptime", "1D")
        self.drift_type = self.parameters.get("drift_type", DriftType.RELATIVE)
        self.drift_threshold = Decimal(self.parameters.get("drift_threshold", "0.10"))
        self.order_type = self.parameters.get("order_type", Order.OrderType.MARKET)
        self.acceptable_slippage = Decimal(self.parameters.get("acceptable_slippage", "0.005"))
        self.fill_sleeptime = self.parameters.get("fill_sleeptime", 15)
        self.portfolio_weights = self.parameters.get("portfolio_weights", {})
        self.shorting = self.parameters.get("shorting", False)
        self.fractional_shares = self.parameters.get("fractional_shares", False)
        self.only_rebalance_drifted_assets = self.parameters.get("only_rebalance_drifted_assets", False)
        self.drift_df = pd.DataFrame()
        self.drift_rebalancer_logic = DriftRebalancerLogic(
            strategy=self,
            drift_type=self.drift_type,
            drift_threshold=self.drift_threshold,
            order_type=self.order_type,
            acceptable_slippage=self.acceptable_slippage,
            fill_sleeptime=self.fill_sleeptime,
            shorting=self.shorting,
            fractional_shares=self.fractional_shares,
            only_rebalance_drifted_assets=self.only_rebalance_drifted_assets,
        )

        # Always include cash_positions or else there will be no cash buy stuff with.
        self.include_cash_positions = True

    # noinspection PyAttributeOutsideInit
    def on_trading_iteration(self) -> None:
        dt = self.get_datetime()
        self.logger.info(f"{dt} on_trading_iteration called")
        self.cancel_open_orders()

        if self.cash < 0:
            self.logger.error(
                f"Negative cash: {self.cash} "
                f"but DriftRebalancer does not support margin yet."
            )

        self.drift_df = self.drift_rebalancer_logic.calculate(portfolio_weights=self.portfolio_weights)
        self.drift_rebalancer_logic.rebalance(drift_df=self.drift_df)