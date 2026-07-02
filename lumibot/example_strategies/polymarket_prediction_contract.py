import os
from datetime import datetime, timezone

from lumibot.backtesting import PolymarketBacktesting
from lumibot.brokers import Polymarket
from lumibot.entities import Asset, Order
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class PolymarketPredictionContractStrategy(Strategy):
    """Polymarket prediction-contract strategy for live smoke tests and backtesting."""

    def initialize(self, parameters=None):
        strategy_parameters = dict(getattr(self, "parameters", {}) or {})
        if isinstance(parameters, dict):
            strategy_parameters.update(parameters)
        self.sleeptime = strategy_parameters.get("sleeptime", "1M")
        token_id = strategy_parameters.get("token_id") or os.environ.get("POLYMARKET_TEST_TOKEN_ID")
        if not token_id:
            raise ValueError("Set POLYMARKET_TEST_TOKEN_ID or pass parameters={'token_id': ...}")
        self.asset = Asset(token_id, asset_type=Asset.AssetType.PREDICTION_CONTRACT, precision="0.000001")
        self.live_trade = str(strategy_parameters.get("live_trade", "false")).lower() in {"1", "true", "yes", "on"}
        self.backtest_trade = str(strategy_parameters.get("backtest_trade", "false")).lower() in {"1", "true", "yes", "on"}
        self.backtest_quantity = strategy_parameters.get("backtest_quantity", 10)
        self.amount = str(strategy_parameters.get("amount", os.environ.get("POLYMARKET_TEST_ORDER_AMOUNT", "1.00")))
        self.max_price = str(strategy_parameters.get("max_price", os.environ.get("POLYMARKET_TEST_MAX_PRICE", "0.99")))

    def on_trading_iteration(self):
        quote = self.get_quote(self.asset)
        self.log_message(f"Polymarket quote bid={quote.bid} ask={quote.ask} mid={quote.mid_price}")

        if self.is_backtesting and self.backtest_trade and self.first_iteration:
            order = self.create_order(
                self.asset,
                quantity=self.backtest_quantity,
                side=Order.OrderSide.BUY,
                order_type=Order.OrderType.MARKET,
            )
            self.submit_order(order)
            return

        if not self.live_trade or not self.first_iteration:
            return

        order = self.create_order(
            self.asset,
            quantity=1,
            side=Order.OrderSide.BUY,
            order_type=Order.OrderType.MARKET,
            time_in_force="fak",
            custom_params={"amount": self.amount, "max_price": self.max_price, "order_type": "FAK"},
        )
        self.submit_order(order)


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    token_id = os.environ.get("POLYMARKET_TEST_TOKEN_ID")
    if not token_id:
        raise SystemExit("Set POLYMARKET_TEST_TOKEN_ID before running this example.")

    if _env_flag("IS_BACKTESTING"):
        os.environ["BACKTESTING_DATA_SOURCE"] = "polymarket"
        start = datetime.fromisoformat(os.environ.get("BACKTESTING_START", "2026-06-01")).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(os.environ.get("BACKTESTING_END", "2026-06-02")).replace(tzinfo=timezone.utc)
        PolymarketPredictionContractStrategy.backtest(
            PolymarketBacktesting,
            start,
            end,
            assets=[Asset(token_id, asset_type=Asset.AssetType.PREDICTION_CONTRACT, precision="0.000001")],
            market="24/7",
            timestep="minute",
            parameters={"token_id": token_id, "backtest_trade": True},
            benchmark_asset=None,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            save_stats_file=False,
            show_progress_bar=False,
        )
    else:
        live_trade = _env_flag("POLYMARKET_LIVE_TRADING_ENABLED") and _env_flag("POLYMARKET_EXAMPLE_LIVE_TRADE")
        broker = Polymarket()
        strategy = PolymarketPredictionContractStrategy(
            broker=broker,
            budget=100000,
            parameters={"token_id": token_id, "live_trade": live_trade},
        )
        trader = Trader()
        trader.add_strategy(strategy)
        trader.run_all()
