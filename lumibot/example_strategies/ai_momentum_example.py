from datetime import datetime

from lumibot.brokers import Alpaca
from lumibot.credentials import ALPACA_TEST_CONFIG as ALPACA_CONFIG
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy


class AIMomentum(Strategy):
    parameters = {
        "ai_allow_trading": False,
    }

    def initialize(self):
        self.sleeptime = "1M"
        self.set_market("us_futures")
        self.asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)

        self.momentum = self.agents.create(
            name="momentum",
            prompt=(
                "You are an MNQ futures momentum agent. Decide buy/sell/hold;"
                " avoid overtrading; follow market hours."
            ),
            cadence="5m",
            allow_trading=self.parameters["ai_allow_trading"],
        )

    def on_ai_decision(self, agent_name, decision):
        # Optional place to tweak decisions before execution
        return None

    def on_ai_executed(self, agent_name, decision, execution_result):
        self.log_message(f"[AI:{agent_name}] Executed {execution_result}", color="green")

    def on_ai_error(self, agent_name, error, context):
        self.log_message(f"[AI:{agent_name}] ERROR: {error}", color="red")

    def on_trading_iteration(self):
        pv = self.get_portfolio_value()
        cash = self.get_cash()
        self.log_message(f"PV=${pv:,.2f}  Cash=${cash:,.2f}", color="blue")


if __name__ == "__main__":
    is_live = False

    if is_live:
        broker = Alpaca(ALPACA_CONFIG)
        strategy = AIMomentum(broker=broker)
        strategy.run_live()
    else:
        from lumibot.backtesting import YahooDataBacktesting

        backtesting_start = datetime(2023, 1, 1)
        backtesting_end = datetime(2023, 3, 1)
        AIMomentum.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            analyze_backtest=False,
        )
