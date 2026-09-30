"""VWAP Strategy AI Trading Bot.

Day trades SPY around VWAP, the average price big funds watch all day. When SPY
dips below VWAP and then climbs back above it, the bot buys the bounce. A
research agent checks the minute bars every hour. A trading agent sizes the
trade from a stop just under the dip and sells on the way back up.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class AIVWAPStrategy(Strategy):
    parameters = {"symbol": "SPY"}

    def initialize(self):
        self.sleeptime = "1H"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "You research a VWAP bounce on the symbol in the context. Use today's completed minute bars "
                "and get_indicator('vwap'). Report the last price, VWAP, and whether since the last hourly "
                "check the price dipped at least 0.15% below VWAP and then closed back above it. Report the "
                "dip's lowest price and any position we hold with its entry time. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You day trade the symbol in the context. First, sell a position bought at an earlier check. "
                "If we hold nothing, have not traded today, and the research shows a dip at least 0.15% "
                "below VWAP followed by a close back above it, buy. Put the stop just under the dip's low and "
                "size so hitting the stop loses at most 1% of the account, at most 200 shares. Sell "
                "everything before the close."
            ),
        )

    def on_trading_iteration(self):
        if self.get_datetime().hour < 10:  # VWAP needs some of today's bars first
            return
        facts = {"symbol": self.parameters["symbol"]}
        research = self.agents["researcher"].run(task_prompt="Check the VWAP setup.", context=facts)
        self.agents["trader"].run(
            task_prompt="Trade the VWAP bounce.",
            context={**facts, "research": research.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AIVWAPStrategy.backtest(AlpacaBacktesting, datetime(2026, 1, 5), datetime(2026, 1, 16))
    else:
        AIVWAPStrategy().run_live()
