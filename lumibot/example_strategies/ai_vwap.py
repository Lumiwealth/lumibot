"""VWAP Strategy AI Trading Bot.

Day trades SPY around VWAP, the average price big funds watch all day. When SPY
dips below VWAP and then climbs back above it, the bot buys the bounce. A
research agent checks the price every hour. A trading agent sizes the trade from
a stop just under the dip and is out by the close.
"""

from lumibot.strategies import Strategy


class AIVWAPStrategy(Strategy):
    parameters = {"symbol": "SPY"}

    def initialize(self):
        self.sleeptime = "1H"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "Compare the symbol's price today with its VWAP. Say whether, since the last hourly check, "
                "the price dipped at least 0.15% below VWAP and then closed back above it, and how low the "
                "dip went. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Day trade the symbol. If that bounce happened, we hold nothing, and we have not traded "
                "today, buy. Put a stop just under the dip's low and size the trade so the stop loses at most "
                "1% of the account. Sell at the next hourly check and always before the close."
            ),
        )

    def on_trading_iteration(self):
        if self.get_datetime().hour < 10:  # VWAP needs some of today's prices first
            return
        facts = {"symbol": self.parameters["symbol"]}
        research = self.agents["researcher"].run(task_prompt="Check the VWAP setup.", context=facts)
        self.agents["trader"].run(task_prompt="Trade the VWAP bounce.", context={**facts, "research": research.summary})


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AIVWAPStrategy.backtest(AlpacaBacktesting)
    else:
        AIVWAPStrategy().run_live()
