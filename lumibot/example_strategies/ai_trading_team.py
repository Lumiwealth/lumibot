"""
AI Trading Team - Leveraged ETF Demo
------------------------------------

Copy, set GEMINI_API_KEY, and run. The model is intentionally hard-coded so
the example stays simple.

    export GEMINI_API_KEY="your-gemini-key"
    python ai_trading_team.py

This is an aggressive educational backtest example, not a return promise.
"""

from datetime import datetime

from lumibot.components.agents import agent_tool
from lumibot.strategies.strategy import Strategy


MODEL = "gemini-3.1-flash-lite"
UNIVERSE = ["TQQQ", "SQQQ", "SOXL", "SOXS", "UPRO", "SPXU", "TECL", "TECS"]
INVERSE_ETFS = {"SQQQ", "SOXS", "SPXU", "TECS"}


class AITradingTeamStrategy(Strategy):
    parameters = {
        "universe": UNIVERSE,
        "lookback_days": 63,
        "rebalance_every": 5,
        "target_cash_buffer": 0.02,
    }

    @agent_tool(
        name="rotate_portfolio",
        description=(
            "Sell existing positions and buy one chosen leveraged ETF with nearly all available capital. "
            "Use only symbols from the strategy universe."
        ),
        mutates_trading=True,
    )
    def rotate_portfolio(self, symbol: str, reason: str = "") -> dict:
        if symbol not in self.parameters["universe"]:
            return {"error": f"{symbol} is not in the allowed universe."}

        price = self.get_last_price(symbol)
        portfolio_value = float(self.get_portfolio_value())
        cash_buffer = float(self.parameters["target_cash_buffer"])
        target_value = portfolio_value * (1 - cash_buffer)
        quantity = int(target_value // float(price)) if price else 0

        if quantity <= 0:
            return {"error": f"Not enough cash to buy {symbol}.", "price": price}

        if self.get_positions():
            self.sell_all()
        order = self.create_order(symbol, quantity, "buy")
        self.submit_order(order)

        message = f"Rotated into {quantity} shares of {symbol} at about ${price:.2f}."
        if reason:
            message += f" Reason: {reason}"
        self.log_message(message)
        return {"symbol": symbol, "quantity": quantity, "price": price, "reason": reason}

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.day = 0

        self.agents.create(
            name="research_agent",
            model=MODEL,
            allow_trading=False,
            system_prompt=(
                "You are the Research Agent for an aggressive leveraged ETF trading team. "
                "Find the fastest upside setup from the provided momentum table. "
                "Use the table only; do not call tools. Be concise."
            ),
        )
        self.agents.create(
            name="bull_agent",
            model=MODEL,
            allow_trading=False,
            system_prompt=(
                "You are the Bull Agent. Make the strongest case for the best upside trade. "
                "Favor decisive risk-on trades when momentum is strong. "
                "Use the table and research only; do not call tools."
            ),
        )
        self.agents.create(
            name="bear_agent",
            model=MODEL,
            allow_trading=False,
            system_prompt=(
                "You are the Bear Agent. Attack the trade, but do not be timid. "
                "Only reject a trade when the evidence is clearly bad or contradictory. "
                "Use the table, research, and bull case only; do not call tools."
            ),
        )
        self.agents.create(
            name="portfolio_manager_agent",
            model=MODEL,
            allow_trading=True,
            system_prompt=(
                "You are the Portfolio Manager Agent. Your job is to make money fast in this backtest. "
                "Pick exactly one ETF from the allowed universe and call rotate_portfolio. "
                "Stay in cash only if every setup is clearly broken. Never short directly."
            ),
            tools=[self.rotate_portfolio],
        )

    def on_trading_iteration(self):
        self.vars.day += 1
        if self.vars.day != 1 and self.vars.day % int(self.parameters["rebalance_every"]) != 0:
            return

        table = self._momentum_table()
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
            "momentum_table": table,
            "rules": (
                "Use one position at a time. Prefer the ETF with the strongest upside score. "
                "Risk-on ETFs are preferred during rebounds. Inverse ETFs are allowed only when "
                "their 5-day and 21-day returns are both positive and risk-on ETFs are breaking down."
            ),
        }

        research = self.agents["research_agent"].run(
            task_prompt="Rank the leveraged ETF universe for the next holding period. Do not call tools.",
            context=context,
        )
        bull = self.agents["bull_agent"].run(
            task_prompt="Choose the strongest upside trade and explain why it can move fast. Do not call tools.",
            context={**context, "research": research.summary},
        )
        bear = self.agents["bear_agent"].run(
            task_prompt="Challenge the bull case. Say what would make this trade fail. Do not call tools.",
            context={**context, "research": research.summary, "bull_case": bull.summary},
        )
        decision = self.agents["portfolio_manager_agent"].run(
            task_prompt=(
                "Make the final decision now. Pick one symbol from context.universe and call "
                "rotate_portfolio with a short reason. If you choose cash, explain why no ETF qualifies."
            ),
            context={
                **context,
                "research": research.summary,
                "bull_case": bull.summary,
                "bear_case": bear.summary,
            },
        )
        self.log_message(f"[ai_trading_team] {decision.summary}")

    def _momentum_table(self) -> list[dict]:
        rows = []
        lookback = int(self.parameters["lookback_days"])
        for symbol in self.parameters["universe"]:
            bars = self.get_historical_prices(symbol, lookback + 2, "day")
            if bars is None:
                continue
            df = bars.pandas_df
            if df is None or len(df) < 22:
                continue
            closes = df["close"].astype(float)
            last = float(closes.iloc[-1])
            ret_5d = last / float(closes.iloc[-6]) - 1 if len(closes) >= 6 else 0
            ret_21d = last / float(closes.iloc[-22]) - 1
            ret_63d = last / float(closes.iloc[0]) - 1
            high_63d = float(closes.max())
            drawdown = last / high_63d - 1 if high_63d else 0
            score = (
                (ret_5d * 5.0)
                + (ret_21d * 3.0)
                + (ret_63d * 0.5)
                + (max(ret_5d, 0) * 2.0)
                + (max(ret_21d, 0) * 1.5)
                - (abs(drawdown) * 0.25)
            )
            if symbol in INVERSE_ETFS and not (ret_5d > 0 and ret_21d > 0):
                score -= 10.0
            rows.append(
                {
                    "symbol": symbol,
                    "inverse_etf": symbol in INVERSE_ETFS,
                    "price": round(last, 2),
                    "return_5d": round(ret_5d, 4),
                    "return_21d": round(ret_21d, 4),
                    "return_63d": round(ret_63d, 4),
                    "drawdown_from_63d_high": round(drawdown, 4),
                    "score": round(score, 4),
                }
            )
        return sorted(rows, key=lambda item: item["score"], reverse=True)


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.entities import Asset, TradingFee

    AITradingTeamStrategy.backtest(
        YahooDataBacktesting,
        backtesting_start=datetime(2026, 4, 7),
        backtesting_end=datetime(2026, 5, 22),
        benchmark_asset=Asset("QQQ", Asset.AssetType.STOCK),
        buy_trading_fees=[TradingFee(percent_fee=0.001)],
        sell_trading_fees=[TradingFee(percent_fee=0.001)],
        budget=10000,
        quiet_logs=False,
    )
