"""
Bull/Bear Large-Cap Stock AI Trading Team
-----------------------------------------

This strategy demonstrates a plain-Python Lumibot AI trading team:

1. Evidence Researcher gathers market data, indicators, news, SEC fundamentals,
   SEC filings, and FRED macro data.
2. Bull Researcher builds the strongest long-only case.
3. Bear Researcher attacks the trade and identifies risks.
4. Portfolio Manager decides whether to place real Lumibot orders.

The same strategy code can run in backtests, paper trading, or live trading.
Research agents use allow_trading=False, so they can inspect data but cannot
submit, cancel, or modify orders. The portfolio manager keeps trading enabled.

Example model setup:

    COMMITTEE_RESEARCH_MODEL="openai/gpt-5.4-mini"
    COMMITTEE_BULL_MODEL="openai/gpt-5.5"
    COMMITTEE_BEAR_MODEL="openai/gpt-5.5"
    COMMITTEE_TRADER_MODEL="openai/gpt-5.5"

Requirements:
    - OPENAI_API_KEY when using OpenAI models
    - Active Alpaca broker credentials or ALPACA_NEWS_API_KEY / ALPACA_NEWS_API_SECRET for Alpaca news, optional
    - SEC fundamentals use public SEC EDGAR APIs and do not require an API key
    - FRED_API_KEY is required for FRED/ALFRED macro tools. Lumibot uses the official
      API with realtime vintage parameters rather than revised public CSV data.
"""

import os

from lumibot.components.agents import BuiltinTools
from lumibot.strategies.strategy import Strategy


DEFAULT_UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "SPY", "QQQ"]
DEFAULT_CASH_PARKING_SYMBOLS = ["SGOV", "BIL", "SHV"]
DEFAULT_HANDOFF_TARGET_TOKENS = 24000


def _prepare_handoff_text(value) -> str:
    return str(value or "")


class AITradingTeamBullBearLargeCapStocksStrategy(Strategy):
    parameters = {
        "universe": DEFAULT_UNIVERSE,
        "cash_parking_symbols": DEFAULT_CASH_PARKING_SYMBOLS,
        "max_position_pct": 0.20,
        "max_new_positions_per_run": 2,
        "handoff_target_tokens": DEFAULT_HANDOFF_TARGET_TOKENS,
        "enable_notifications": False,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.last_evidence_pack = None
        self.vars.last_bull_case = None
        self.vars.last_bear_case = None

        if self.parameters.get("enable_notifications"):
            self.notifications.enabled = True
            self.notifications.configure_telegram()

        research_model = os.environ.get("COMMITTEE_RESEARCH_MODEL", "openai/gpt-5.4-mini")
        bull_model = os.environ.get("COMMITTEE_BULL_MODEL", "openai/gpt-5.5")
        bear_model = os.environ.get("COMMITTEE_BEAR_MODEL", "openai/gpt-5.5")
        trader_model = os.environ.get("COMMITTEE_TRADER_MODEL", "openai/gpt-5.5")
        evidence_tools = self._evidence_tools()
        debate_tools = self._debate_tools()

        self.agents.create(
            name="evidence_researcher",
            model=research_model,
            allow_trading=False,
            system_prompt=self._evidence_researcher_prompt(),
            tools=evidence_tools,
            include_builtin_tools=False,
        )
        self.agents.create(
            name="bull_researcher",
            model=bull_model,
            allow_trading=False,
            system_prompt=self._bull_researcher_prompt(),
            tools=debate_tools,
            include_builtin_tools=False,
        )
        self.agents.create(
            name="bear_researcher",
            model=bear_model,
            allow_trading=False,
            system_prompt=self._bear_researcher_prompt(),
            tools=debate_tools,
            include_builtin_tools=False,
        )
        self.agents.create(
            name="portfolio_manager",
            model=trader_model,
            allow_trading=True,
            system_prompt=self._portfolio_manager_prompt(),
            tools=self._portfolio_manager_tools(),
            include_builtin_tools=False,
        )

    def _evidence_tools(self):
        return [
            BuiltinTools.market.last_price(),
            BuiltinTools.market.load_history_table(),
            BuiltinTools.duckdb.query(),
            BuiltinTools.indicators.get_indicators(),
            BuiltinTools.news.alpaca_news(),
            BuiltinTools.fundamentals.income_statement(),
            BuiltinTools.fundamentals.balance_sheet(),
            BuiltinTools.fundamentals.cash_flow(),
            BuiltinTools.fundamentals.company_facts(),
            BuiltinTools.fundamentals.filings(),
            BuiltinTools.macro.list_fred_series(),
            BuiltinTools.macro.get_fred_snapshot(),
        ]

    def _debate_tools(self):
        return []

    def _portfolio_manager_tools(self):
        return [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.orders.open_orders(),
            BuiltinTools.orders.submit(),
            BuiltinTools.market.last_price(),
            BuiltinTools.indicators.get_indicators(),
            BuiltinTools.memory.search(),
            BuiltinTools.memory.remember_decision(),
            BuiltinTools.notifications.notify_user(),
        ]

    def on_trading_iteration(self):
        universe = list(self.parameters.get("universe") or DEFAULT_UNIVERSE)
        cash_parking_symbols = list(self.parameters.get("cash_parking_symbols") or [])
        tradable_symbols = list(dict.fromkeys([*universe, *cash_parking_symbols]))
        context = {
            "universe": universe,
            "cash_parking_symbols": cash_parking_symbols,
            "tradable_symbols": tradable_symbols,
            "max_position_pct": self.parameters.get("max_position_pct", 0.20),
            "max_new_positions_per_run": self.parameters.get("max_new_positions_per_run", 2),
            "handoff_target_tokens": self.parameters.get("handoff_target_tokens", DEFAULT_HANDOFF_TARGET_TOKENS),
            "datetime": self.get_datetime().isoformat(),
        }

        evidence = self.agents["evidence_researcher"].run(
            task_prompt=(
                "Build the evidence pack for today's committee. Start with the full universe, "
                "then focus deeply on the best one or two long-only candidates. Use SEC fundamentals, SEC filings, "
                "news, market data, indicators, and FRED macro data only if FRED tools are available. "
                "Also review context.cash_parking_symbols as low-risk cash-parking alternatives, using price, "
                "volatility, and trend data rather than corporate SEC fundamentals for those ETFs. "
                "Keep the research pass bounded: use no more than about 20 tool calls, do not call every tool for "
                "every symbol, prefer compact tools, and finish once the committee has enough evidence to decide."
            ),
            context=context,
        )
        self.vars.last_evidence_pack = _prepare_handoff_text(evidence.summary or evidence.text)

        bull = self.agents["bull_researcher"].run(
            task_prompt="Build the strongest long-only investment case from the evidence pack.",
            context={**context, "evidence_pack": self.vars.last_evidence_pack},
        )
        self.vars.last_bull_case = _prepare_handoff_text(bull.summary or bull.text)

        bear = self.agents["bear_researcher"].run(
            task_prompt="Attack the long case and identify reasons to avoid, delay, reduce, or monitor the trade.",
            context={
                **context,
                "evidence_pack": self.vars.last_evidence_pack,
                "bull_case": self.vars.last_bull_case,
            },
        )
        self.vars.last_bear_case = _prepare_handoff_text(bear.summary or bear.text)

        decision = self.agents["portfolio_manager"].run(
            task_prompt=(
                "Make the final long-only portfolio decision. Review current positions, cash, open orders, "
                "the evidence pack, bull case, and bear case. Trade only symbols in context.tradable_symbols. "
                "If the account already holds an oversized, unsupported, or out-of-universe position, reduce or exit it "
                "before adding new exposure. "
                "If no growth-equity candidate clears the risk/reward bar, consider parking idle cash in a "
                "short-duration Treasury or cash ETF from context.cash_parking_symbols. Place orders only when "
                "justified by the evidence and within the risk limits."
            ),
            context={
                **context,
                "evidence_pack": self.vars.last_evidence_pack,
                "bull_case": self.vars.last_bull_case,
                "bear_case": self.vars.last_bear_case,
            },
        )
        self.log_message(f"[investment_committee] {decision.summary}", color="yellow")

    def _evidence_researcher_prompt(self) -> str:
        return """
You are the Evidence Researcher for a Lumibot AI trading team.

You cannot place, modify, or cancel trades. You are responsible for building a compact, source-backed evidence pack.
Your answer is a handoff to the other committee members. Keep the final answer under context.handoff_target_tokens tokens.
Do not pad the answer to fill the token budget; shorter is better when the important evidence is complete.
Do not paste raw tool payloads, long tables, SEC excerpts, or full time series. Synthesize the important facts and cite tool names/sources.

Use a two-stage process:
1. Broad screen: compare the universe with price/history, indicators, news if available, and one macro snapshot if FRED tools are available.
2. Deep dive: pick only the best one or two common-stock candidates and pull SEC fundamentals plus latest filings for those names.

Do not do SEC filing section reads in this pass. The Bull and Bear Researchers handle targeted filing-section work.

For reviewed candidates, gather:
1. Market context and current/visible historical prices.
2. Technical indicators using get_indicators. Include RSI, MACD, moving averages, volatility/ATR, and trend context when available.
3. Recent news using alpaca_news when credentials are available.
4. SEC fundamentals for the one or two best common-stock candidates using get_income_statement, get_balance_sheet, get_cash_flow, and get_company_facts.
5. Latest SEC filing list for those one or two common-stock candidates using get_filings.
6. Macro context using list_fred_series and one get_fred_snapshot call only when those tools are available for rates, inflation, labor, growth, liquidity, credit spreads, and market risk when relevant.
7. Cash-parking context for context.cash_parking_symbols. Treat these as short-duration Treasury/cash ETF alternatives for idle cash; use price, volatility, drawdown/trend, and macro/rate context. Do not run corporate SEC fundamental analysis on cash-parking ETFs.

Tool discipline:
- Use no more than about 20 tool calls total.
- Do not call every tool for every symbol. Screen broadly first, then investigate only the most promising candidates.
- Prefer compact tools and targeted queries over broad raw payloads.
- Do not call tools just because they are available. Gather enough evidence to support the handoff and stop.

Return JSON-like markdown with:
- candidate symbols reviewed
- top long candidates
- price/technical summary, with only the decisive numbers
- fundamental summary, with only the decisive numbers
- filing highlights, summarized in your own words
- macro regime summary, if macro tools are available
- news/catalyst summary, summarized in your own words
- bull evidence
- bear evidence
- missing data or uncertainty
- tools/sources used
"""

    def _bull_researcher_prompt(self) -> str:
        return """
You are the Bull Researcher. You cannot place, modify, or cancel trades.

Build the strongest long-only case from the evidence pack.
Focus on catalysts, fundamentals, technical setup, filing evidence, market regime, and why the reward is worth the risk.
Your answer is a handoff to the Bear Researcher and Portfolio Manager. Keep the final answer under context.handoff_target_tokens tokens.
Do not pad the answer to fill the token budget; shorter is better when the investment case is complete.
Do not repeat the full evidence pack. Extract only the strongest investable thesis and the supporting facts.
Use only the evidence pack and runtime context. Do not reopen the research process.

Return:
- strongest buy candidates
- thesis
- evidence
- catalysts
- risks you accept
- invalidation points
- suggested monitoring points
"""

    def _bear_researcher_prompt(self) -> str:
        return """
You are the Bear Researcher. You cannot place, modify, or cancel trades.

Attack the long case. Find reasons the portfolio manager should avoid, delay, reduce size, or demand more evidence.
Look for valuation risk, technical weakness, bad filing details, balance-sheet issues, deteriorating cash flow, crowding, liquidity risk, and missing data.
Your answer is a handoff to the Portfolio Manager. Keep the final answer under context.handoff_target_tokens tokens.
Do not pad the answer to fill the token budget; shorter is better when the risk case is complete.
Do not repeat the full evidence pack or bull case. Extract the highest-impact objections, what would change your mind, and the risk controls needed.
Use only the evidence pack, bull case, and runtime context. Do not reopen the research process.

Return:
- strongest objections
- downside catalysts
- evidence contradicting the bull case
- filing/news/fundamental red flags
- conditions that would make the trade acceptable
"""

    def _portfolio_manager_prompt(self) -> str:
        return """
You are the Portfolio Manager for a long-only Lumibot strategy.
Keep the final answer concise and decision-focused. Do not repeat the full evidence pack, bull case, or bear case.

You may place real Lumibot orders, but only within the strategy risk limits. Before trading:
1. Check current portfolio, positions, open orders, and prices.
2. Review the evidence pack, bull case, and bear case.
3. Respect max_position_pct and max_new_positions_per_run from context.
4. Trade only symbols in context.tradable_symbols. Growth-equity trades must come from context.universe. Defensive cash-parking trades must come from context.cash_parking_symbols.
5. Do not short. Do not use options in this v1 committee example.
6. If an existing holding violates max_position_pct, is outside context.tradable_symbols, or no longer has support in the evidence, reduce or sell it. Long-only does not mean buy-only.
7. If no growth-equity candidate clears the risk/reward bar, consider parking idle cash in a short-duration Treasury or cash ETF from context.cash_parking_symbols instead of leaving all capital idle.
8. Prefer doing nothing only when current holdings are acceptable and both the growth-equity case and the cash-parking case are weak, unavailable, or outside risk limits.
9. If you trade, use remember_decision and optionally notify_user.

Prefer portfolio/order/price checks over new research. Do not reopen the full research process.

Return:
- final decision
- orders submitted, if any
- sizing rationale
- risk controls applied
- why the bear case was accepted or rejected
- monitoring and invalidation points
"""


if __name__ == "__main__":
    from datetime import datetime

    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.entities import Asset, TradingFee

    trading_fee = TradingFee(percent_fee=0.001)
    AITradingTeamBullBearLargeCapStocksStrategy.backtest(
        YahooDataBacktesting,
        backtesting_start=datetime(2024, 1, 1),
        backtesting_end=datetime(2024, 2, 1),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        quote_asset=Asset("USD", Asset.AssetType.FOREX),
        budget=10000,
        quiet_logs=False,
    )


AIInvestmentCommitteeStrategy = AITradingTeamBullBearLargeCapStocksStrategy
