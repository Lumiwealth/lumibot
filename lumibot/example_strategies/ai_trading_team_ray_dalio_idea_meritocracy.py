"""Ray Dalio / Bridgewater-inspired idea-meritocracy AI trading team example.

Regular ETF data-on variant. Uses LumiBot's default built-in tools, including
FRED/ALFRED macro tools when FRED_API_KEY is supplied, Alpaca News when
ALPACA_NEWS_API_KEY / ALPACA_NEWS_API_SECRET are supplied, SEC tools, market
state, account state, and order tools. No custom public CSV FRED helper is used.
"""

from lumibot.credentials import IS_BACKTESTING
from lumibot.entities import Asset, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


ORDER_READINESS_RULE = (
    "Immediately before every buy or sell order, call account_portfolio, "
    "account_positions, and market_last_price for the exact ordered symbol in "
    "this same agent run, then call orders_submit_order. LumiBot rejects blind "
    "orders with ORDER_READINESS_REQUIRED when those readiness calls are missing."
)

DATA_USAGE_RULE = (
    "Use official LumiBot built-in tools for evidence: FRED/ALFRED macro tools "
    "for rates, inflation, liquidity, growth, and credit; Alpaca News for "
    "recent market and ETF-proxy headlines; SEC tools only when sector or "
    "company fundamentals are relevant. Keep tool use bounded: at most one FRED "
    "snapshot or short series request and one Alpaca News call per agent run; set "
    "Alpaca News limit <= 5; do not paginate or repeatedly re-check the same evidence. "
    "Do not rely on a custom public CSV FRED helper."
)


class AITradingTeamRayDalioIdeaMeritocracyStrategy(Strategy):
    parameters = {
        "universe": [
            "SPY", "QQQ", "IWM", "TLT", "IEF", "TIP", "GLD", "DBC",
            "VNQ", "UUP", "FXI", "EEM", "SHV",
        ],
        "min_positions": 3,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="growth_agent",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt=(
                "Argue which ETFs win if growth improves. Inspect price/market tools, "
                "FRED growth/liquidity/rates context, and relevant Alpaca News before answering. "
                "Be direct and expose weak assumptions. " + DATA_USAGE_RULE
            ),
        )
        self.agents.create(
            name="inflation_agent",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt=(
                "Argue which ETFs win or lose if inflation and rates surprise. Inspect FRED CPI, "
                "inflation expectations, Treasury/rate data, and relevant Alpaca News before answering. "
                "Be direct. " + DATA_USAGE_RULE
            ),
        )
        self.agents.create(
            name="debt_liquidity_agent",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt=(
                "Argue from debt, liquidity, currency, and policy pressure. Inspect FRED liquidity, "
                "credit, dollar, and rate context plus relevant Alpaca News before answering. "
                "Be direct. " + DATA_USAGE_RULE
            ),
        )
        self.agents.create(
            name="thoughtful_disagreement",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt=(
                "Challenge all views with thoughtful disagreement. Identify the best diversified "
                "basket after stress testing. Check whether the upstream agents actually used "
                "FRED and Alpaca News evidence. Do not re-call tools unless upstream evidence is entirely absent; "
                "if you must, make only one short FRED call and one Alpaca News call with limit <= 5."
            ),
        )
        self.agents.create(
            name="trader",
            model="gemini-3.1-flash-lite",
            allow_trading=True,
            system_prompt=(
                "Build a Ray Dalio-style idea-meritocracy macro ETF basket, not a one-ETF bet. "
                "Hold at least three positions when risk is on; SHV or cash-like exposure may count "
                "as one position when evidence is weak. Reconcile and justify any override of the "
                "specialists or disagreement agent. Use variable weights and diversify across growth, "
                "duration, inflation/commodities, international/currency, and defensive sleeves when supported. "
                + DATA_USAGE_RULE + " " + ORDER_READINESS_RULE
            ),
        )

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
            "min_positions": self.parameters["min_positions"],
            "data_expectation": "Use official FRED tools and Alpaca News commonly; smoke tests will inspect agent_detail for actual tool calls.",
            "data_tool_validation_run_id": "2026-07-08-fresh-alpaca-news-fred-smoke-v1",
        }
        growth = self.agents["growth_agent"].run(
            task_prompt="Use FRED growth/liquidity/rate context and Alpaca News, then rank the strongest regular ETFs from a growth-regime view.",
            context=context,
        )
        inflation = self.agents["inflation_agent"].run(
            task_prompt="Use FRED inflation/rate context and Alpaca News, then rank the strongest regular ETFs from an inflation-and-rates view.",
            context=context,
        )
        liquidity = self.agents["debt_liquidity_agent"].run(
            task_prompt="Use FRED debt/liquidity/currency context and Alpaca News, then rank the strongest regular ETFs from a debt-and-liquidity view.",
            context=context,
        )
        disagreement = self.agents["thoughtful_disagreement"].run(
            task_prompt="Challenge the growth, inflation, and liquidity views. Prefer a diversified basket of at least three ETFs unless risk evidence argues for SHV/cash-like ballast.",
            context={**context, "growth": growth.summary, "inflation": inflation.summary, "liquidity": liquidity.summary},
        )
        self.agents["trader"].run(
            task_prompt=(
                "Rebalance into a diversified basket of at least three regular ETFs, using SHV/cash-like exposure only as ballast or a risk break. "
                "Before each order, call account_portfolio, account_positions, and market_last_price for the exact ordered symbol in this same run. "
                "Explain which specialist advice you accepted or rejected and cite FRED/Alpaca evidence used."
            ),
            context={**context, "growth": growth.summary, "inflation": inflation.summary, "liquidity": liquidity.summary, "disagreement": disagreement.summary},
        )


if __name__ == "__main__":
    quote_asset = Asset("USD", Asset.AssetType.FOREX)
    params = AITradingTeamRayDalioIdeaMeritocracyStrategy.parameters

    if IS_BACKTESTING:
        trading_fee = TradingFee(percent_fee=0.001)
        AITradingTeamRayDalioIdeaMeritocracyStrategy.backtest(
            datasource_class=None,
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=quote_asset,
            parameters=params,
        )
    else:
        trader = Trader()
        strategy = AITradingTeamRayDalioIdeaMeritocracyStrategy(
            quote_asset=quote_asset,
            parameters=params,
        )
        trader.add_strategy(strategy)
        trader.run_all()
