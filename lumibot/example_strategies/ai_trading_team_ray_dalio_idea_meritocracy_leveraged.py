"""Ray Dalio / Bridgewater-inspired idea-meritocracy AI trading team example.

Leveraged ETF data-on variant. Uses LumiBot's default built-in tools, including
FRED/ALFRED macro tools when FRED_API_KEY is supplied, Alpaca News when
ALPACA_NEWS_API_KEY / ALPACA_NEWS_API_SECRET are supplied, SEC tools, market
state, account state, and order tools. The trade universe is 2x/3x leveraged
ETFs plus SHV/cash as a rare escape hatch.
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
    "Use official LumiBot built-in tools by their real names: get_fred_snapshot, "
    "get_fred_latest, get_fred_series, and list_fred_series for macro evidence; "
    "alpaca_news for recent market, sector, rates, and ETF-proxy headlines; SEC "
    "tools only when sector or company fundamentals are relevant. Keep tool use bounded: "
    "at most one FRED snapshot or short series request and one Alpaca News call per agent run; "
    "set Alpaca News limit <= 5; do not paginate or repeatedly re-check the same evidence. "
    "Do not rely on a custom public CSV FRED helper."
)

LEVERAGE_RULE = (
    "This is a leveraged ETF strategy. Use only symbols from the leveraged "
    "universe plus SHV/cash-like exposure. Stay biased toward diversified 3x "
    "exposure when evidence supports risk-taking; use 2x as a risk-down choice; "
    "use inverse leveraged ETFs only with explicit downside or hedge evidence. "
    "Hold at least three positions in normal conditions, and do not make a single "
    "all-in bet. SHV/cash may count as one position when the model refuses every "
    "reasonable leveraged setup."
)


class AITradingTeamRayDalioIdeaMeritocracyStrategy(Strategy):
    parameters = {
        "universe": [
            "TQQQ", "QLD", "SQQQ", "QID", "UPRO", "SSO", "SPXU", "SDS",
            "UDOW", "DDM", "SDOW", "DXD", "TNA", "UWM", "TZA", "TWM",
            "FNGU", "FNGD", "TECL", "TECS", "SOXL", "SOXS", "FAS", "FAZ",
            "CURE", "RXD", "LABU", "LABD", "ERX", "ERY", "GUSH", "DRIP",
            "TMF", "UBT", "TBT", "TTT", "UGL", "GLL", "AGQ", "ZSL",
            "UCO", "SCO", "NUGT", "DUST", "YINN", "YANG", "EDC", "EDZ",
            "EURL", "EUO", "YCS", "DRN", "SRS", "SHV",
        ],
        "min_positions": 3,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="growth_agent",
            model="openai/gpt-6-luna",
            allow_trading=False,
            system_prompt=(
                "Argue which leveraged ETFs win if growth improves. First call get_fred_snapshot "
                "or get_fred_latest for growth, liquidity, rates, and credit context, then call "
                "alpaca_news for broad market and ETF-proxy headlines. Use exact symbols from the "
                "universe only. " + DATA_USAGE_RULE + " " + LEVERAGE_RULE
            ),
        )
        self.agents.create(
            name="inflation_agent",
            model="openai/gpt-6-luna",
            allow_trading=False,
            system_prompt=(
                "Argue which leveraged ETFs win or lose if inflation and rates surprise. First call "
                "get_fred_snapshot or get_fred_latest for CPI, inflation expectations, Treasury yields, "
                "and policy-rate context, then call alpaca_news for rates, commodities, and market headlines. "
                "Use exact symbols from the universe only. " + DATA_USAGE_RULE + " " + LEVERAGE_RULE
            ),
        )
        self.agents.create(
            name="debt_liquidity_agent",
            model="openai/gpt-6-luna",
            allow_trading=False,
            system_prompt=(
                "Argue from debt, liquidity, currency, credit, and policy pressure. First call FRED tools "
                "such as get_fred_snapshot/get_fred_latest, then call alpaca_news for market stress and ETF-proxy headlines. "
                "Use exact symbols from the universe only. " + DATA_USAGE_RULE + " " + LEVERAGE_RULE
            ),
        )
        self.agents.create(
            name="thoughtful_disagreement",
            model="openai/gpt-6-luna",
            allow_trading=False,
            system_prompt=(
                "Challenge all views with thoughtful disagreement. Identify the best diversified leveraged basket after stress testing. "
                "Check whether upstream agents actually used get_fred_snapshot/get_fred_latest and alpaca_news. Do not re-call tools unless upstream evidence is entirely absent; if you must, make only one short FRED call and one Alpaca News call with limit <= 5. "
                + DATA_USAGE_RULE + " " + LEVERAGE_RULE
            ),
        )
        self.agents.create(
            name="trader",
            model="openai/gpt-6-luna",
            allow_trading=True,
            system_prompt=(
                "Build a Ray Dalio-style idea-meritocracy leveraged ETF basket. This is not All Weather and not a one-ETF momentum bet. "
                "Use the specialists' disagreement process, reconcile overrides, and size at least three positions in normal conditions. "
                "Favor 3x ETFs for high-conviction sleeves, use 2x when conviction or drawdown risk is lower, and use inverse ETFs only as carefully justified hedge or downside exposure. "
                "SHV/cash-like exposure may count as one position only when evidence is too weak for full leveraged risk. "
                + DATA_USAGE_RULE + " " + LEVERAGE_RULE + " " + ORDER_READINESS_RULE
            ),
        )

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
            "min_positions": self.parameters["min_positions"],
            "data_expectation": "Use get_fred_snapshot/get_fred_latest and alpaca_news commonly; smoke tests inspect agent_detail for actual tool calls.",
            "leverage_expectation": "Use only leveraged ETFs plus SHV/cash escape, hold at least three positions, and bias toward diversified 3x exposure.",
            "data_tool_validation_run_id": "2026-07-08-fresh-alpaca-news-fred-smoke-v1",
        }
        growth = self.agents["growth_agent"].run(
            task_prompt=(
                "Call get_fred_snapshot or get_fred_latest and alpaca_news, then rank the strongest leveraged ETFs from a growth-regime view. "
                "Prefer diversified 3x exposure when evidence supports it."
            ),
            context=context,
        )
        inflation = self.agents["inflation_agent"].run(
            task_prompt=(
                "Call get_fred_snapshot or get_fred_latest and alpaca_news, then rank the strongest leveraged ETFs from an inflation-and-rates view. "
                "Explain any inverse or 2x risk-down preference."
            ),
            context=context,
        )
        liquidity = self.agents["debt_liquidity_agent"].run(
            task_prompt=(
                "Call get_fred_snapshot or get_fred_latest and alpaca_news, then rank the strongest leveraged ETFs from a debt-and-liquidity view. "
                "Explain whether liquidity argues for 3x risk, 2x risk-down, inverse exposure, or SHV ballast."
            ),
            context=context,
        )
        disagreement = self.agents["thoughtful_disagreement"].run(
            task_prompt="Challenge the growth, inflation, and liquidity views. Prefer a diversified leveraged basket of at least three symbols unless risk evidence argues for SHV/cash-like ballast.",
            context={**context, "growth": growth.summary, "inflation": inflation.summary, "liquidity": liquidity.summary},
        )
        self.agents["trader"].run(
            task_prompt=(
                "Rebalance into a diversified leveraged ETF basket of at least three positions. Bias toward 3x ETFs, use 2x only as a risk-down choice, and use inverse ETFs only with explicit hedge/downside evidence. "
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
