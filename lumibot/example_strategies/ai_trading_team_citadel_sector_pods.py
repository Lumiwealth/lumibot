"""Citadel / Surveyor-inspired sector-pod AI trading team example.

Regular sector ETF data-on variant. Uses LumiBot's default built-in tools,
including FRED/ALFRED macro tools when FRED_API_KEY is supplied, Alpaca News
when ALPACA_NEWS_API_KEY / ALPACA_NEWS_API_SECRET are supplied, SEC tools,
market state, account state, and order tools. It preserves the sector-pod debate
structure but requires a diversified final allocation.
"""

import os

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
    "tools are available and optional when company or sector fundamentals matter. "
    "Keep tool use bounded: at most one FRED snapshot or short series request and "
    "one Alpaca News call per agent run; set Alpaca News limit <= 5; do not paginate "
    "or repeatedly re-check the same evidence. Do not rely on a custom public CSV FRED helper."
)

DIVERSIFICATION_RULE = (
    "Preserve the sector-pod process and convert it into a diversified portfolio. "
    "Hold at least three sector ETFs in normal conditions. Do not buy a single "
    "sector ETF with nearly all capital unless the only non-sector allocation is "
    "cash-like SHV due to explicit risk-off evidence."
)


class AITradingTeamCitadelSectorPodsStrategy(Strategy):
    parameters = {
        "universe": [
            "XLK", "XLF", "XLV", "XLE", "XLY", "XLI", "XLP", "XLU", "XLB", "XLRE", "XLC", "SHV",
        ],
        "min_positions": 3,
    }

    def initialize(self):
        self.sleeptime = "1D"
        model = os.environ.get("AI_TRADING_TEAM_MODEL", "openai/gpt-6-luna")
        self.agents.create(
            name="technology_pod",
            model=model,
            allow_trading=False,
            system_prompt=(
                "Rank technology and communications sector ETFs. First call alpaca_news for XLK/XLC/QQQ/SMH-relevant headlines and call get_fred_snapshot or get_fred_latest for rates, growth, and liquidity context. "
                "Use exact symbols from the universe only. " + DATA_USAGE_RULE
            ),
        )
        self.agents.create(
            name="financials_pod",
            model=model,
            allow_trading=False,
            system_prompt=(
                "Rank financial and rate-sensitive sector ETFs. First call get_fred_snapshot or get_fred_latest for yield curve, credit, liquidity, and policy-rate context, then call alpaca_news for financial-sector headlines. "
                "Use exact symbols from the universe only. " + DATA_USAGE_RULE
            ),
        )
        self.agents.create(
            name="healthcare_pod",
            model=model,
            allow_trading=False,
            system_prompt=(
                "Rank healthcare and defensive growth sector ETFs. Call alpaca_news for healthcare/biotech/defensive-growth headlines and use FRED tools for macro risk context. "
                "Use exact symbols from the universe only. " + DATA_USAGE_RULE
            ),
        )
        self.agents.create(
            name="energy_pod",
            model=model,
            allow_trading=False,
            system_prompt=(
                "Rank energy and commodity-sensitive sector ETFs. Call alpaca_news for oil/energy headlines and get_fred_snapshot/get_fred_latest for inflation, rates, dollar, and growth context. "
                "Use exact symbols from the universe only. " + DATA_USAGE_RULE
            ),
        )
        self.agents.create(
            name="consumer_pod",
            model=model,
            allow_trading=False,
            system_prompt=(
                "Rank consumer discretionary, staples, and housing-sensitive sector ETFs. Call alpaca_news for consumer/housing headlines and FRED tools for inflation, income, rates, and growth context. "
                "Use exact symbols from the universe only. " + DATA_USAGE_RULE
            ),
        )
        self.agents.create(
            name="risk_manager",
            model=model,
            allow_trading=False,
            system_prompt=(
                "Compare the pod picks. Challenge crowding, factor exposure, drawdown risk, reversal risk, and macro contradictions. "
                "Check whether pods actually used get_fred_snapshot/get_fred_latest and alpaca_news. Do not re-call tools unless pod evidence is entirely absent; if you must, make only one short FRED call and one Alpaca News call with limit <= 5. "
                + DATA_USAGE_RULE + " " + DIVERSIFICATION_RULE
            ),
        )
        self.agents.create(
            name="portfolio_manager",
            model=model,
            allow_trading=True,
            system_prompt=(
                "Allocate across the best sector ETFs from the pod process. Build a diversified portfolio of at least three positions in normal conditions, using variable weights based on pod conviction and risk-manager objections. "
                "Do not make a one-sector all-in trade. Reconcile any override of pod advice or risk-manager warnings. "
                + DATA_USAGE_RULE + " " + DIVERSIFICATION_RULE + " " + ORDER_READINESS_RULE
            ),
        )

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
            "min_positions": self.parameters["min_positions"],
            "data_expectation": "Use get_fred_snapshot/get_fred_latest and alpaca_news commonly; smoke tests inspect agent_detail for actual tool calls.",
            "portfolio_expectation": "Portfolio manager should normally hold at least three sector ETFs and avoid single-sector all-in behavior.",
            "data_tool_validation_run_id": "2026-07-08-fresh-alpaca-news-fred-smoke-v1",
        }
        technology = self.agents["technology_pod"].run(
            task_prompt="Call alpaca_news and FRED tools, then rank technology/communications sector opportunities.",
            context=context,
        )
        financials = self.agents["financials_pod"].run(
            task_prompt="Call get_fred_snapshot or get_fred_latest and alpaca_news, then rank financial/rate-sensitive sector opportunities.",
            context=context,
        )
        healthcare = self.agents["healthcare_pod"].run(
            task_prompt="Call alpaca_news and FRED tools, then rank healthcare/defensive-growth sector opportunities.",
            context=context,
        )
        energy = self.agents["energy_pod"].run(
            task_prompt="Call alpaca_news and FRED tools, then rank energy/commodity-sensitive sector opportunities.",
            context=context,
        )
        consumer = self.agents["consumer_pod"].run(
            task_prompt="Call alpaca_news and FRED tools, then rank consumer/housing-sensitive sector opportunities.",
            context=context,
        )
        risk = self.agents["risk_manager"].run(
            task_prompt="Compare all pod picks, challenge concentration/crowding/macro risks, and recommend a diversified 3+ sector allocation.",
            context={**context, "technology": technology.summary, "financials": financials.summary, "healthcare": healthcare.summary, "energy": energy.summary, "consumer": consumer.summary},
        )
        self.agents["portfolio_manager"].run(
            task_prompt=(
                "Rebalance into the best diversified 3+ sector ETF portfolio. Do not sell everything into one strongest ETF. "
                "Before each order, call account_portfolio, account_positions, and market_last_price for the exact ordered symbol in this same run. "
                "Explain which pod advice you accepted or rejected and cite FRED/Alpaca evidence used."
            ),
            context={**context, "technology": technology.summary, "financials": financials.summary, "healthcare": healthcare.summary, "energy": energy.summary, "consumer": consumer.summary, "risk": risk.summary},
        )


if __name__ == "__main__":
    quote_asset = Asset("USD", Asset.AssetType.FOREX)
    params = AITradingTeamCitadelSectorPodsStrategy.parameters

    if IS_BACKTESTING:
        trading_fee = TradingFee(percent_fee=0.001)
        AITradingTeamCitadelSectorPodsStrategy.backtest(
            datasource_class=None,
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=quote_asset,
            parameters=params,
        )
    else:
        trader = Trader()
        strategy = AITradingTeamCitadelSectorPodsStrategy(
            quote_asset=quote_asset,
            parameters=params,
        )
        trader.add_strategy(strategy)
        trader.run_all()
