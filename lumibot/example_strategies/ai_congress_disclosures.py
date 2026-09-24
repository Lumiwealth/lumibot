"""Nancy Pelosi congressional-disclosure strategy.

Python creates the agents and runs them. It does not download filings or place orders.
The research agent fetches the public House reports. The trading agent places the orders.
"""

from lumibot.example_strategies.agent_cycle import add_agent, run_cycle, trader_prompt
from lumibot.strategies import Strategy

_FILING_URLS = (
    "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033725.pdf",
    "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034836.pdf",
    "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20035143.pdf",
)
_BOOK = (
    "Use filings whose report date is on or before the session date. "
    "Each filing line has a ticker in parentheses and an asset code in square brackets. "
    "Count [ST] and [AB]. Skip [OP]. The research lines already exclude [OP], so each "
    "research line is a counted ticker even when its code is missing. "
    "P is a buy. S, including S (partial), is a sell. "
    "A line with transaction E, or a description that says gift, spinoff, or donor-advised, is not a trade. "
    "Each amount is a dollar range. The midpoint is dollars, never a share count. "
    "A sell is not a buy. For each ticker, net_dollars = buy midpoint minus sell midpoint. "
    "Keep every counted sell. A ticker that has both a buy and a sell uses both. "
    "If net_dollars is zero or negative, the weight is zero and you do not buy it, even if another agent calls it a buy. "
    "Example: AAA has only a buy of $3,000,000, so it can be bought. BBB has only a $15,000,000 sale, so it is not bought. "
    "CCC has a $375,000 buy and a $3,000,000 sell, so the net is a sale and it is not bought. "
    "DDD is a units line whose bracket code is AB and whose only trade is a buy, so it can be bought. "
    "Weights are each positive net divided by the sum of positive nets, and those weights sum to 100% of account value. "
    "Do not calculate shares yourself. For each positive-net ticker, call risk_calculate_stock_quantity with "
    "maximum_notional equal to account value times that weight and available_cash equal to cash still unspent. "
    "Submit exactly the quantity that tool returns. After each fill, the next order uses the cash that is left. Never short."
)
_EXIT = (
    "Sell a name with the order tool when a later visible filing is a sale or its scaled "
    "weight fell. Otherwise keep the replica invested, with cash near 0% to 5%."
)


class AICongressDisclosuresStrategy(Strategy):
    parameters = {
        "member": "Nancy Pelosi",
        "filing_urls": list(_FILING_URLS),
    }

    def initialize(self):
        self.sleeptime = "1D"
        add_agent(
            self,
            "congress_researcher",
            (
                "Research public House periodic transaction reports for the named member. "
                "Use http_request to fetch each filing URL. Each transaction is already one line. "
                "Read the report date in the filing text. Ignore any filing whose report date is after as_of. "
                "A transaction date is not the public date. "
                "The ticker is the symbol in parentheses. The code in square brackets is the asset type. "
                "Count [ST] and [AB]. Skip [OP]. "
                "P is a buy. S, including S (partial), is a sell. "
                "Skip a line whose transaction is E, or whose description says gift, spinoff, or donor-advised. "
                "For each ticker write one line: TICKER [CODE] buy_dollars sell_dollars net_dollars. "
                "Add every counted buy into buy_dollars and every counted sell into sell_dollars. "
                "Do not drop a sell because the same ticker also has a buy. "
                "buy_dollars and sell_dollars are range midpoints in dollars. "
                "net_dollars = buy_dollars - sell_dollars. A sale is not a buy. Do not submit orders."
            ),
            allow_trading=False,
            allow_network=True,
        )
        add_agent(
            self,
            "bull",
            "Argue for copying the visible buys, using range midpoints. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            "Argue the risks: stale filings, wide ranges, and names that should be sold. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            "Weight only [ST] and [AB] lines whose net_dollars is positive. Name every zero or negative net as do not buy. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "trading_risk_manager",
            trader_prompt(book_rule=_BOOK, exit_rule=_EXIT),
            allow_trading=True,
        )

    def on_trading_iteration(self):
        as_of = self.get_datetime()
        context = {
            "as_of": as_of.isoformat(),
            "member": self.parameters["member"],
            "filing_urls": list(self.parameters["filing_urls"]),
            "clock_rule": (
                "Ignore any filing whose report date is after as_of. "
                "Transaction date is not the public date."
            ),
            "risk_policy": {
                "cash_target": "0% to 5%",
                "sizing": "scale visible filing range midpoints to account value",
                "never_short": True,
            },
        }
        run_cycle(
            self,
            context,
            researcher="congress_researcher",
            bull="bull",
            bear="bear",
            interpreter="interpreter",
            trader="trading_risk_manager",
            research_task="Fetch the filings and report only the rows already public on as_of.",
            bull_task="Make the bull case for copying the visible book.",
            bear_task="Make the bear case against copying the visible book.",
            interpret_task="Assign account weights from the filing range midpoints.",
            trade_task=(
                "Buy only tickers whose research line has positive net_dollars. "
                "Call risk_calculate_stock_quantity for each of those tickers and submit that quantity. "
                "Do not buy a ticker another agent likes if its net_dollars is zero or negative."
            ),
        )
