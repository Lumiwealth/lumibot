import logging

from lumibot.strategies.strategy import Strategy

"""
Strategy Description

Allocates the budget between self.portfolio and rebalances every self.period days.
For example, if there is a budget of $100,000 then we will buy $30,000 SPY, $40,000 TLT, etc.
We will then buy/sell assets every day depending on self.portfolio_value (the amount of money
we have in this strategy) so that we match the percentages laid out in self.portfolio.
"""


class Diversification(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Setting the waiting period (in days) and the counter
        self.period = 1
        self.counter = 0

        # There is only one trading operation per day
        # no need to sleep between iterations
        self.sleeptime = 0

        # Initializing the portfolio variable with the assets and proportions we want to own
        self.initialized = False
        self.portfolio = [
            {
                "symbol": "SPY",  # Long Term Bond
                "weight": 0.3,
                "last_price": None,
            },
            {
                "symbol": "TLT",  # Equity
                "weight": 0.4,
                "last_price": None,
            },
            {
                "symbol": "IEF",  # Intermediate Term Bond
                "weight": 0.15,
                "last_price": None,
            },
            {
                "symbol": "GLD",  # Gold
                "weight": 0.075,
                "last_price": None,
            },
            {
                "symbol": "DJP",  # Commidities
                "weight": 0.075,
                "last_price": None,
            },
        ]

    def on_trading_iteration(self):
        # If the target number of days (period) has passed, rebalance the portfolio
        if self.counter == self.period or self.counter == 0:
            self.counter = 0
            self.update_prices()
            self.rebalance_portfolio()
            logging.info(
                "Next portfolio rebalancing will be in %d day(s)" % self.period
            )

        logging.info("Sleeping untill next trading day")
        self.counter += 1

        # Stop for the day, since we are looking at daily momentums
        self.await_market_to_close()

    def trace_stats(self, context, snapshot_before):
        # Add the price, quantity and weight of each asset for the time period (row)
        row = {}
        for item in self.portfolio:
            # Symbol is a dictionary with price, quantity and weight of the asset
            symbol = item.get("symbol")
            for key in item:
                if key != "symbol":
                    row[f"{symbol}_{key}"] = item[key]

        return row

    def on_abrupt_closing(self):
        # Sell all positions
        self.sell_all()

    # =============Helper methods====================

    def update_prices(self):
        """Update portfolio assets price"""
        symbols = [a.get("symbol") for a in self.portfolio]
        prices = self.get_last_prices(symbols)
        for asset in self.portfolio:
            asset["last_price"] = prices.get(asset["symbol"])

    def rebalance_portfolio(self):
        """Rebalance the portfolio and create orders"""
        orders = []
        for asset in self.portfolio:
            # Get all of our variables from portfolio
            symbol = asset.get("symbol")
            weight = asset.get("weight")
            last_price = asset.get("last_price")

            # Get how many shares we already own
            # (including orders that haven't been executed yet)
            quantity = self.get_asset_potential_total(symbol)
            if quantity:
                logging.info(
                    "Asset %s shares value: %.2f$. %.2f$ per %d shares."
                    % (symbol, quantity * last_price, last_price, quantity)
                )

            # Calculate how many shares we need to buy or sell
            shares_value = self.portfolio_value * weight
            new_quantity = shares_value // last_price
            quantity_difference = new_quantity - quantity
            logging.info(
                "Weighted %s shares value with %.2f%% weight: %.2f$. %.2f$ per %d shares."
                % (symbol, weight * 100, shares_value, last_price, new_quantity)
            )

            # If quantity is positive then buy, if it's negative then sell
            side = ""
            if quantity_difference > 0:
                side = "buy"
            elif quantity_difference < 0:
                side = "sell"

            # Execute the order if necessary
            if side:
                order = self.create_order(symbol, abs(quantity_difference), side)
                orders.append(order)
                asset["quantity"] = new_quantity

        self.submit_orders(orders)
