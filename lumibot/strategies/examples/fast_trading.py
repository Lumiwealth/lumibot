import time

from lumibot.strategies.strategy import Strategy

"""
Strategy Description

Buys the best performing asset from self.symbols over self.momentum_length number of minutes.
For example, if TSLA increased 0.03% in the past two minutes, but SPY, GLD, TLT and MSFT only 
increased 0.01% in the past two minutes, then we will buy TSLA.
"""


class FastTrading(Strategy):
    IS_BACKTESTABLE = False

    # =====Overloading lifecycle methods=============
    def initialize(self, momentum_length=2, max_assets=3):
        # Setting the momentum period (in minutes)
        self.momentum_length = momentum_length

        # Set how often (in minutes) we should be running on_trading_iteration
        self.sleeptime = "1S"

        # Set the symbols that we want to be monitoring
        self.symbols = [
            "SPY",
            "GLD",
            "TLT",
            "MSFT",
            "TSLA",
            "MCHI",
            "SPXL",
            "SPXS",
            "TUEM",
        ]

        # Set up assets, orders, positions.
        self.assets = [self.create_asset(symbol) for symbol in self.symbols]
        for asset in self.assets:
            asset.quantity = 0
            asset.last_price = 0

        # Set up order dict. Will hold active orders.
        self.orders = list()

        # Positions. Will track positions, held, sold or bought.
        self.trade_positions = list()

        # Initialize our variables
        self.max_assets = min(max_assets, len(self.assets))

    def on_trading_iteration(self):

        # Setting the buying budget

        # ACTUAL
        # Cash
        cash = self.unspent_money
        # Positions
        positions = self.get_tracked_positions()
        # Value
        value = self.portfolio_value
        # orders
        orders_pending = self.get_tracked_orders()
        # orders_pending = [order for order in self.get_tracked_orders() if order.status != "fill"]

        # DELTA
        # Cash pending from outstanding orders.
        cash_pending = sum(
            [order.cash_pending(self) for order in self.get_tracked_orders()]
        )
        # Net shares to trade
        shares_to_trade = [
            (order.asset, order.get_increment()) for order in orders_pending
        ]
        if len(shares_to_trade) > 0 or cash_pending > 0:
            print(f"{50*'#'}")
        # PROJECTED

        # Positions
        # self.trade_positions

        self.log_message(
            f"Check Values \n\n"
            f"Actual:    Cash: {cash:7.2f}, Value: {value:8.2f}, Positions: {positions}\n"
            f"Pending:   Cash: {cash_pending:7.2f}, Value: 'N/A', Positions: {shares_to_trade}\n"
            f"Expected:  Cash: {cash+cash_pending:7.2f}, Value: 'N/A', Positions: "
            f"{self.trade_positions}"
        )

        cash = cash + cash_pending

        self.orders = list()

        # self.print_details("Start Iteration")

        # Get the momentums of all the assets we are tracking, attach to assets.
        self.get_assets_momentums()

        # Get the assets with the highest return in our momentum_length
        best_assets = sorted(self.assets, key=lambda x: x.momentum)[-self.max_assets :]

        # Selling assets
        for asset in self.trade_positions:
            if asset not in best_assets:
                self.log_message(f"Selling {asset.quantity} shares of {asset.symbol}")
                self.orders.append(self.create_order(asset, asset.quantity, "sell"))
                cash += asset.last_price * asset.quantity
                self.trade_positions.remove(asset)
                asset.quantity = 0
        selling_orders = self.orders_sell()
        self.submit_orders(selling_orders)

        # Buying assets
        for asset in best_assets:
            if asset in self.trade_positions:
                continue
            items_to_trade = self.max_assets - len(self.trade_positions)
            if items_to_trade <= 0:
                break
            trade_cash = cash / items_to_trade
            asset.quantity = trade_cash // asset.last_price
            self.log_message(f"Buying {asset.quantity} shares of {asset.symbol}.")
            self.orders.append(self.create_order(asset, asset.quantity, "buy"))
            cash -= asset.last_price * asset.quantity
            self.trade_positions.append(asset)
        self.submit_orders(self.orders_buy())

        self.log_message(
            f"At end of iteration: Cash: {cash:7.2f}, Value: {self.portfolio_value:7.2f}, "
            f"Orders: {self.orders}, Positions: {self.trade_positions}"
        )

    def trace_stats(self, context, snapshot_before):
        """
        Add additional stats to the CSV logfile
        """
        # Get the values of all our variables from the last iteration
        row = {
            "old_unspent_money": snapshot_before.get("unspent_money"),
            "old_portfolio_value": snapshot_before.get("portfolio_value"),
        }

        for asset in self.assets:
            row[f"{asset.symbol}_quantity"] = asset.quantity
            row[f"{asset.symbol}_momentum"] = asset.momentum
            row[f"{asset.symbol}_last_price"] = asset.last_price
            row[f"{asset.symbol}_mkt_value"] = asset.quantity * asset.last_price

        # Add all of our values to the row in the CSV file. These automatically get
        # added to portfolio_value, unspent_money and return
        return row

    def before_market_closes(self):
        # Make sure that we sell everything before the market closes
        self.sell_all()
        self.orders = list()
        self.trade_positions = list()

    def on_abrupt_closing(self):
        self.sell_all()

    # =============Helper methods====================

    def get_assets_momentums(self):
        """
        Gets the momentums (the percentage return) for all the assets we are tracking,
        over the time period set in self.momentum_length
        """
        for asset in self.assets:
            # Get the return for symbol over self.momentum_length minutes
            bars_set = self.get_symbol_bars(asset, self.momentum_length + 1)
            asset.last_price = bars_set.get_last_price()
            start_date = self.get_round_minute(timeshift=self.momentum_length + 1)
            asset.momentum = bars_set.get_momentum(start=start_date)
            self.log_message(
                "%s has a return value of %.2f%% over the last %d minutes(s)."
                % (asset.symbol, 100 * asset.momentum, self.momentum_length)
            )

        return None

    def orders_buy(self):
        """Returns list of buy orders."""
        return [order for order in self.orders if order.side == "buy"]

    def orders_sell(self):
        """Returns list of sell orders."""
        return [order for order in self.orders if order.side == "sell"]

    def print_details(self, txt):
        d = f"{'~' * 50}\n"
        print(
            f"{d}"
            f"{txt}. Cash: {self.unspent_money:7.2f}, Value: {self.portfolio_value:7.2f}\n"
            f"UNFILLED ORDERS: {self.orders} \n"
            f"TRACKED ORDERS: {self.get_tracked_orders()} \n"
            f"TRADE POSITIONS: {self.trade_positions} \n"
            f"POSITIONS: {self.positions} \n"
            f"TRACKED POSITIONS: {self.get_tracked_positions()} \n"
            f"{d}"
        )
