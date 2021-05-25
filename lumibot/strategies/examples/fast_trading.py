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
        self.sleeptime = 1

        # Set the symbols that we want to be monitoring
        self.symbols = ["SPY", "GLD", "TLT", "MSFT", "TSLA", "MCHI", "SPXL", "SPXS"]

        # Initialize our variables
        self.assets_quantity = {symbol: 0 for symbol in self.symbols}
        self.max_assets = min(max_assets, len(self.symbols))
        self.quantity = 0

    def on_trading_iteration(self):
        # Setting the buying budget
        buying_budget = self.unspent_money

        # Get the momentums of all the assets we are tracking
        momentums = self.get_assets_momentums()
        for item in momentums:
            symbol = item.get("symbol")
            if self.assets_quantity[symbol] > 0:
                item["held"] = True
            else:
                item["held"] = False

        # Get the assets with the highest return in our momentum_length
        # (aka the highest momentum)
        # In case of parity, giving priority to current assets
        momentums.sort(key=lambda x: (x.get("return"), x.get("held")))
        prices = {item.get("symbol"): item.get("price") for item in momentums}
        best_assets = momentums[-self.max_assets :]
        best_assets_symbols = [item.get("symbol") for item in best_assets]

        # Deciding which assets to keep, sell and buy
        assets_to_keep = []
        assets_to_sell = []
        assets_to_buy = []
        for symbol, quantity in self.assets_quantity.items():
            if quantity > 0 and symbol in best_assets_symbols:
                # The asset is still a top asset and should be kept
                assets_to_keep.append(symbol)
            elif quantity <= 0 and symbol in best_assets_symbols:
                # Need to buy this new asset
                assets_to_buy.append(symbol)
            elif quantity > 0 and symbol not in best_assets_symbols:
                # The asset is no longer a top asset and should be sold
                assets_to_sell.append(symbol)

        # Printing decisions
        self.log_message("Keeping %r" % assets_to_keep)
        self.log_message("Selling %r" % assets_to_sell)
        self.log_message("Buying %r" % assets_to_buy)

        # Selling assets
        selling_orders = []
        for symbol in assets_to_sell:
            self.log_message("Selling %s." % symbol)
            quantity = self.assets_quantity[symbol]
            order = self.create_order(symbol, quantity, "sell")
            selling_orders.append(order)
        self.submit_orders(selling_orders)
        self.wait_for_orders_execution(selling_orders)

        # Checking if all orders went successfully through
        assets_sold = 0
        for order in selling_orders:
            if order.status == "fill":
                self.assets_quantity[order.symbol] = 0
                assets_sold += 1
                buying_budget += order.quantity * prices.get(order.symbol)

        # Buying new assets
        if self.first_iteration:
            number_of_assets_to_buy = self.max_assets
        else:
            number_of_assets_to_buy = assets_sold

        for i in range(number_of_assets_to_buy):
            symbol = assets_to_buy[i]
            price = prices.get(symbol)
            quantity = (buying_budget / number_of_assets_to_buy) // price
            order = self.create_order(symbol, quantity, "buy")
            self.log_message("Buying %d shares of %s." % (quantity, symbol))
            self.submit_order(order)
            self.assets_quantity[symbol] = quantity

    def trace_stats(self, context, snapshot_before):
        """
        Add additional stats to the CSV logfile
        """
        # Get the values of all our variables from the last iteration
        row = {
            "old_unspent_money": snapshot_before.get("unspent_money"),
            "old_portfolio_value": snapshot_before.get("portfolio_value"),
        }

        # Get the momentums of all the assets from the context of on_trading_iteration
        # (notice that on_trading_iteration has a variable called momentums, this is what
        # we are reading here)
        momentums = context.get("momentums")
        for item in momentums:
            symbol = item.get("symbol")
            for key in item:
                if key != "symbol":
                    row[f"{symbol}_{key}"] = item[key]

            row[f"{symbol}_quantity"] = self.assets_quantity[symbol]

        # Add all of our values to the row in the CSV file. These automatically get
        # added to portfolio_value, unspent_money and return
        return row

    def before_market_closes(self):
        # Make sure that we sell everything before the market closes
        self.sell_all()
        self.quantity = 0
        self.assets_quantity = {symbol: 0 for symbol in self.symbols}

    def on_abrupt_closing(self):
        self.sell_all()
        self.quantity = 0
        self.assets_quantity = {symbol: 0 for symbol in self.symbols}

    # =============Helper methods====================

    def get_assets_momentums(self):
        """
        Gets the momentums (the percentage return) for all the assets we are tracking,
        over the time period set in self.momentum_length
        """
        momentums = []
        for symbol in self.symbols:
            # Get the return for symbol over self.momentum_length minutes
            bars_set = self.get_symbol_bars(symbol, self.momentum_length + 1)
            start_date = self.get_round_minute(timeshift=self.momentum_length + 1)
            symbol_momentum = bars_set.get_momentum(start=start_date)
            self.log_message(
                "%s has a return value of %.2f%% over the last %d minutes(s)."
                % (symbol, 100 * symbol_momentum, self.momentum_length)
            )

            momentums.append(
                {
                    "symbol": symbol,
                    "price": bars_set.get_last_price(),
                    "return": symbol_momentum,
                }
            )

        return momentums
