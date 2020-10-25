import logging

from data_sources import Yahoo

from .strategy import Strategy


class Screener(Strategy):

    # =====Overloading lifecycle methods=============

    def initialize(self):
        # canceling open orders
        self.broker.cancel_open_orders()

        # creating an asset blacklist
        self.blacklist = []

        # setting sleeptime af each iteration to 5 minutes
        self.sleeptime = 5

        # setting risk management variables
        self.capital_per_asset = 4000
        self.period_trading_daily_average = 10
        self.minimum_trading_daily_average = 500000
        self.max_positions = self.budget // self.capital_per_asset
        self.increase_target = 0.02
        self.limit_increase_target = 0.02
        self.stop_loss_target = 0.04

    def before_market_opens(self):
        # sell all positions
        self.broker.sell_all()

    def before_starting_trading(self):
        """Resetting the list of blacklisted assets"""
        self.blacklist = []

    def on_trading_iteration(self):
        ongoing_assets = self.broker.get_ongoing_assets()
        if len(ongoing_assets) < self.max_positions:
            self.buy_winning_stocks(
                self.increase_target, self.stop_loss_target, self.limit_increase_target
            )
        else:
            logging.info("Max positions %d reached" % self.max_positions)

    def before_market_closes(self):
        # sell all positions
        self.broker.sell_all()

    def on_abrupt_closing(self):
        # sell all positions
        self.broker.sell_all()

    # =============Helper methods====================

    def buy_winning_stocks(
        self, increase_target, stop_loss_target, limit_increase_target
    ):
        logging.info("Requesting asset bars from alpaca API")
        data = self.get_data()
        logging.info("Selecting best positions")
        new_positions = self.select_assets(data, increase_target)
        logging.info(
            "Placing orders for top assets %s."
            % [p.get("symbol") for p in new_positions]
        )
        self.place_orders(new_positions, stop_loss_target, limit_increase_target)

    def get_data(self):
        """extract the data"""
        ongoing_assets = self.broker.get_ongoing_assets()
        assets = self.broker.get_tradable_assets()
        symbols = [a for a in assets if a not in (ongoing_assets + self.blacklist)]
        length = 4 * 24 + 1
        symbols_df = self.pricing_data.get_assets_momentum(
            symbols, time_unit="15Min", length=length, momentum_length=length - 1
        )
        return symbols_df

    def select_assets(self, data, increase_target):
        """Select the assets for which orders are going to be placed"""

        # filtering and sorting assets on momentum
        potential_positions = []
        for symbol, df in data.items():
            momentum = df["momentum"][-1]
            if momentum >= increase_target:
                record = {"symbol": symbol, "momentum": momentum}
                potential_positions.append(record)
        potential_positions.sort(key=lambda x: x.get("momentum"), reverse=True)

        ongoing_assets = self.broker.get_ongoing_assets()
        positions_count = len(ongoing_assets)
        n_empty_positions = self.max_positions - positions_count
        logging.info(
            "Account has %d postion(s) %s. Looking for %d additional position(s). Max allowed %d."
            % (
                positions_count,
                str(ongoing_assets),
                n_empty_positions,
                self.max_positions,
            )
        )

        logging.info(
            "Selecting %d assets with increase over %d %% (Ranked by increase)"
            % (n_empty_positions, 100 * increase_target)
        )
        selected_assets = []
        for potential_position in potential_positions:
            symbol = potential_position.get("symbol")
            momentum = potential_position.get("momentum")
            logging.info(
                "Asset %s recorded %.2f%% increase over 24h" % (symbol, 100 * momentum)
            )
            atv = Yahoo.get_average_trading_volume(
                symbol, self.period_trading_daily_average
            )
            test = atv >= self.minimum_trading_daily_average
            if test:
                selected_assets.append(potential_position)
                logging.info("Asset %s added to order queue." % symbol)
                if len(selected_assets) == n_empty_positions:
                    break
            else:
                self.blacklist.append(symbol)
                logging.info(
                    "Asset %s blacklisted. Trading Daily Average %d inferior to %d."
                    % (symbol, int(atv), self.minimum_trading_daily_average)
                )

        return selected_assets

    def place_orders(self, new_positions, stop_loss_target, limit_increase_target):
        """Placing the orders"""
        orders = []
        symbols = [p.get("symbol") for p in new_positions]
        last_prices = self.broker.get_last_prices(symbols)
        logging.info("Last prices for selected assets: %s" % str(last_prices))
        for position in new_positions:
            symbol = position.get("symbol")
            price = last_prices.get(symbol)
            if price:
                stop_price = price * (1 - stop_loss_target)
                limit_price = price * (1 + limit_increase_target)
                quantity = int(self.capital_per_asset / price)
                order = self.create_order(
                    symbol,
                    quantity,
                    "buy",
                    limit_price=limit_price,
                    stop_price=stop_price,
                )
                orders.append(order)
            else:
                logging.error(
                    "Could not submit order for asset %s. Something went wrong when requesting last price"
                    % symbol
                )

        self.broker.submit_orders(orders)
