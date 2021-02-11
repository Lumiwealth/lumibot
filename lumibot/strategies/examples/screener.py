import logging

from lumibot.strategies.strategy import Strategy


class Screener(Strategy):

    # =====Overloading lifecycle methods=============

    def initialize(self):
        # creating an asset blacklist
        self.blacklist = []

        # setting sleeptime af each iteration to 5 minutes
        self.sleeptime = 5

        # setting risk management variables
        self.capital_per_asset = 4000
        self.minimum_trading_volume = 500000
        self.max_positions = self.unspent_money // self.capital_per_asset
        self.min_increase_target = 0.02
        self.limit_price_increase = 0.02
        self.stop_loss_target = 0.04

    def before_starting_trading(self):
        """Resetting the list of blacklisted assets"""
        self.blacklist = []
        self.sell_all()

    def on_trading_iteration(self):
        ongoing_assets = self.get_tracked_assets()
        if len(ongoing_assets) < self.max_positions:
            self.buy_winning_stocks(
                self.min_increase_target,
                self.stop_loss_target,
                self.limit_price_increase,
            )
        else:
            logging.info("Max positions %d reached" % self.max_positions)

    def before_market_closes(self):
        # sell all positions
        self.sell_all()

    def on_abrupt_closing(self):
        # sell all positions
        self.sell_all()

    # =============Helper methods====================

    def buy_winning_stocks(
        self, min_increase_target, stop_loss_target, limit_price_increase
    ):
        logging.info("Requesting asset bars from alpaca API")
        data = self.get_data()
        logging.info("Selecting best positions")
        new_positions = self.select_assets(data, min_increase_target)
        logging.info(
            "Placing orders for top assets %s."
            % [p.get("symbol") for p in new_positions]
        )
        self.place_orders(new_positions, stop_loss_target, limit_price_increase)

    def get_data(self):
        """extract the data"""
        ongoing_assets = self.get_tracked_assets()
        assets = self.get_tradable_assets()
        symbols = [a for a in assets if a not in (ongoing_assets + self.blacklist)]
        length = 4 * 24 + 1
        bars_list = self.get_bars(symbols, length)
        return bars_list

    def select_assets(self, data, min_increase_target):
        """Select the assets for which orders are going to be placed"""

        # filtering and sorting assets on momentum
        potential_positions = []
        for symbol, bars in data.items():
            momentum = bars.get_momentum()
            if momentum >= min_increase_target:
                record = {"symbol": symbol, "momentum": momentum}
                potential_positions.append(record)
        potential_positions.sort(key=lambda x: x.get("momentum"), reverse=True)

        ongoing_assets = self.get_tracked_assets()
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
            % (n_empty_positions, 100 * min_increase_target)
        )
        selected_assets = []
        for potential_position in potential_positions:
            symbol = potential_position.get("symbol")
            momentum = potential_position.get("momentum")
            logging.info(
                "Asset %s recorded %.2f%% increase over 24h" % (symbol, 100 * momentum)
            )

            # Get last 24h total trading volume
            trading_volume = data[symbol].get_total_volume(period=24 * 4)
            has_minimum_trading_volume = trading_volume >= self.minimum_trading_volume
            if has_minimum_trading_volume:
                selected_assets.append(potential_position)
                logging.info("Asset %s added to order queue." % symbol)
                if len(selected_assets) == n_empty_positions:
                    break
            else:
                self.blacklist.append(symbol)
                logging.info(
                    "Asset %s blacklisted. Trading Daily Average %d inferior to %d."
                    % (symbol, int(atv), self.minimum_trading_volume)
                )

        return selected_assets

    def place_orders(self, new_positions, stop_loss_target, limit_price_increase):
        """Placing the orders"""
        orders = []
        symbols = [p.get("symbol") for p in new_positions]
        last_prices = self.get_last_prices(symbols)
        logging.info("Last prices for selected assets: %s" % str(last_prices))
        for position in new_positions:
            symbol = position.get("symbol")
            price = last_prices.get(symbol)
            if price:
                stop_price = price * (1 - stop_loss_target)
                limit_price = price * (1 + limit_price_increase)
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

        self.submit_orders(orders)
