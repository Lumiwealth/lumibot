from .strategy import Strategy

class QuickMomentum(Strategy):

    #=====Overloading lifecycle methods=============

    def initialize(self):
        # canceling open orders
        self.api.cancel_open_orders()

        # setting sleeptime af each iteration to 5 minutes
        self.sleeptime = 5

        # setting risk management variables
        self.capital_per_asset = 4000
        self.max_positions = self.budget // self.capital_per_asset
        self.increase_target = 0.02
        self.limit_increase_target = 0.02
        self.stop_loss_target = 0.04

    def before_market_opens(self):
        # sell all positions
        self.api.sell_all()

    def on_market_open(self):
        ongoing_assets = self.api.get_ongoing_assets()
        if len(ongoing_assets) < self.max_positions:
            self.buy_winning_stocks(self.increase_target, self.stop_loss_target, self.limit_increase_target)
        else:
            logging.info("Max positions %d reached" % self.max_positions)

    def before_market_closes(self):
        # sell all positions
        self.api.sell_all()

    #=============Helper methods====================

    def buy_winning_stocks(self, increase_target, stop_loss_target, limit_increase_target):
        logging.info("Requesting asset bars from alpaca API")
        data = self.get_data()
        logging.info("Selecting best positions")
        new_positions = self.select_assets(data, increase_target)
        logging.info("Placing orders for top assets %s." % [p.get('symbol') for p in new_positions])
        self.place_orders(new_positions, stop_loss_target, limit_increase_target)

    def get_data(self):
        """extract the data"""
        assets = self.api.get_tradable_assets()
        ongoing_assets = self.api.get_ongoing_assets()
        symbols = [a.symbol for a in assets if a.symbol not in ongoing_assets]

        bars = self.api.get_bars(symbols, 'day', 1)
        data = []
        for symbol in symbols:
            bar = bars.get(symbol)
            if bar:
                first_value = bar[0].o
                last_value = bar[-1].c
                change = (last_value - first_value) / first_value
                record = {
                    'symbol': symbol,
                    'price': last_value,
                    'change': change
                }
                data.append(record)
        return data

    def select_assets(self, data, increase_target):
        """Select the assets for which orders are going to be placed"""
        potential_positions = []
        for record in data:
            price = record.get('price')
            change = record.get('change')
            if price and change and change >= increase_target:
                potential_positions.append(record)

        potential_positions.sort(key=lambda x: x.get('change'), reverse=True)
        logging.info("The top 15 assets with increase over %d %% (Ranked by increase)" % (100 * increase_target))
        for potential_position in potential_positions[:15]:
            symbol = potential_position.get('symbol')
            change = potential_position.get('change')
            logging.info("Asset %s recorded %.2f%% increase over 24h" % (symbol, 100 * change))

        ongoing_assets = self.api.get_ongoing_assets()
        positions_count = len(ongoing_assets)
        n_empty_positions = self.max_positions - positions_count
        potential_positions = potential_positions[:n_empty_positions]

        logging.info(
            "Account has %d postion(s) %s. Looking for %d additional position(s). Max allowed %d." %
            (positions_count, str(ongoing_assets), n_empty_positions, self.max_positions)
        )

        return potential_positions

    def place_orders(self, new_positions, stop_loss_target, limit_increase_target):
        """Placing the orders"""
        orders = []
        for position in new_positions:
            price = position.get('price')
            stop_price = price * (1 - stop_loss_target)
            limit_price = price * (1 + limit_increase_target)
            quantity = int(self.capital_per_asset / price)
            order = {
                'symbol': position.get('symbol'),
                'quantity': quantity,
                'side': 'buy',
                'price': price,
                'stop_price': stop_price,
                'limit_price': limit_price
            }
            orders.append(order)

        self.api.submit_orders(orders)
