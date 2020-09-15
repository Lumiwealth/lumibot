import time, logging, math

from BlueprintBot import BlueprintBot

class MomentumBot(BlueprintBot):
    """A very simple risk management is impelmented in this bot:
    4000$ to spend per asset and 40000$ in total, so the bot
    should have 10 positions maximum and for each one spend 4000.
    Also needs to set data stream, cannot retrieve all the data from the API
    without risking of building very large urls or reaching the limit of 200 requests per minute
    set by alpaca
    """

    #=====Overloading lifecycle methods=============

    def initialize(self):
        #canceling open orders
        self.cancel_buying_orders()

        # setting risk management variables
        self.total_capital = 40000
        self.capital_per_asset = 4000
        self.max_positions = self.total_capital // self.capital_per_asset
        self.increase_target = 0.02
        self.stop_loss_target = 0.04

    def before_market_opens(self):
        # sell all positions
        self.sell_all()

    def on_market_open(self):
        ongoing_assets = self.get_ongoing_assets()
        if len(ongoing_assets) < self.max_positions:
            self.buy_winning_stocks(self.increase_target, self.stop_loss_target)

    def before_market_closes(self):
        # sell all positions
        self.sell_all()

    #=============Helper methods====================

    def buy_winning_stocks(self, increase_target, stop_loss_target):
        logging.info("Requesting asset bars from alpaca API")
        data = self.get_data()
        logging.info("Selection best positions")
        new_positions = self.select_assets(data, increase_target)
        logging.info("Placing orders for assets %s." % [p.get('symbol') for p in new_positions])
        self.place_orders(new_positions, stop_loss_target)

    def get_data(self):
        """extract the data"""
        assets = self.get_tradable_assets()
        ongoing_assets = self.get_ongoing_assets()
        symbols = [a.symbol for a in assets if a.symbol not in ongoing_assets]

        bars = self.get_bars(symbols, 'day', 1)
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
            if price and change and change>=increase_target:
                potential_positions.append(record)

        potential_positions.sort(key=lambda x: x.get('change'), reverse=True)
        n_empty_positions = self.max_positions - len(self.get_ongoing_assets())
        potential_positions = potential_positions[:n_empty_positions]
        return potential_positions

    def place_orders(self, new_positions, stop_loss_target):
        """Placing the orders"""
        orders = []
        for position in new_positions:
            stop_price_func = lambda x: x * (1 - stop_loss_target)
            quantity = int(self.capital_per_asset / position.get('price'))
            order = {
                'symbol': position.get('symbol'),
                'quantity': quantity,
                'side': 'buy',
                'stop_price_func': stop_price_func
            }
            orders.append(order)

        self.submit_orders(orders)
