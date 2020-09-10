import time

from BlueprintBot import BlueprintBot

class MomentumBot(BlueprintBot):
    def run(self):
        self.cancel_buying_orders()
        self.update_positions()
        self.await_market_to_open()

        time_to_close = self.get_time_to_close()
        while time_to_close > 15 * 60:
            self.buy_winning_stocks()
            self.update_positions()
            time_to_close = self.get_time_to_close()
            sleeptime = time_to_close - 15 * 60 - 60
            sleeptime = max(min(sleeptime, 60 * 10), 0)
            time.sleep(sleeptime)

        self.sell_all()

    def buy_winning_stocks(self):
        assets = self.get_tradable_assets()
        orders = []
        for asset in assets:
            change = self.get_percentage_change(asset.symbol, time_unity='day', length=1)
            if change >= 0.02:
                func_stop_price = lambda x: x * (1 - 0.04)
                # 40000$ for the entire strategy
                # 4000$ for each asset
                quantity = 1
                order = {
                    'qty': quantity,
                    'stock': asset.symbol,
                    'side': 'buy',
                    'func_stop_price': func_stop_price
                }
                orders.append(order)
        self.submit_orders(orders)

    def sell_all(self):
        orders = []
        for position in self.positions:
            order = {'qty': position.qty, 'stock': position.symbol, 'side': 'sell'}
            orders.append(order)

        self.submit_orders(orders)
