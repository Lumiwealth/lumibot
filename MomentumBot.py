import time, logging

from BlueprintBot import BlueprintBot

class MomentumBot(BlueprintBot):
    """A very simple risk management is impelmented in this bot:
    4000$ to spend per asset and 40000$ in total, so the bot
    should have 10 positions maximum and for each one spend 4000.
    Also needs to set data stream, cannot retrieve all the data from the API
    without risking of building very large urls or reaching the limit of 200 requests per minute
    set by alpaca
    """

    def run(self, increase_target=0.02, stop_loss_target=0.04, capital_per_asset=4000, total_capital=40000):
        #setting risk management variables
        self.total_capital = total_capital
        self.capital_per_asset = capital_per_asset
        self.max_positions = self.total_capital // self.capital_per_asset

        # sell all positions
        self.sell_all()
        #Cancel all the buying orders
        self.cancel_buying_orders()
        #Get the account updated positions
        self.update_positions()
        #Await till the market is open
        self.await_market_to_open()

        #Check if the market will still be open for more than 15 minutes
        time_to_close = self.get_time_to_close()
        while time_to_close > 15 * 60:
            #buy assets
            self.buy_winning_stocks(increase_target, stop_loss_target)
            self.update_positions()

            #Sleep for 10 minutes or till 15 minutes before the market closes
            time_to_close = self.get_time_to_close()
            sleeptime = time_to_close - 15 * 60
            sleeptime = max(min(sleeptime, 60 * 10), 0)
            time.sleep(sleeptime)

        #sell all positions
        self.sell_all()

    def buy_winning_stocks(self, increase_target, stop_loss_target):
        if len(self.positions) >= self.max_positions:
            return

        orders = []
        #Get all the tradable assets data without a current account position
        assets = self.get_tradable_assets()
        positions_symbols = [p.symbol for p in self.positions]
        symbols = [a.symbol for a in assets if a.symbol not in positions_symbols]

        #Truncate the first 100 assets, so that get requests
        #sent by the API does not exceed maximum
        symbols = symbols[:100]
        prices = self.get_last_prices(symbols)
        changes = self.get_percentage_changes(symbols, time_unity='day', length=1)
        for asset in assets:
            change = changes.get(asset.symbol)
            price = prices.get(asset.symbol)
            if change is not None and price is not None:
                if change >= increase_target:
                    stop_price_func = lambda x: x * (1 - stop_loss_target)
                    quantity = int(self.capital_per_asset/price)
                    order = {
                        'symbol': asset.symbol,
                        'quantity': quantity,
                        'side': 'buy',
                        'stop_price_func': stop_price_func
                    }
                    orders.append(order)

                    if len(orders) + len(self.positions) == self.max_positions:
                        logging.info('Maximum number of positions will be reached after orders submit')
                        break

        self.submit_orders(orders)
