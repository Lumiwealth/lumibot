import logging
from datetime import datetime

import pandas as pd
import pandas_datareader.data as pdr

from lumibot.strategies.strategy import Strategy


class DebtTrading(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Setting the waiting period (in days) and the counter
        self.period = 1
        self.counter = 0

        self.debt_change_threshold = 0.15
        self.normal_ratio = 0.6
        self.buy_sp_ratio = 1

        # There is only one trading operation per day
        # No need to sleep between iterations
        self.sleeptime = 1
        self.debt_to_gdp_chng = self.get_debt_to_gdp_chng()

        self.portfolio = [
            {
                "symbol": "SPY",  # Equity
                "weight": 0.6,
                "last_price": None,
            },
            {
                "symbol": "TLT",  # Long Term Bond
                "weight": 0.4,
                "last_price": None,
            },
        ]

    def on_trading_iteration(self):
        # If the target number of days (period) has passed, rebalance the portfolio
        if self.counter == self.period or self.counter == 0:
            findme = self.get_round_day().replace(tzinfo=None)
            debt_change = self.debt_to_gdp_chng.loc[
                self.debt_to_gdp_chng.index == findme
            ]["Debt to GDP 300 Day Change"].values[0]
            logging.info(f"Debt level: {debt_change}")

            if debt_change > self.debt_change_threshold:
                logging.info(f"Using the buy_sp_ratio: {self.buy_sp_ratio}")
                self.portfolio[0]["weight"] = self.buy_sp_ratio
                self.portfolio[1]["weight"] = 1 - self.buy_sp_ratio
            else:
                logging.info(f"Using the normal_ratio: {self.normal_ratio}")
                self.portfolio[0]["weight"] = self.normal_ratio
                self.portfolio[1]["weight"] = 1 - self.normal_ratio

            self.counter = 0
            self.update_prices()
            self.rebalance_portfolio()
            logging.info(
                "Next portfolio rebalancing will be in %d day(s)" % self.period
            )

        logging.info("Sleeping until next trading day")
        self.counter += 1

        # Wait until the end of the day
        self.await_market_to_close()

    def on_abrupt_closing(self):
        # Sell all positions
        self.sell_all()

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

    # =============Helper methods====================

    def get_debt_to_gdp_chng(self):
        start = datetime(1900, 1, 1)
        end = datetime.now()

        # Create a Dataframe that will hold all of our cleaned data to analyze
        fed_debt = pdr.DataReader(["GFDEBTN"], "fred", start, end)
        gdp = pdr.DataReader(["GDPA"], "fred", start, end)
        resampled_fed_debt = fed_debt["GFDEBTN"].resample("D").fillna(method="ffill")
        resampled_gdp = gdp["GDPA"].resample("D").fillna(method="ffill")

        all_days = pd.DataFrame(index=pd.date_range(start, end))
        all_days = pd.concat(
            [all_days, resampled_fed_debt.to_frame(), resampled_gdp.to_frame()], axis=1
        ).fillna(method="ffill")
        all_days["Debt to GDP"] = all_days["GFDEBTN"] / all_days["GDPA"]
        all_days["Debt to GDP 300 Day Change"] = (
            all_days["Debt to GDP"] / all_days["Debt to GDP"].shift(300)
        ) - 1
        all_days = all_days.dropna()

        return all_days

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

            # Get how many shares we already own (including orders that haven't been executed yet)
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
