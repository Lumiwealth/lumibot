import datetime
from pandas import Timestamp

from credentials import InteractiveBrokersConfig, AlpacaConfig
from lumibot.brokers.interactive_brokers import InteractiveBrokersData
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.brokers.interactive_brokers import InteractiveBrokers



class TestDataBroker:
    """Comparing Alpaca and Interactive Brokers

    Tests making sure results are consistently the same between the two brokers.
    """
    ib_data = InteractiveBrokersData(InteractiveBrokersConfig)
    alpaca_data = AlpacaData(AlpacaConfig)
    ib_broker = InteractiveBrokers(InteractiveBrokersConfig)

    T1 = "SPY"
    T2 = "FB"

    if __name__ == "__main__":
        main = printon = True
    else:
        main = printon = False

    def print_or_test(self, name, ib, ap):
        if self.main:
            return print(f"{name} IB: {ib}, Alpaca: {ap}")
        else:
            assert ib == ap

    def test_get_datetime(self):
        name = "get_datetime"
        ib = self.ib_data.get_datetime().replace(microsecond=0)
        ap = self.alpaca_data.get_datetime().replace(microsecond=0)
        self.print_or_test(name, ib, ap)

    def test_get_timestamp(self):
        name = "get_timestamp"
        ib = round(self.ib_data.get_timestamp(), 2)
        ap = round(self.alpaca_data.get_timestamp(), 2)
        self.print_or_test(name, ib, ap)

    def test_market_hours(self):
        name = "market_hours"

        result = [
            (
                self.ib_broker.market_hours(
                    market=market,
                    close=close,
                    next=next,
                    date=datetime.date(2020, 10, 12),
                ),
                market,
                close,
                next,
            )
            for market in ["NASDAQ", "TSX"]
            for next in ["True", "False"]
            for close in ["True", "False"]
        ]

        expected = [
            Timestamp("2020-10-13 20:00:00+0000", tz="UTC"),
            Timestamp("2020-10-13 20:00:00+0000", tz="UTC"),
            Timestamp("2020-10-13 20:00:00+0000", tz="UTC"),
            Timestamp("2020-10-13 20:00:00+0000", tz="UTC"),
            Timestamp("2020-10-14 20:00:00+0000", tz="UTC"),
            Timestamp("2020-10-14 20:00:00+0000", tz="UTC"),
            Timestamp("2020-10-14 20:00:00+0000", tz="UTC"),
            Timestamp("2020-10-14 20:00:00+0000", tz="UTC"),
        ]
        compare = zip(result, expected)
        for i in compare:
            self.print_or_test(
                f"{name}, {i[0][1]},  {i[0][2]},  {i[0][3]}", i[0][0], i[1]
            )

    def test_get_round_minute(self):
        name = "get_round_minute"
        ib = self.ib_data.get_round_minute()
        ap = self.alpaca_data.get_round_minute()
        self.print_or_test(name, ib, ap)

    def test_get_last_minute(self):
        name = "get_last_minute"
        ib = self.ib_data.get_last_minute()
        ap = self.alpaca_data.get_last_minute()
        self.print_or_test(name, ib, ap)

    def test_get_round_day(self):
        name = "get_round_day"
        ib = self.ib_data.get_round_day()
        ap = self.alpaca_data.get_round_day()
        self.print_or_test(name, ib, ap)

    def test_get_last_day(self):
        name = "get_last_day"
        ib = self.ib_data.get_last_day()
        ap = self.alpaca_data.get_last_day()
        self.print_or_test(name, ib, ap)

    def test_get_datetime_range(self):
        name = "get_datetime_range"
        ib = self.ib_data.get_datetime_range(20, timestep="day")
        ap = self.alpaca_data.get_datetime_range(20, timestep="day")
        self.print_or_test(name, ib, ap)

    def test_get_symbol_bars(self):
        name = "get_symbol_bar"
        ib = self.ib_broker.get_symbol_bars(self.T1, length=5, timestep="day")
        ap = self.alpaca_data.get_symbol_bars(self.T1, length=5, timestep="day")

        ib_prices_sum = (
            ib.df[["open", "high", "low", "close"]].sum(axis=1).tail(1).values[0]
        )
        ap_prices_sum = (
            ap.df[["open", "high", "low", "close"]].sum(axis=1).tail(1).values[0]
        )

        if self.main:
            return print(f"{name} IB: {ib_prices_sum}, Alpaca: {ap_prices_sum}")
        else:
            assert abs(ib_prices_sum / ap_prices_sum - 1) < 0.01

    def test_get_last_price(self):
        name = "get_last_price"
        ib = self.ib_broker.get_last_price(self.T1)
        ap = self.alpaca_data.get_last_price(self.T1)
        if self.main:
            return print(f"{name} IB: {ib}, Alpaca: {ap}")
        else:
            assert abs(ib / ap - 1) < 0.01

    def test_get_last_prices(self):
        name = "get_last_prices"
        ib = self.ib_broker.get_last_prices(["FB", "MSFT"])
        ap = self.alpaca_data.get_last_prices(["FB", "MSFT"])
        if self.main:
            return print(f"{name} IB: {ib}, Alpaca: {ap}")
        else:
            assert (
                abs(
                    sum([cp for cp in ib.values()]) / sum([cp for cp in ap.values()])
                    - 1
                )
                < 0.01
            )

    def test_get_bars(self):
        name = "get_bars"
        ib = self.ib_broker.get_bars([self.T1, self.T2], length=5, timestep="day")
        ap = self.alpaca_data.get_bars([self.T1, self.T2], length=5, timestep="day")

        ib_prices_sum = sum(
            [
                bars.df[["open", "high", "low", "close"]].sum(axis=1).tail(1).values[0]
                for bars in ib.values()
            ]
        )

        ap_prices_sum = sum(
            [
                bars.df[["open", "high", "low", "close"]].sum(axis=1).tail(1).values[0]
                for bars in ap.values()
            ]
        )

        if self.main:
            print(f"{name} IB: {ib_prices_sum}, Alpaca: {ap_prices_sum}")
            return
        else:
            assert abs(ib_prices_sum / ap_prices_sum - 1) < 1



    def test_close(self):
        self.ib_broker._close_connection()

    def run_test(self):
        if self.main:
            self.test_get_datetime()
            self.test_get_timestamp()
            self.test_market_hours()
            self.test_get_round_minute()
            self.test_get_last_minute()
            self.test_get_round_day()
            self.test_get_last_day()
            self.test_get_datetime_range()
            self.test_get_symbol_bars()
            self.test_get_bars()
            self.test_get_last_price()
            self.test_get_last_prices()
            print("End")



if __name__ == "__main__":
    td = TestDataBroker()
    td.run_test()
    td.ib_broker._close_connection()

