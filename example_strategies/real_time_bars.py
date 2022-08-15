import datetime
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from credentials import InteractiveBrokersConfig
from lumibot.brokers import InteractiveBrokers
from lumibot.traders import Trader


class RealTimeBars(Strategy):

    def initialize(
        self,
        asset=None,
        interval_time=1,
        timestep="minute",
        timeshift=datetime.timedelta(days=0),
    ):
        # Parameter initialization at program start.
        self.sleeptime = interval_time
        self.asset = asset
        self.timestep = timestep
        self.timeshift = timeshift
        self.set_market("24/7")

        self.start_realtime_bars(asset=self.asset, keep_bars=10)
        self.rtb_count = 0

    def on_trading_iteration(self):
        n = 2
        if self.rtb_count < n:
            rtb = self.get_realtime_bars(self.asset)
            print(f"less than {self.rtb_count} {n}, \n{rtb}")
        elif self.rtb_count == n:
            print(f"Canel bars. {self.rtb_count} {n}")
            self.cancel_realtime_bars(self.asset)
        elif self.rtb_count > n + 3:
            try:
                rtb = self.get_realtime_bars(self.asset)
                print(f"There should be no bars here!! {self.rtb_count} {n}, \n{rtb}")
            except:
                print(f"Real time bars successfully cancelled.")
        else:
            print(f"No real time bars: {self.rtb_count} {n}")

        self.rtb_count += 1



if __name__ == "__main__":

    logfile = "logs/rtb.log"

    # Initialize all our classes
    trader = Trader(logfile=logfile)  # debug="debug")
    broker = InteractiveBrokers(InteractiveBrokersConfig)

    asset = Asset(symbol="SPY")


    kwargs = dict(
        asset=asset,
        interval_time="5S",
        timestep="minute",
        timeshift=datetime.timedelta(days=0),
    )
    print(kwargs)
    strategy = RealTimeBars(broker=broker, **kwargs)

    trader.add_strategy(strategy)
    trader.run_all()
