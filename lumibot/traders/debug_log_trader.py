from lumibot.traders.trader import Trader


class DebugLogTrader(Trader):
    """I'm just a trader instance with debug turned on by default"""

    def __init__(self, logfile="", backtest=False, debug=True, strategies=None, quiet_logs=False):
        super().__init__(logfile=logfile, backtest=backtest, debug=debug, strategies=strategies, quiet_logs=quiet_logs)

