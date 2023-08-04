from decimal import Decimal


class TradingFee:
    """TradingFee class. Used to define the trading fees for a broker in a strategy/backtesting."""

    def __init__(self, flat_fee=0.0, percent_fee=0.0, maker=True, taker=True):
        """
        Parameters
        ----------
        flat_fee : Decimal, float, or None
            Flat fee to pay for each order. This is a fixed fee that is paid for each order in the quote currency.
        percent_fee : Decimal, float, or None
            Percentage fee to pay for each order. This is a percentage of the order value that is paid for each order in the quote currency.
        maker : bool
            Whether this fee is a maker fee (applies to limit orders).
            Default is True, which means that this fee will be used on limit orders.
        taker : bool
            Whether this fee is a taker fee (applies to market orders).
            Default is True, which means that this fee will be used on market orders.

        Example
        --------
        >>> from datetime import datetime
        >>> from lumibot.entities import TradingFee
        >>> from lumibot.strategies import Strategy
        >>> from lumibot.backtesting import YahooDataBacktesting
        >>> class MyStrategy(Strategy):
        >>>     pass
        >>>
        >>> trading_fee_1 = TradingFee(flat_fee=5.2) # $5.20 flat fee
        >>> trading_fee_2 = TradingFee(percent_fee=0.01) # 1% fee
        >>> backtesting_start = datetime(2022, 1, 1)
        >>> backtesting_end = datetime(2022, 6, 1)
        >>> result = MyStrategy.backtest(
        >>>     YahooDataBacktesting,
        >>>     backtesting_start,
        >>>     backtesting_end,
        >>>     buy_trading_fees=[trading_fee_1, trading_fee_2],
        >>> )
        """
        self.flat_fee = Decimal(flat_fee)
        self.percent_fee = Decimal(percent_fee)
        self.maker = maker
        self.taker = taker
