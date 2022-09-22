from datetime import datetime
from decimal import Decimal
from typing import Union

from lumibot.entities import Order


class TradingFee:
    flat_fee: Decimal = 0
    percent_fee: Decimal = 0
    maker: bool = False
    taker: bool = False

    def __init__(self, flat_fee=0, percent_fee=0, maker=True, taker=True):
        """TradingFee class

        Parameters
        ----------
        flat_fee : Decimal, float, or None
            Flat fee to pay for each order
        percentage_fee : Decimal, float, or None
            Percentage fee to pay for each order
        maker : bool
            Whether this fee is a maker fee
        taker : bool
            Whether this fee is a taker fee
        """
        self.flat_fee = Decimal(flat_fee)
        self.percent_fee = Decimal(percent_fee)
        self.maker = maker
        self.taker = taker


class RealizedTradingFee:
    created_at: datetime
    order: Order
    amount: Decimal
    trading_fee: TradingFee

    class Config:
        arbitrary_types_allowed = True
