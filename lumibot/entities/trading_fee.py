from datetime import datetime
from decimal import Decimal
from typing import Union

from pydantic import BaseModel

from lumibot.entities import Order


class TradingFee(BaseModel):
    flat_fee: Union[Decimal, float, None] = 0
    percentage_fee: Union[Decimal, float, None] = 0
    maker: bool = False
    taker: bool = False

    def __init__(self, flat_fee=0, percentage_fee=0, maker=False, taker=False):
        self.flat_fee = Decimal(flat_fee)
        self.percentage_fee = Decimal(percentage_fee)
        self.maker = maker
        self.taker = taker


class RealizedTradingFee(BaseModel):
    created_at: datetime
    order: Order
    amount: Decimal
    trading_fee: TradingFee
    
    class Config:
        arbitrary_types_allowed = True
