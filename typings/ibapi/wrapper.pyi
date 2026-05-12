from typing import Any

from ibapi.common import OrderId, SetOfFloat, SetOfString, TickerId
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.ticktype import TickType

class OrderState:
    status: str
    initMarginBefore: str
    maintMarginBefore: str
    equityWithLoanBefore: str
    initMarginChange: str
    maintMarginChange: str
    equityWithLoanChange: str
    initMarginAfter: str
    maintMarginAfter: str
    equityWithLoanAfter: str
    commission: float
    minCommission: float
    maxCommission: float
    commissionCurrency: str
    warningText: str
    completedTime: str
    completedStatus: str
    def __init__(self) -> None: ...

class EWrapper:
    def __init__(self) -> None: ...
    def tickSnapshotEnd(self, reqId: int) -> None: ...
    def tickOptionComputation(
        self,
        reqId: TickerId,
        tickType: TickType,
        tickAttrib: int,
        impliedVol: float,
        delta: float,
        optPrice: float,
        pvDividend: float,
        gamma: float,
        vega: float,
        theta: float,
        undPrice: float,
    ) -> None: ...
    def realtimeBar(
        self,
        reqId: TickerId,
        time: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        wap: float,
        count: int,
    ) -> None: ...
    def accountSummaryEnd(self, reqId: int) -> None: ...
    def nextValidId(self, orderId: int) -> None: ...
    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState) -> None: ...
    def openOrderEnd(self) -> None: ...
    def orderStatus(self, *args: Any, **kwargs: Any) -> None: ...
    def execDetails(self, *args: Any, **kwargs: Any) -> Any: ...
    def contractDetails(self, *args: Any, **kwargs: Any) -> None: ...
    def contractDetailsEnd(self, reqId: int) -> None: ...
    def securityDefinitionOptionParameter(
        self,
        reqId: int,
        exchange: str,
        underlyingConId: int,
        tradingClass: str,
        multiplier: str,
        expirations: SetOfString,
        strikes: SetOfFloat,
    ) -> None: ...
    def securityDefinitionOptionParameterEnd(self, reqId: int) -> None: ...
