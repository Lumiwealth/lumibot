from decimal import Decimal
from typing import Any

class Order:
    account: str
    action: str
    auxPrice: float | str | Decimal | None
    cashQty: float
    clientId: int
    contract: Any
    eTradeOnly: bool
    firmQuoteOnly: bool
    goodTillDate: str
    lmtPrice: float | Decimal | None
    ocaGroup: str
    ocaType: int
    orderId: int
    orderState: Any
    orderType: str
    parentId: int
    permId: int
    tif: str
    totalQuantity: int | float | Decimal
    trailingPercent: float | str
    transmit: bool
    def __init__(self) -> None: ...
