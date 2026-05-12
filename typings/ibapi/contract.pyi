from typing import Any

class ComboLeg:
    conId: int
    ratio: int
    action: str
    exchange: str
    openClose: int
    shortSaleSlot: int
    designatedLocation: str
    exemptCode: int
    def __init__(self) -> None: ...

class Contract:
    conId: int
    symbol: str
    secType: str
    lastTradeDateOrContractMonth: str
    strike: float | str
    right: str
    multiplier: str | int | float
    exchange: str
    primaryExchange: str
    currency: str
    localSymbol: str
    tradingClass: str
    includeExpired: bool
    secIdType: str
    secId: str
    comboLegsDescrip: str
    comboLegs: list[ComboLeg]
    deltaNeutralContract: Any
    def __init__(self) -> None: ...
