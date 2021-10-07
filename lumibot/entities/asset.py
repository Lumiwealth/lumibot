from collections import UserDict
from datetime import date
from pydantic import BaseModel, validator
from typing import Optional


class Asset(BaseModel, frozen=True, extra="forbid"):
    """
    This is a base class for Assets including stocks, futures, options
    and forex.

    Parameters
    ----------
    symbol : str
        Symbol of the stock or underlying in case of futures/options.
    asset_type : str
        Asset types are only `stock`, 'option`, `future`, `forex`,
        default : `stock`
    expiration : datetime.date
        Option or futures expiration.
        The datetime.date will be converted to broker specific formats.
        IB Format: for options "YYYYMMDD", for futures "YYYYMM"
    strike : str
        Options strike as string.
    right : str
        `CALL` or `PUT`
        default : ""
    multiplier : int
        Price multiplier.
        default : 1
    currency=None,

    Attributes
    ----------
    symbol : string
        The symbol used to retrieve stock quotes if stock. The underlying
        symbol if option. For Forex: The base currency.
    asset_type (string, default: `stock`): One of the following:
        - `stock`
        - `option`
        - `future`
        - 'forex'
    expiration : datetime.date
        Contract expiration dates for futures and options.
    strike : float
        Contract strike price.
    right : str
        Option call or put.
    multiplier : int
        Contract leverage over the underlying.
    currency : string
        Conversion currency.
    _asset_types : list of str
        Acceptable asset types.
    _right : list of str
        Acceptable values for right.

    Methods
    -------
    asset_type_must_be_one_of(@"asset_type")
        validates asset types.
    right_must_be_one_of(@"right")
        validates rights types.
    """

    symbol: str
    asset_type: str = "stock"
    expiration: Optional[date] = None
    strike: Optional[str] = ""
    right: Optional[str] = None
    multiplier: int = 1
    currency: Optional[str] = "USD"
    _asset_types: list = ["stock", "option", "future", "forex"]
    _right: list = ["CALL", "PUT"]

    @validator("asset_type")
    def asset_type_must_be_one_of(cls, v):
        if v not in cls._asset_types:
            raise ValueError(
                f"`asset_type` must be one of {', '.join(cls._asset_types)}"
            )
        return v

    @validator("right")
    def right_must_be_one_of(cls, v):
        if v is None:
            return

        v = v.upper()
        if v not in cls._right:
            raise ValueError(
                f"`right` is {v} must be one of {', '.join(cls._right)}, upper case."
            )
        return v


class AssetsMapping(UserDict):
    def __init__(self, mapping):
        UserDict.__init__(self, mapping)
        symbols_mapping = {k.symbol: v for k, v in mapping.items()}
        self._symbols_mapping = symbols_mapping

    def __missing__(self, key):
        if isinstance(key, str):
            if key in self._symbols_mapping:
                return self._symbols_mapping[key]
        raise KeyError(key)

    def __contains__(self, key):
        if isinstance(key, str):
            return key in self._symbols_mapping
        return key in self.data

    def __setitem__(self, key, value):
        if isinstance(key, str):
            self.data[Asset(symbol=key)] = value
        else:
            self.data[key] = value
