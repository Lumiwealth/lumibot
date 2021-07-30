from collections import UserDict
from pydantic import BaseModel, validator
from typing import Optional


class Asset(BaseModel, frozen=True, extra='forbid'):
    """
    This is a base class for Assets.
    Member attributes:
      - symbol (string): The symbol used to retrieve stock quotes if stock. The
          underlying symbol if option.
      - asset_type (string, default: `stock`): One of the following:
        - `stock`
        - `option`
        - `future`
        - 'forex'
      If asset_type is `option` then the following fields are mandatory.
      - expiration (string, "YYYY-MM-DD"): Contract expiration date.
          For futures:
          - expiration (string, "YYYYMM"): (use for futures)
      - strike (float): Contract strike price.
      - right(string): `CALL` or `PUT`
      - multiplier (int): Contract leverage over the underlying.
      If the asset_type if 'forex' then use the following fields:
      - symbol (string): The base currency.
      - currency (string): Conversion currency.
      - asset_type(string): `forex`
      When ordering forex, use the full dollar value, minimums of 20,000.
    """
    symbol: str
    asset_type: str = "stock"
    expiration: Optional[str] = None
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

        if v not in cls._right:
            raise ValueError(f"`right` must be one of {', '.join(cls._right)}, upper case.")
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
