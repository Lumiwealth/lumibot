from collections import UserDict


class Asset:
    """
    This is a base class for Assets.
    Member attributes:
      - symbol (string): The symbol used to retrieve stock quotes if stock. The
          underlying symbol if option.
      - name (string): Long form name. Used only for printing. e.g. `Facebook Inc.`
      - asset_type (string, default: `stock`): One of the following:
        - `stock`
        - `option`
      If asset_type is `option` then the following fields are mandatory.
      - expiration (string, "YYYY-MM-DD"): Contract expiration date.
      - strike (float): Contract strike price.
      - right(string): `call` or `put`
      - multiplier (int): Contract leverage over the underlying.
    """

    def __init__(
        self,
        symbol,
        asset_type="stock",
        name="",
        expiration=None,
        strike=None,
        right=None,
        multiplier=1,
    ):
        self.asset_types = ["stock", "option"]

        self.symbol = symbol
        self.asset_type = asset_type
        self.name = name

        # Options
        self.expiration = expiration
        self.strike = strike
        self.right = right
        self.multiplier = int(multiplier)

    @property
    def asset_type(self):
        return self._asset_type

    @asset_type.setter
    def asset_type(self, value):
        if not value:
            value = "stock"
        if value not in self.asset_types:
            raise ValueError(f"Asset asset_type must be one of {self.asset_types}")
        self._asset_type = value

    # Option methods
    def is_option(self):
        return self._asset_type == "option"

    @property
    def strike_str(self):
        return str(self.strike)

    def __repr__(self):
        stock_repr = f"{self.symbol.upper()}, Type: {self.asset_type} "
        option_repr = (
            f"Exp: {self.expiration} " f"Strike: {self.strike} " f"Right: {self.right} "
        )

        if self.asset_type == "stock":
            return stock_repr
        else:
            return stock_repr + option_repr

    def same_as(self, other):
        # Check if an asset is the same as other, return `True` if so.
        if isinstance(other, Asset):
            return (
                self.symbol == other.symbol
                and self.asset_type == other.asset_type
                and self.expiration == other.expiration
                and self.strike == other.strike
                and self.right == other.right
                and self.multiplier == other.multiplier
            )

        return False

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
            self.data[Asset(key)] = value
        else:
            self.data[key] = value
