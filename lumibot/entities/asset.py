from collections import UserDict
from datetime import date, datetime
from enum import Enum

from lumibot.tools import parse_symbol


# Custom string enum implementation for Python 3.9 compatibility
class StrEnum(str, Enum):
    """
    A string enum implementation that works with Python 3.9+
    
    This class extends str and Enum to create string enums that:
    1. Can be used like strings (string methods, comparison)
    2. Are hashable (for use in dictionaries, sets, etc.)
    3. Can be used in string comparisons without explicit conversion
    """
    def __str__(self):
        return self.value
        
    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        return super().__eq__(other)
    
    def __hash__(self):
        # Use the hash of the enum member, not the string value
        # This ensures proper hashability while maintaining enum identity
        return super().__hash__()


class Asset:
    """
    This is a base class for Assets including stocks, futures, options,
    forex, and crypto.

    Parameters
    ----------
    symbol : str
        Symbol of the stock or underlying in case of futures/options.
    asset_type : str
        Type of the asset. Asset types are only 'stock', 'option', 'future', 'forex', 'crypto'
        default : 'stock'
    expiration : datetime.date
        Option or futures expiration.
        The datetime.date will be converted to broker specific formats.
        IB Format: for options "YYYYMMDD", for futures "YYYYMM"
    strike : str
        Options strike as string.
    right : str
        'CALL' or 'PUT'
        default : ""
    multiplier : int
        Price multiplier.
        default : 1
    underlying_asset : Asset
        Underlying asset for options.

    Attributes
    ----------
    symbol : string (required)
        The symbol used to retrieve stock quotes if stock. The underlying
        symbol if option. For Forex: The base currency.
    asset_type : (string, default: `stock`)
        One of the following:
        - 'stock'
        - 'option'
        - 'future'
        - 'forex'
        - 'crypto'
        - 'multileg'
    expiration : datetime.date (required if asset_type is 'option' or 'future')
        Contract expiration dates for futures and options.
    strike : float (required if asset_type is 'option')
        Contract strike price.
    right : str (required if asset_type is 'option')
        Option call or put.
    multiplier : int  (required if asset_type is 'forex')
        Contract leverage over the underlying.
    precision : str (required if asset_type is 'crypto')
        Conversion currency.
    _right : list of str
        Acceptable values for right.

    Methods
    -------
    asset_type_must_be_one_of(@"asset_type")
        validates asset types.
    right_must_be_one_of(@"right")
        validates rights types.

    Example
    -------
    >>> # Create an Asset object for a stock.
    >>> from lumibot.entities import Asset
    >>> asset = Asset(symbol="AAPL")

    >>> # Create an Asset object for a futures contract.
    >>> from lumibot.entities import Asset
    >>> asset = Asset(symbol="ES", asset_type='future', expiration=datetime.date(2021, 12, 17))

    >>> # Create an Asset object for an options contract.
    >>> from lumibot.entities import Asset
    >>> asset = Asset(
    >>>     symbol="AAPL",
    >>>     asset_type='option',
    >>>     expiration=datetime.date(2021, 11, 26),
    >>>     strike=155,
    >>>     right= 'CALL',
    >>> )

    >>> # Create an Asset object for a FOREX contract.
    >>> from lumibot.entities import Asset
    >>> base_asset = Asset(symbol="USD", asset_type='forex')
    >>> quote_asset = Asset(symbol="EUR", asset_type='forex')
    >>> order = self.create_order(asset, 100, 'BUY', quote=quote_asset)
    >>> self.submit_order(order)

    >>> # Create an Asset object for crypto.
    >>> from lumibot.entities import Asset
    >>> base = Asset(symbol="BTC", asset_type='crypto')
    >>> quote = Asset(symbol="USDT", asset_type='crypto')
    >>> order = self.create_order(asset, 100, 'BUY', quote=quote)
    >>> self.submit_order(order)
    """

    class OptionRight(StrEnum):
        CALL = "CALL"
        PUT = "PUT"

    class AssetType(StrEnum):
        STOCK = "stock" # Stock
        OPTION = "option" # Option
        FUTURE = "future" # Future
        CONT_FUTURE = "cont_future" # Continuous future
        FOREX = "forex" # Forex or cash
        CRYPTO = "crypto" # Crypto
        INDEX = "index" # Index
        MULTILEG = "multileg" # Multileg option

    # Pull the rights from the OptionRight class
    _right: list = [v for k, v in OptionRight.__dict__.items() if not k.startswith("__")]

    def __init__(
        self,
        symbol: str,
        asset_type: str = AssetType.STOCK,
        expiration: date = None,
        strike: float = 0.0,
        right: str = None,
        multiplier: int = 1,
        precision: str = None,
        underlying_asset: "Asset" = None,
    ):
        """
        Parameters
        ----------
        symbol : str
            Symbol of the stock or underlying in case of futures/options.
        asset_type : str
            Type of the asset. Asset types are only 'stock', 'option', 'future', 'forex', 'crypto'
            default : 'stock'
        expiration : datetime.date
            Option or futures expiration.
            The datetime.date will be converted to broker specific formats.
            IB Format: for options "YYYYMMDD", for futures "YYYYMM"
        strike : str
            Options strike as string.
        right : str
            'CALL' or 'PUT'
            default : ""
        multiplier : int
            Price multiplier.
            default : 1
        underlying_asset : Asset
            Underlying asset for options.

        Raises
        ------
        ValueError
            If the asset type is not one of the accepted types.
        ValueError
            If the right is not one of the accepted types.

        Returns
        -------
        None
        """
        # Capitalize the symbol because some brokers require it
        self.symbol = symbol.upper() if symbol is not None else None
        self.asset_type = asset_type
        self.strike = strike
        self.multiplier = multiplier
        self.precision = precision
        self.underlying_asset = underlying_asset

        # If the underlying asset is set but the symbol is not, set the symbol to the underlying asset symbol
        if self.underlying_asset is not None and self.symbol is None:
            self.symbol = self.underlying_asset.symbol

        # If the expiration is a datetime object, convert it to date
        if isinstance(expiration, datetime):
            self.expiration = expiration.date()
        else:
            self.expiration = expiration

        # Multiplier for options must always be 100
        if asset_type == self.AssetType.OPTION:
            self.multiplier = 100

        # Make sure right is upper case
        if right is not None:
            self.right = right.upper()

        self.asset_type = self.asset_type_must_be_one_of(asset_type)
        self.right = self.right_must_be_one_of(right)

    @classmethod
    def symbol2asset(cls, symbol: str):
        """
        Convert a symbol string to an Asset object. This is particularly useful for converting option symbols.

        Parameters
        ----------
        symbol : str
            The symbol string to convert.

        Returns
        -------
        Asset
            The Asset object.
        """
        if not symbol:
            raise ValueError("Cannot convert an empty symbol to an Asset object.")

        symbol_info = parse_symbol(symbol)
        if symbol_info["type"] == "option":
            return Asset(
                symbol=symbol_info["stock_symbol"],
                asset_type="option",
                expiration=symbol_info["expiration_date"],
                strike=symbol_info["strike_price"],
                right=symbol_info["option_type"],
            )
        elif symbol_info["type"] == "stock":
            return Asset(symbol=symbol, asset_type="stock")
        elif symbol_info["type"] == "future":
            return Asset(symbol=symbol, asset_type="future", expiration=symbol_info["expiration_date"])
        elif symbol_info["type"] == "forex":
            return Asset(symbol=symbol, asset_type="forex")
        elif symbol_info["type"] == "crypto":
            return Asset(symbol=symbol, asset_type="crypto")
        else:
            return Asset(symbol=None)

    def __hash__(self):
        # Original hash implementation - keep this unchanged
        return hash((self.symbol, self.asset_type, self.expiration, self.strike, self.right))

    def __repr__(self):
        if self.asset_type == "future":
            return f"{self.symbol} {self.expiration}"
        elif self.asset_type == "option":
            return f"{self.symbol} {self.expiration} {self.strike} {self.right}"
        else:
            return f"{self.symbol}"

    def __str__(self):
        if self.asset_type == "future":
            return f"{self.symbol} {self.expiration}"
        elif self.asset_type == "option":
            return f"{self.symbol} {self.expiration} {self.strike} {self.right}"
        else:
            return f"{self.symbol}"

    def __eq__(self, other):
        # Check if other is None
        if other is None:
            return False

        # Check if other is an Asset object
        if not isinstance(other, Asset):
            return False

        return (
            self.symbol == other.symbol
            and self.asset_type == other.asset_type
            and self.expiration == other.expiration
            and self.strike == other.strike
            and self.right == other.right
        )

    def asset_type_must_be_one_of(self, v):
        # TODO: check if this works!
        if v == "us_equity":
            v = "stock"
        if v is None or isinstance(v, self.AssetType):
            return v

        v = v.lower()
        try:
            asset_type = self.AssetType(v)
        except ValueError:
            raise ValueError(f"`asset_type` must be one of {', '.join(self._asset_types)}")
        return asset_type

    def right_must_be_one_of(self, v):
        if v is None or isinstance(v, self.OptionRight):
            return v

        v = v.upper()
        try:
            right = self.OptionRight(v)
        except ValueError:
            valid_rights = ", ".join([x for x in self.OptionRight])
            raise ValueError(f"`right` must be one of {valid_rights}, uppercase") from None
        return right

    def is_valid(self):
        # All assets should have a symbol
        if self.symbol is None:
            return False

        # All assets should have an asset type
        if self.asset_type is None:
            return False

        # If it's an option it should have an expiration date, strike and right
        if self.asset_type == "option":
            if self.expiration is None:
                return False
            if self.strike is None:
                return False
            if self.right is None:
                return False

        return True

    # ========= Serialization methods ===========
    def to_dict(self):
        return {
            "symbol": self.symbol,
            "asset_type": self.asset_type,
            "expiration": self.expiration.strftime("%Y-%m-%d") if self.expiration else None,
            "strike": self.strike,
            "right": self.right,
            "multiplier": self.multiplier,
            "precision": self.precision,
            "underlying_asset": self.underlying_asset.to_dict() if self.underlying_asset else None,
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(
            symbol=data["symbol"],
            asset_type=data["asset_type"],
            expiration=datetime.strptime(data["expiration"], "%Y-%m-%d").date() if data["expiration"] else None,
            strike=data["strike"],
            right=data["right"],
            multiplier=data["multiplier"],
            precision=data["precision"],
            underlying_asset=cls.from_dict(data["underlying_asset"]) if data["underlying_asset"] else None,
        )

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
