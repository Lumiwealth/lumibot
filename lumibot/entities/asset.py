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

    >>> # Create an Asset object for a futures contract with specific expiration.
    >>> from lumibot.entities import Asset
    >>> asset = Asset(symbol="ES", asset_type='future', expiration=datetime.date(2021, 12, 17))

    >>> # Create an Asset object for a futures contract with auto-expiry (front month).
    >>> from lumibot.entities import Asset
    >>> asset = Asset(symbol="MES", asset_type=Asset.AssetType.FUTURE, auto_expiry=Asset.AutoExpiry.FRONT_MONTH)

    >>> # Create an Asset object for a futures contract with auto-expiry (next quarter).
    >>> from lumibot.entities import Asset
    >>> asset = Asset(symbol="ES", asset_type=Asset.AssetType.FUTURE, auto_expiry=Asset.AutoExpiry.NEXT_QUARTER)

    >>> # Create an Asset object for a continuous futures contract (recommended for backtesting).
    >>> from lumibot.entities import Asset
    >>> asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)

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
        CRYPTO_FUTURE = "crypto_future" # Crypto Future
        CONT_FUTURE = "cont_future" # Continuous future
        FOREX = "forex" # Forex or cash
        CRYPTO = "crypto" # Crypto
        INDEX = "index" # Index
        MULTILEG = "multileg" # Multileg option
    
    class AutoExpiry(StrEnum):
        FRONT_MONTH = "front_month" # Front month (nearest quarterly expiry)
        NEXT_QUARTER = "next_quarter" # Next quarterly expiry (same as front month for quarterly contracts)
        AUTO = "auto" # Auto (default to front month behavior)

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
        leverage: int = 1,
        precision: str = None,
        underlying_asset: "Asset" = None,
        auto_expiry: str = None,
    ):
        """
        Parameters
        ----------
        symbol : str
            Symbol of the stock or underlying in case of futures/options.
        asset_type : str
            Type of the asset. Asset types are only 'stock', 'option', 'future', 'forex', 'crypto', 'crypto_future'
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
        auto_expiry : str or Asset.AutoExpiry, optional
            Automatic expiry resolution for futures. Options:
            - Asset.AutoExpiry.FRONT_MONTH: Always use the front month (nearest quarterly expiry)
            - Asset.AutoExpiry.NEXT_QUARTER: Use the next quarterly expiry (same as front month for quarterly contracts)
            - Asset.AutoExpiry.AUTO: Use front_month behavior
            If specified, this overrides the expiration parameter for futures.

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

        # Leverage for futures assets (ignored for other asset types)
        self.leverage = leverage if asset_type == self.AssetType.FUTURE else 1

        # If the underlying asset is set but the symbol is not, set the symbol to the underlying asset symbol
        if self.underlying_asset is not None and self.symbol is None:
            self.symbol = self.underlying_asset.symbol

        # If the expiration is a datetime object, convert it to date
        if isinstance(expiration, datetime):
            self.expiration = expiration.date()
        else:
            self.expiration = expiration

        # Handle auto expiry for futures
        self.auto_expiry = auto_expiry
        if auto_expiry and asset_type == self.AssetType.FUTURE and self.expiration is None:
            # Only use auto_expiry if no manual expiration was provided
            self.expiration = self._calculate_auto_expiry(auto_expiry)

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
            "leverage": self.leverage,
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
            leverage=data.get("leverage", 1),
            precision=data["precision"],
            underlying_asset=cls.from_dict(data["underlying_asset"]) if data["underlying_asset"] else None,
        )

    def _calculate_auto_expiry(self, auto_expiry):
        """
        Calculate automatic expiry date for futures contracts
        
        Parameters
        ----------
        auto_expiry : str
            Type of auto expiry: 'front_month', 'next_quarter', 'auto', or True
            
        Returns
        -------
        datetime.date
            The calculated expiry date
        """
        from datetime import date, datetime, timedelta
        
        current_date = date.today()
        
        # Handle different auto_expiry options
        if auto_expiry in [True, Asset.AutoExpiry.AUTO, Asset.AutoExpiry.FRONT_MONTH]:
            return self._get_front_month_expiry(current_date)
        elif auto_expiry == Asset.AutoExpiry.NEXT_QUARTER:
            return self._get_next_quarterly_expiry(current_date)
        else:
            # Default to front month if unrecognized option
            return self._get_front_month_expiry(current_date)
    
    def _get_front_month_expiry(self, current_date):
        """Get the front month (nearest) futures expiry"""
        # Standard futures expiry: 3rd Friday of the month
        # For active months, use quarterly cycle: Mar, Jun, Sep, Dec
        quarterly_months = [3, 6, 9, 12]
        
        current_year = current_date.year
        current_month = current_date.month
        
        # Find the next quarterly month
        target_month = None
        target_year = current_year
        
        for month in quarterly_months:
            if month >= current_month:
                target_month = month
                break
        
        # If no month found in current year, use March of next year
        if target_month is None:
            target_month = 3
            target_year += 1
        
        # Calculate 3rd Friday of the target month
        third_friday = self._get_third_friday(target_year, target_month)
        
        # If we're in the expiry month and past the 3rd Friday, move to next quarter
        if target_year == current_year and target_month == current_month and current_date >= third_friday:
            next_quarter_idx = (quarterly_months.index(target_month) + 1) % len(quarterly_months)
            if next_quarter_idx == 0:  # Wrapped around to March of next year
                target_year += 1
                target_month = 3
            else:
                target_month = quarterly_months[next_quarter_idx]
            third_friday = self._get_third_friday(target_year, target_month)
        
        return third_friday
    
    def _get_next_quarterly_expiry(self, current_date):
        """Get the next quarterly expiry (Mar, Jun, Sep, Dec)"""
        # This is the same as front month for most futures since they follow quarterly cycles
        return self._get_front_month_expiry(current_date)
    
    def _get_third_friday(self, year, month):
        """Calculate the 3rd Friday of a given month/year"""
        from datetime import date, timedelta
        
        # Start with the first day of the month
        first_day = date(year, month, 1)
        
        # Find the first Friday
        days_until_friday = (4 - first_day.weekday()) % 7  # Friday is day 4
        first_friday = first_day + timedelta(days=days_until_friday)
        
        # Add 14 days to get the third Friday
        third_friday = first_friday + timedelta(days=14)
        
        return third_friday

    # ========== Continuous Futures Resolution Methods ==========
    
    def resolve_continuous_futures_contract(self, reference_date: datetime = None) -> str:
        """
        Resolve a continuous futures asset to a specific contract symbol.
        
        This method is used to convert continuous futures (CONT_FUTURE) to the appropriate
        active contract based on standard market conventions.
        
        Parameters
        ----------
        reference_date : datetime, optional
            Reference date for contract resolution. If None, uses current date.
        
        Returns
        -------
        str
            Specific futures contract symbol (e.g., 'MESU25' for MES September 2025)
            
        Raises
        ------
        ValueError
            If called on a non-continuous futures asset
        """
        if self.asset_type != self.AssetType.CONT_FUTURE:
            raise ValueError(f"resolve_continuous_futures_contract() can only be called on CONT_FUTURE assets, got {self.asset_type}")
        
        return self._generate_current_futures_contract(reference_date)
    
    def get_potential_futures_contracts(self) -> list:
        """
        Get a list of potential futures contracts in order of preference.
        
        This is useful for data sources or brokers that need to try multiple
        contract symbols to find available data.
        
        Returns
        -------
        list
            List of potential contract symbols in order of preference
            
        Raises
        ------
        ValueError
            If called on a non-continuous futures asset
        """
        if self.asset_type != self.AssetType.CONT_FUTURE:
            raise ValueError(f"get_potential_futures_contracts() can only be called on CONT_FUTURE assets, got {self.asset_type}")
        
        return self._generate_potential_contracts()
    
    def _generate_current_futures_contract(self, reference_date: datetime = None) -> str:
        """
        Generate the most appropriate futures contract for the given date.
        
        Parameters
        ----------
        reference_date : datetime, optional
            Reference date for contract resolution. If None, uses current date.
        
        Returns
        -------
        str
            Contract symbol (e.g., 'MESU25')
        """
        from datetime import datetime
        
        month_codes = {
            1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
            7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
        }
        
        if reference_date is None:
            reference_date = datetime.now()
        
        current_month = reference_date.month
        current_year = reference_date.year
        
        # Use quarterly contracts (Mar, Jun, Sep, Dec) which are typically most liquid
        # Standard quarterly contract rollover logic based on market conventions
        # We use the current quarter if it hasn't expired, otherwise the next quarter
        if current_month >= 10:  # October onwards, use December
            target_month = 12  # December
            target_year = current_year
        elif current_month >= 7:  # July-September, use September
            target_month = 9  # September
            target_year = current_year
        elif current_month >= 4:  # April-June, use June
            target_month = 6  # June
            target_year = current_year
        elif current_month >= 1:  # Jan-March, use March
            target_month = 3  # March
            target_year = current_year
        else:  # December (fallback), use March next year
            target_month = 3  # March
            target_year = current_year + 1
        
        month_code = month_codes.get(target_month, 'U')  # Default to September
        year_code = target_year % 100
        
        contract = f"{self.symbol}{month_code}{year_code:02d}"
        return contract
    
    def _generate_potential_contracts(self) -> list:
        """
        Generate potential contract symbols in order of preference.
        
        Returns
        -------
        list
            List of contract symbols
        """
        from datetime import datetime
        
        month_codes = {
            1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
            7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
        }
        
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        
        potential_contracts = []
        
        # Generate quarterly contracts based on current month
        if current_month >= 10:  # October onwards, use Dec, then Mar
            target_quarters = [
                (12, current_year),     # December this year
                (3, current_year + 1),  # March next year
                (6, current_year + 1),  # June next year
            ]
        elif current_month >= 7:  # July-September, use Sep, then Dec
            target_quarters = [
                (9, current_year),      # September this year
                (12, current_year),     # December this year
                (3, current_year + 1),  # March next year
            ]
        elif current_month >= 4:  # April-June - use Sep, then Dec
            target_quarters = [
                (9, current_year),      # September this year
                (12, current_year),     # December this year
                (3, current_year + 1),  # March next year
            ]
        else:  # Jan-March - use Jun, then Sep
            target_quarters = [
                (6, current_year),      # June this year
                (9, current_year),      # September this year
                (12, current_year),     # December this year
            ]
        
        # Generate quarterly contract symbols in multiple formats
        for month, year in target_quarters:
            month_code = month_codes.get(month, 'Z')
            year_code = year % 100
            
            # Format 1: Standard futures format (MESZ25)
            contract1 = f"{self.symbol}{month_code}{year_code:02d}"
            potential_contracts.append(contract1)
            
            # Format 2: With dot separator (MES.Z25) - some platforms use this
            contract2 = f"{self.symbol}.{month_code}{year_code:02d}"
            potential_contracts.append(contract2)
            
            # Format 3: Full year format (MESZ2025) - some platforms use this
            contract3 = f"{self.symbol}{month_code}{year}"
            potential_contracts.append(contract3)
        
        # Also add some monthly contracts as backup
        for month_offset in range(1, 4):  # Next 3 months
            target_month = current_month + month_offset
            target_year = current_year
            
            # Handle year rollover
            while target_month > 12:
                target_month -= 12
                target_year += 1
            
            month_code = month_codes.get(target_month, 'H')
            year_code = target_year % 100
            
            contract = f"{self.symbol}{month_code}{year_code:02d}"
            if contract not in potential_contracts:  # Avoid duplicates
                potential_contracts.append(contract)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_contracts = []
        for contract in potential_contracts:
            if contract not in seen:
                seen.add(contract)
                unique_contracts.append(contract)
        
        return unique_contracts


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
