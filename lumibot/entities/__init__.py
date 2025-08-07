from .asset import Asset, AssetsMapping
from .bar import Bar
from .dataline import Dataline
from .order import Order
from .position import Position
from .trading_fee import TradingFee
from .quote import Quote
from .chains import Chains

# Import base implementations
from .bars import Bars as _BarsBase
from .data import Data as _DataBase

# Try to import polars versions
try:
    from .bars_polars import BarsPolars
    from .data_polars import DataPolars
    _POLARS_AVAILABLE = True
except ImportError:
    BarsPolars = None
    DataPolars = None
    _POLARS_AVAILABLE = False

# Dynamic selection based on configuration
def _get_bars_class():
    """Get the appropriate Bars class based on configuration."""
    try:
        from lumibot.config import use_polars
        if use_polars() and _POLARS_AVAILABLE:
            return BarsPolars
    except ImportError:
        pass
    return _BarsBase

def _get_data_class():
    """Get the appropriate Data class based on configuration."""
    try:
        from lumibot.config import use_polars
        if use_polars() and _POLARS_AVAILABLE:
            return DataPolars
    except ImportError:
        pass
    return _DataBase

# Create wrapper classes that dynamically select implementation
class Bars(_BarsBase):
    """Bars class that automatically uses polars backend if configured."""
    def __new__(cls, *args, **kwargs):
        implementation = _get_bars_class()
        if implementation != _BarsBase:
            return implementation(*args, **kwargs)
        return super().__new__(cls)

class Data(_DataBase):
    """Data class that automatically uses polars backend if configured."""
    def __new__(cls, *args, **kwargs):
        implementation = _get_data_class()
        if implementation != _DataBase:
            return implementation(*args, **kwargs)
        return super().__new__(cls)
