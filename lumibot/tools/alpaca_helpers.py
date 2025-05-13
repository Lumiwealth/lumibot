import re
import datetime as dt
from decimal import Decimal
from typing import Union, Tuple, Optional

from lumibot.entities import Asset
from lumibot import LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL, LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE

_OPTION_SYMBOL_RE = re.compile(r'^([A-Z]+)(\d{6})([CP])(\d{8})$')

def sanitize_base_and_quote_asset(
    base_asset: Union[str, Asset, Tuple[Union[str, Asset], Union[str, Asset]]],
    quote_asset: Optional[Union[str, Asset]] = None
) -> Tuple[Asset, Asset]:
    """
    Normalize base_asset and quote_asset to Asset instances.

    - If base_asset is a tuple, it's interpreted as (asset, quote) pair.
    - If base_asset is an Asset instance, it's used directly.
    - If base_asset is a string matching an option symbol (e.g. 'AAPL230915C00150000'),
      it's parsed into an OPTION Asset.
    - If base_asset is a string containing '/', it's parsed as a CRYPTO pair (e.g. 'BTC/USD').
    - Otherwise, base_asset string is treated as STOCK.

    For quote_asset:
    - If provided and is an Asset, it's used directly.
    - If provided and is a string, it's treated as CASH (FOREX).
    - If not provided, the default quote asset (USD CASH) is used.
    """
    # Handle tuple input
    if isinstance(base_asset, tuple) and len(base_asset) == 2:
        asset_input, quote_input = base_asset
    else:
        asset_input = base_asset
        quote_input = quote_asset

    # Parse base asset
    if isinstance(asset_input, Asset):
        parsed_asset = asset_input

    elif isinstance(asset_input, str):
        # Crypto pair case, e.g. "BTC/USD"
        if '/' in asset_input:
            base_sym, quote_sym = asset_input.split('/', 1)
            parsed_asset = Asset(base_sym, Asset.AssetType.CRYPTO)
            quote_input = quote_sym

        else:
            m = _OPTION_SYMBOL_RE.match(asset_input)
            if m:
                underlying, exp_str, right_char, strike_str = m.groups()
                expiration = dt.datetime.strptime(exp_str, '%y%m%d').date()
                strike = Decimal(int(strike_str)) / Decimal('1000')
                right = 'call' if right_char.upper() == 'C' else 'put'
                # Use the full option symbol as the Asset.symbol
                parsed_asset = Asset(
                    asset_input,
                    Asset.AssetType.OPTION,
                    expiration=expiration,
                    strike=strike,
                    right=right
                )
            else:
                parsed_asset = Asset(asset_input, Asset.AssetType.STOCK)

    else:
        raise TypeError(f"Unsupported type for base_asset: {type(asset_input)}")

    # Parse quote asset
    if isinstance(quote_input, Asset):
        parsed_quote = quote_input
    elif isinstance(quote_input, str):
        parsed_quote = Asset(quote_input, Asset.AssetType.FOREX)
    elif quote_input is None:
        parsed_quote = Asset(
            LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL,
            LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE
        )
    else:
        raise TypeError(f"Unsupported type for quote_asset: {type(quote_input)}")

    return parsed_asset, parsed_quote
