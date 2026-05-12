from __future__ import annotations

import datetime as dt
import re
from typing import TypeAlias

from lumibot.constants import LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL, LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE
from lumibot.entities.asset import Asset

_OPTION_SYMBOL_RE = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")
AssetInput: TypeAlias = str | Asset  # noqa: UP040 - keep Python 3.11 parser compatibility.


def sanitize_base_and_quote_asset(
    base_asset: AssetInput | tuple[AssetInput, AssetInput],
    quote_asset: AssetInput | None = None,
) -> tuple[Asset, Asset]:
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
    if isinstance(base_asset, tuple):
        if len(base_asset) != 2:
            raise TypeError(f"Expected a 2-item asset pair tuple, got {len(base_asset)} items")
        asset_input: AssetInput = base_asset[0]
        quote_input: AssetInput | None = base_asset[1]
    else:
        asset_input = base_asset
        quote_input = quote_asset

    # Parse base asset
    if isinstance(asset_input, Asset):
        parsed_asset = asset_input

    else:
        # Crypto pair case, e.g. "BTC/USD"
        if "/" in asset_input:
            base_sym, quote_sym = asset_input.split("/", 1)
            parsed_asset = Asset(base_sym, Asset.AssetType.CRYPTO)
            quote_input = quote_sym

        else:
            m = _OPTION_SYMBOL_RE.match(asset_input)
            if m:
                _underlying, exp_str, right_char, strike_str = m.groups()
                expiration = dt.datetime.strptime(exp_str, "%y%m%d").date()
                strike = int(strike_str) / 1000
                right = "call" if right_char.upper() == "C" else "put"
                # Use the full option symbol as the Asset.symbol
                parsed_asset = Asset(
                    asset_input, Asset.AssetType.OPTION, expiration=expiration, strike=strike, right=right
                )
            else:
                parsed_asset = Asset(asset_input, Asset.AssetType.STOCK)

    # Parse quote asset
    if isinstance(quote_input, Asset):
        parsed_quote = quote_input
    elif isinstance(quote_input, str):
        parsed_quote = Asset(quote_input, Asset.AssetType.FOREX)
    elif quote_input is None:
        parsed_quote = Asset(LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL, LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE)
    else:
        raise TypeError(f"Unsupported type for quote_asset: {type(quote_input)}")

    return parsed_asset, parsed_quote
