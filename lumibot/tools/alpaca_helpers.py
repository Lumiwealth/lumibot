from lumibot.entities import Asset
import re


def sanitize_base_and_quote_asset(base_asset, quote_asset) -> tuple[Asset, Asset]:
    # Handle asset tuple case
    if isinstance(base_asset, tuple):
        quote = base_asset[1]
        asset = base_asset[0]
    else:
        asset = base_asset
        quote = quote_asset

    # Handle string case
    if isinstance(asset, str):
        # Check if the string matches an option contract
        pattern = r'^[A-Z]{1,6}\d{6}[CP]\d{8}$'
        if re.match(pattern, asset) is not None:
            # Use Asset.symbol2asset to ensure full Asset parsing everywhere in lumibot
            asset = Asset.symbol2asset(asset)
        else:
            asset = Asset(symbol=asset)

    if isinstance(quote, str):
        quote = Asset(symbol=quote, asset_type=Asset.AssetType.FOREX)

    return asset, quote
