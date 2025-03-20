from lumibot.entities import Asset
from lumibot.tools.alpaca_helpers import sanitize_base_and_quote_asset


def test_sanitize_base_and_quote_asset():
    """
    Test _sanitize_base_and_quote_asset method with various inputs:
    - Regular input with two Asset objects
    - Tuple input
    - String inputs for regular stocks
    - String inputs for option contracts
    - String inputs for forex quotes
    - String inputs for crypto
    - Mixed input types
    """

    # Create some Asset objects for testing
    stock_asset = Asset(symbol="AAPL")
    usd_asset = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
    option_asset = Asset(symbol="AAPL230915C00150000", asset_type=Asset.AssetType.OPTION)
    crypto_asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)

    # Regular input with two Asset objects
    base, quote = sanitize_base_and_quote_asset(stock_asset, usd_asset)
    assert base == stock_asset
    assert quote == usd_asset

    # Tuple input with Asset objects
    base, quote = sanitize_base_and_quote_asset((stock_asset, usd_asset), None)
    assert base == stock_asset
    assert quote == usd_asset

    # String input for regular stock
    base, quote = sanitize_base_and_quote_asset("AAPL", "USD")
    assert base.symbol == "AAPL"
    assert base.asset_type == Asset.AssetType.STOCK
    assert quote.symbol == "USD"
    assert quote.asset_type == Asset.AssetType.FOREX

    # String input for regular stock
    base, quote = sanitize_base_and_quote_asset("AAPL", usd_asset)
    assert base.symbol == "AAPL"
    assert base.asset_type == Asset.AssetType.STOCK
    assert quote.symbol == "USD"
    assert quote.asset_type == Asset.AssetType.FOREX

    # String input for option contract
    option_symbol = "AAPL230915C00150000"
    base, quote = sanitize_base_and_quote_asset(option_symbol, usd_asset)
    assert base.symbol == option_symbol
    assert base.asset_type == Asset.AssetType.OPTION
    assert quote.symbol == "USD"
    assert quote.asset_type == Asset.AssetType.FOREX

    # Tuple input with strings
    base, quote = sanitize_base_and_quote_asset(("AAPL", "USD"), None)
    assert base.symbol == "AAPL"
    assert base.asset_type == Asset.AssetType.STOCK
    assert quote.symbol == "USD"
    assert quote.asset_type == Asset.AssetType.FOREX

    # Mixed input (string and Asset)
    base, quote = sanitize_base_and_quote_asset("AAPL", usd_asset)
    assert base.symbol == "AAPL"
    assert base.asset_type == Asset.AssetType.STOCK
    assert quote == usd_asset

    # Option in tuple
    base, quote = sanitize_base_and_quote_asset((option_symbol, "USD"), None)
    assert base.symbol == option_symbol
    assert base.asset_type == Asset.AssetType.OPTION
    assert quote.symbol == "USD"
    assert quote.asset_type == Asset.AssetType.FOREX

    # Crypto with USD quote
    base, quote = sanitize_base_and_quote_asset(crypto_asset, "USD")
    assert base.symbol == "BTC"
    assert base.asset_type == Asset.AssetType.CRYPTO
    assert quote.symbol == "USD"
    assert quote.asset_type == Asset.AssetType.FOREX

    # Crypto with USD quote
    base, quote = sanitize_base_and_quote_asset(crypto_asset, usd_asset)
    assert base.symbol == "BTC"
    assert base.asset_type == Asset.AssetType.CRYPTO
    assert quote.symbol == "USD"
    assert quote.asset_type == Asset.AssetType.FOREX

    # # Crypto with crypto quote is unsupported
    # base, quote = sanitize_base_and_quote_asset("ETH", "BTC")
    # assert base.symbol == "ETH"
    # assert base.asset_type == Asset.AssetType.CRYPTO
    # assert quote.symbol == "BTC"
    # assert quote.asset_type == Asset.AssetType.CRYPTO

    # Pre-created crypto Asset objects
    base, quote = sanitize_base_and_quote_asset(crypto_asset, usd_asset)
    assert base == crypto_asset
    assert quote == usd_asset
