"""AlpacaData.get_quotes: many option quotes per request, the same Quote get_quote builds.

Strategy.get_quotes and OptionsHelper's delta pick use this bulk adapter so a live pick
needs one request for all candidate strikes instead of one per strike.
"""

import datetime as dt
from types import SimpleNamespace
from unittest.mock import Mock

from lumibot.data_sources import AlpacaData
from lumibot.entities import Asset


def _option(strike):
    return Asset("SPY", asset_type="option", expiration=dt.date(2026, 12, 18), strike=strike, right="CALL")


def _native(strike):
    return SimpleNamespace(bid_price=strike / 100, ask_price=strike / 100 + 0.1, bid_size=3.0, ask_size=4.0,
                           timestamp=dt.datetime(2026, 11, 11, 15, 0, tzinfo=dt.timezone.utc),
                           ask_exchange="C", conditions=" ")


def _source(missing=()):
    source = AlpacaData({"API_KEY": "test", "API_SECRET": "test"})
    client = Mock()

    def latest(request):
        symbols = request.symbol_or_symbols
        symbols = [symbols] if isinstance(symbols, str) else list(symbols)
        out = {}
        for symbol in symbols:
            strike = int(symbol[-8:]) / 1000
            if strike not in missing:
                out[symbol] = _native(strike)
        return out

    client.get_option_latest_quote.side_effect = latest
    source._option_client = client
    return source, client


def _fields(quote):
    return (quote.asset, quote.price, quote.bid, quote.ask, quote.bid_size, quote.ask_size, quote.timestamp,
            quote.raw_data["symbol"], quote.raw_data["exchange"])


def test_one_request_per_100_contracts_and_same_quotes_as_get_quote():
    assets = [_option(400 + k) for k in range(150)]
    source, client = _source()
    quotes = source.get_quotes(assets)
    assert [len(c.args[0].symbol_or_symbols) for c in client.get_option_latest_quote.call_args_list] == [100, 50]
    single, _ = _source()
    for asset in assets:
        assert _fields(quotes[asset]) == _fields(single.get_quote(asset))


def test_contract_without_a_quote_maps_to_none():
    assets = [_option(500), _option(505)]
    source, _ = _source(missing={505.0})
    quotes = source.get_quotes(assets)
    assert quotes[assets[1]] is None
    assert quotes[assets[0]].bid == 5.0
