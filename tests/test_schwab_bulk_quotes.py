"""Exact-contract quote batching; no broker credentials or network writes."""
from datetime import date
from types import SimpleNamespace

import pytest

from lumibot.data_sources.schwab_data import SchwabData, _schwab_market_symbol
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy


def source(payload, status=200):
    calls = []
    def get_quotes(symbols):
        calls.append(symbols)
        return SimpleNamespace(status_code=status, json=lambda: payload)
    return SchwabData(client=SimpleNamespace(get_quotes=get_quotes), auto_create_client=False), calls


def test_bulk_maps_exact_contract_not_same_strike_adjusted_contract():
    option = Asset('ABC', asset_type='option', expiration=date(2026, 10, 16), strike=20, right='CALL')
    symbol = _schwab_market_symbol(option)
    data, calls = source({symbol: {'quote': {'bidPrice': .1, 'askPrice': .12, 'quoteTime': 1792080000000}},
                          'ABC1  261016C00020000': {'quote': {'bidPrice': 1, 'askPrice': 1.2}}})
    quotes = data.get_quotes([option])
    assert calls == [[symbol]]
    assert quotes[option].asset is option
    assert quotes[option].bid == .1
    assert quotes[option].quote_time.timestamp() == 1792080000


def test_stocks_options_and_duplicates_use_one_request_and_preserve_missing():
    stock, absent = Asset('ABC'), Asset('MISSING')
    option = Asset('ABC', asset_type='option', expiration=date(2026, 10, 16), strike=20, right='CALL')
    sym = _schwab_market_symbol(option)
    data, calls = source({'ABC': {'quote': {'bidPrice': 23, 'askPrice': 24}}, sym: {'quote': {'lastPrice': 3}}})
    result = data.get_quotes([stock, option, absent, stock])
    assert calls == [['ABC', sym, 'MISSING']]
    assert list(result) == [stock, option, absent]
    assert result[absent] is None
    assert result[option].bid is None and result[option].ask is None
    assert result[option].price == 3 and result[option].quote_time is None


@pytest.mark.parametrize('status', [401, 429, 500])
def test_bulk_provider_errors_are_visible_and_do_not_retry_serially(status):
    data, calls = source({}, status)
    with pytest.raises(ConnectionError):
        data.get_quotes([Asset('ABC'), Asset('XYZ')])
    assert len(calls) == 1


def test_empty_assets_make_no_request_and_crypto_rejected_before_network():
    data, calls = source({})
    assert data.get_quotes([]) == {}
    with pytest.raises(ValueError):
        data.get_quotes([Asset('ABC'), Asset('BTC', asset_type='crypto')])
    assert not calls


def test_large_batch_is_bounded_without_dropping_contracts():
    calls = []
    def get_quotes(symbols):
        calls.append(symbols)
        return SimpleNamespace(status_code=200, json=lambda: {symbol: {'quote': {'bidPrice': 1, 'askPrice': 2}} for symbol in symbols})
    data = SchwabData(client=SimpleNamespace(get_quotes=get_quotes), auto_create_client=False)
    assets = [Asset(f'S{i}') for i in range(101)]
    result = data.get_quotes(assets)
    assert len(calls) == 3 and max(map(len, calls)) <= 50
    assert list(result) == assets


def test_public_strategy_uses_correct_data_sources_and_preserves_serial_adapter():
    stock = Asset('ABC')
    option = Asset('ABC', asset_type='option', expiration=date(2026, 10, 16), strike=20, right='CALL')
    native_calls, fallback_calls = [], []
    def native(assets, quote=None, exchange=None):
        native_calls.append((assets, quote, exchange))
        return {asset: SimpleNamespace(asset=asset, bid=1) for asset in assets}
    strategy = SimpleNamespace(
        _sanitize_user_asset=lambda asset: Asset(asset) if isinstance(asset, str) else asset,
        broker=SimpleNamespace(data_source=SimpleNamespace(get_quotes=native), option_source=SimpleNamespace()),
        get_quote=lambda asset, quote=None, exchange=None: fallback_calls.append(asset) or SimpleNamespace(asset=asset, bid=2),
    )
    result = Strategy.get_quotes(strategy, ['ABC', option, stock], exchange='TEST')
    assert native_calls == [([stock], None, 'TEST')]
    assert fallback_calls == [option]
    assert result[stock].bid == 1 and result[option].bid == 2


def test_public_strategy_never_masks_native_batch_failure_with_serial_reads():
    def native(*args, **kwargs):
        raise ConnectionError('failure')
    strategy = SimpleNamespace(_sanitize_user_asset=lambda asset: asset,
        broker=SimpleNamespace(data_source=SimpleNamespace(get_quotes=native), option_source=None),
        get_quote=lambda *args, **kwargs: pytest.fail('must not fall back after provider error'))
    with pytest.raises(ConnectionError):
        Strategy.get_quotes(strategy, [Asset('ABC')])


def test_singleton_preserves_quote_fields_and_none_on_error():
    asset = Asset('ABC')
    data, calls = source({'abc': {'quote': {'bidPrice': 1, 'askPrice': 2, 'lastPrice': 1.25, 'bidSize': 3}}})
    result = data.get_quote(asset)
    assert result.asset is asset and result.bid == 1 and result.ask == 2
    assert result.price == 1.25 and result.bid_size == 3
    assert calls == [['ABC']]
    data, _ = source({}, status=500)
    assert data.get_quote(asset) is None
    data, _ = source({'ABC': {}})
    assert data.get_quote(asset) is None


@pytest.mark.parametrize('payload', [[], {'ABC': {'symbol': 'ABC1', 'quote': {'bidPrice': 1}}}, {'ABC': 'malformed'}])
def test_malformed_or_wrong_contract_payloads_are_visible(payload):
    data, calls = source(payload)
    with pytest.raises(ValueError):
        data.get_quotes([Asset('ABC')])
    assert len(calls) == 1


def test_public_strategy_rejects_native_quote_for_another_asset():
    asset = Asset('ABC')
    strategy = SimpleNamespace(_sanitize_user_asset=lambda asset: asset,
        broker=SimpleNamespace(data_source=SimpleNamespace(get_quotes=lambda *a, **k: {asset: SimpleNamespace(asset=Asset('XYZ'))}), option_source=None))
    with pytest.raises(ValueError, match='asset'):
        Strategy.get_quotes(strategy, [asset])


def test_one_malformed_quote_member_does_not_erase_healthy_batch_quotes():
    """A batch optimization must retain the singleton read's per-asset outcome."""
    broken, healthy = Asset('ABC'), Asset('XYZ')
    data, calls = source({'ABC': {'quote': []}, 'XYZ': {'quote': {'bidPrice': 23, 'askPrice': 24}}})
    assert data.get_quote(broken) is None
    assert data.get_quote(healthy).bid == 23
    calls.clear()
    result = data.get_quotes([broken, healthy])
    assert result[broken] is None
    assert result[healthy].bid == 23
    assert result[healthy].ask == 24
    assert calls == [['ABC', 'XYZ']]
