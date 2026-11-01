"""Currency conversion over the exchange's CCXT market and ticker structures."""
from copy import deepcopy
from decimal import Decimal
from unittest.mock import Mock

import pytest

from lumibot.entities import Asset
from tests.test_ccxt_unified_spot import make_broker


def conversion_market(template, symbol, base, quote, *, contract=False, option=False, active=True):
    # Shape follows CCXT load_markets; symbols and prices are neutral synthetic data.
    market = deepcopy(template)
    market.update(id=symbol, symbol=symbol, base=base, quote=quote, baseId=base,
                  quoteId=quote, active=active, spot=not contract, contract=contract,
                  swap=contract and not option, option=option,
                  type='option' if option else 'swap' if contract else 'spot')
    if contract:
        market.update(settle=base, settleId=base, linear=False, inverse=True, contractSize=100)
    return market


@pytest.mark.parametrize('exchange', ['phemex', 'coinbase', 'kraken', 'kucoin'])
def test_account_equity_values_currency_without_a_direct_reporting_pair(make_broker, exchange):
    broker = make_broker(exchange)
    template = broker.api.markets['BTC/USDT']
    broker.api.set_markets([
        conversion_market(template, 'AAA/CCC', 'AAA', 'CCC'),
        conversion_market(template, 'AAA/USD:AAA', 'AAA', 'USD', contract=True),
    ])
    broker._fetch_balance = Mock(return_value={
        'total': {'CCC': 4}, 'free': {'CCC': 4}, 'used': {'CCC': 0},
    })
    broker._phemex_contract_accounts = Mock(return_value={'balances': {}, 'positions': []})
    prices = {'AAA/CCC': Decimal('100'), 'AAA/USD:AAA': Decimal('250')}
    broker.api.fetch_ticker = Mock(side_effect=lambda symbol: {'last': prices[symbol]})

    # Four CCC / 100 CCC per AAA * 250 USD per AAA. Never assume currency parity.
    assert broker._get_balances_at_broker(Asset('USD', 'forex'), None) == (0.0, 10.0, 10.0)
    assert {call.args[0] for call in broker.api.fetch_ticker.call_args_list} == set(prices)


def test_cross_currency_conversion_preserves_negative_settlement_pnl(make_broker):
    broker = make_broker()
    template = broker.api.markets['BTC/USDT']
    broker.api.set_markets([
        conversion_market(template, 'CCC/BBB', 'CCC', 'BBB'),
        conversion_market(template, 'USD/BBB', 'USD', 'BBB'),
    ])
    prices = {'CCC/BBB': Decimal('3'), 'USD/BBB': Decimal('2')}
    broker.api.fetch_ticker = Mock(side_effect=lambda symbol: {'last': prices[symbol]})

    assert broker._currency_value('CCC', Decimal('-8'), Asset('USD', 'forex')) == Decimal('-12')


def test_spot_conversion_path_precedes_a_contract_reference(make_broker):
    broker = make_broker()
    template = broker.api.markets['BTC/USDT']
    broker.api.set_markets([
        conversion_market(template, 'AAA/CCC', 'AAA', 'CCC'),
        conversion_market(template, 'AAA/USD:AAA', 'AAA', 'USD', contract=True),
        conversion_market(template, 'CCC/BBB', 'CCC', 'BBB'),
        conversion_market(template, 'BBB/DDD', 'BBB', 'DDD'),
        conversion_market(template, 'DDD/USD', 'DDD', 'USD'),
    ])
    prices = {'CCC/BBB': 2, 'BBB/DDD': 3, 'DDD/USD': 4}
    broker.api.fetch_ticker = Mock(side_effect=lambda symbol: {'last': prices[symbol]})

    assert broker._currency_value('CCC', Decimal('5'), Asset('USD', 'forex')) == 120
    assert {call.args[0] for call in broker.api.fetch_ticker.call_args_list} == set(prices)


@pytest.mark.parametrize('unusable', ['option', 'inactive'])
def test_currency_paths_do_not_treat_option_premiums_or_retired_contracts_as_fx(make_broker, unusable):
    broker = make_broker()
    template = broker.api.markets['BTC/USDT']
    broker.api.set_markets([
        conversion_market(template, 'AAA/CCC', 'AAA', 'CCC'),
        conversion_market(template, 'AAA/USD:AAA', 'AAA', 'USD', contract=True,
                          option=unusable == 'option', active=unusable != 'inactive'),
    ])
    broker.api.fetch_ticker = Mock()

    assert broker._currency_value('CCC', Decimal('4'), Asset('USD', 'forex')) == 0
    broker.api.fetch_ticker.assert_not_called()


@pytest.mark.parametrize('symbol,price,expected', [
    ('CCC/USD', 3, 12), ('USD/CCC', 2, 2),
])
def test_existing_direct_and_inverse_conversions_keep_one_price_read(make_broker, symbol, price, expected):
    broker = make_broker()
    template = broker.api.markets['BTC/USDT']
    base, quote = symbol.split('/')
    broker.api.set_markets([conversion_market(template, symbol, base, quote)])
    broker.api.fetch_ticker = Mock(return_value={'last': price})

    assert broker._currency_value('CCC', Decimal('4'), Asset('USD', 'forex')) == expected
    broker.api.fetch_ticker.assert_called_once_with(symbol)
