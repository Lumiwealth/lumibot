"""CCXT contract identity and quantities, independent of settlement currency."""

from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.entities import Asset, Order
from lumibot.tools.symbol_normalization import build_ccxt_crypto_symbol, resolve_ccxt_market_symbol
from tests.test_ccxt_unified_spot import make_broker


@pytest.fixture
def derivatives(make_broker):
    broker = make_broker()
    spot = broker.api.markets["BTC/USDT"]
    markets = [spot]
    for quote, settle, inverse in (("USDT", "USDT", False), ("USDC", "USDC", False), ("USD", "BTC", True)):
        market = deepcopy(spot)
        market.update(id=f"BTC{quote}", symbol=f"BTC/{quote}:{settle}", quote=quote,
                      quoteId=quote, settle=settle, settleId=settle, type="swap", spot=False,
                      swap=True, contract=True, linear=not inverse, inverse=inverse,
                      contractSize=1, priceScale=4, valueScale=8)
        market["precision"] = {"amount": 1, "price": 0.5}
        market["info"] = {"type": "Perpetual" if inverse else "PerpetualV2"}
        markets.append(market)
    broker.api.set_markets(markets)
    broker.data_source.get_last_price = type(broker.data_source).get_last_price.__get__(broker.data_source)
    return broker


@pytest.mark.parametrize("symbol", ["BTC/USDT:USDT", "BTC/USDC:USDC", "BTC/USD:BTC"])
def test_derivative_symbol_keeps_settlement_in_every_data_path(derivatives, symbol):
    asset = Asset(symbol, asset_type="crypto_future")
    quote = Asset(symbol.split("/")[1].split(":")[0], asset_type="crypto")
    assert build_ccxt_crypto_symbol(symbol, quote.symbol) == symbol
    derivatives.api.fetch_ticker = Mock(return_value={"last": 20000})
    assert derivatives.data_source.get_last_price(asset, quote) == 20000
    derivatives.api.fetch_ticker.assert_called_once_with(symbol)


def test_missing_settlement_market_never_falls_back_to_spot():
    assert resolve_ccxt_market_symbol({"BTC/USDT": {}}, "BTC/USDT:USDC") is None


@pytest.mark.parametrize("symbol", ["BTC/USDT:USDT", "BTC/USDC:USDC", "BTC/USD:BTC"])
def test_contract_order_keeps_symbol_contract_count_and_short_leg(derivatives, symbol):
    asset = Asset(symbol, asset_type="crypto_future")
    submitted = Order("strategy", asset, 4, "buy", position_side="short")
    submitted.reduce_only = True

    derivatives._submit_order(submitted)

    args, kwargs = derivatives.api.create_order.call_args
    assert args[:4] == (symbol, "market", "buy", "4")
    assert kwargs["params"]["reduceOnly"]
    assert kwargs["params"]["posSide"] == "Short"


@pytest.mark.parametrize("symbol", ["BTC/USDT:USDT", "BTC/USDC:USDC", "BTC/USD:BTC"])
def test_position_snapshot_preserves_contract_metadata_and_both_legs(derivatives, symbol):
    derivatives.api.fetch_balance = Mock(return_value={"total": {}})
    derivatives._fetch_derivative_positions = Mock(return_value=[
        {"symbol": symbol, "contracts": 10, "side": "long", "hedged": True,
         "contractSize": 1, "entryPrice": 19000, "markPrice": 20000, "unrealizedPnl": 100,
         "info": {"posSide": "Long", "execSeq": "changing-sequence"}},
        {"symbol": symbol, "contracts": 4, "side": "short", "hedged": True,
         "contractSize": 1, "entryPrice": 21000, "markPrice": 20000, "unrealizedPnl": 50,
         "info": {"posSide": "Short", "execSeq": "another-sequence"}},
    ])

    positions = derivatives._pull_positions(SimpleNamespace(name="strategy", quote_asset=Asset("USDT", "crypto")))

    assert [(p.position_side, p.quantity) for p in positions] == [("long", 10), ("short", -4)]
    assert all(p.asset.symbol == symbol and p.asset.asset_type == "crypto_future" for p in positions)
    assert all(p.position_id is None for p in positions)  # Phemex execSeq is not a stable position ID.
    assert positions[0].pnl == 100
    assert positions[0].current_price == 20000


@pytest.mark.parametrize("symbol,base,quote", [
    ("BTC/USDT:USDT", "4", "80000"), ("BTC/USDC:USDC", "4", "80000"),
    ("BTC/USD:BTC", "0.0002", "4"),
])
def test_contract_exposure_uses_metadata_for_linear_and_inverse(derivatives, symbol, base, quote):
    exposure = derivatives.get_contract_exposure(Asset(symbol, "crypto_future"), 4, 20000)
    assert exposure["base"] == Decimal(base)
    assert exposure["quote"] == Decimal(quote)


def test_order_import_keeps_hedge_leg_and_settlement(derivatives):
    parsed = derivatives._parse_broker_order({
        "id": "contract-order", "symbol": "BTC/USDC:USDC", "type": "market", "side": "buy",
        "amount": 4, "status": "open", "info": {"posSide": "Short"}, "reduceOnly": True,
    }, "strategy")
    assert parsed.asset.symbol == "BTC/USDC:USDC"
    assert parsed.quote.symbol == "USDC"
    assert parsed.position_side == "short"
    assert parsed.reduce_only


def test_phemex_account_routes_follow_product_metadata_and_real_value_fields(derivatives):
    def response(params):
        currency = params["currency"]
        real = currency != "BTC"
        account = {"currency": currency,
                   "accountBalanceRv" if real else "accountBalanceEv": "100" if real else "100000000",
                   "totalUsedBalanceRv" if real else "totalUsedBalanceEv": "10" if real else "10000000"}
        return {"code": 0, "data": {"account": account, "positions": []}}
    derivatives.api.privateGetGAccountsAccountPositions = Mock(side_effect=response)
    derivatives.api.privateGetAccountsAccountPositions = Mock(side_effect=response)

    snapshot = derivatives._phemex_contract_accounts()

    assert derivatives.api.privateGetGAccountsAccountPositions.call_count == 2
    derivatives.api.privateGetAccountsAccountPositions.assert_called_once_with({"currency": "BTC"})
    assert snapshot["balances"]["USDC"]["total"] == Decimal("100")
    assert snapshot["balances"]["USDC"]["free"] == Decimal("90")
    assert snapshot["balances"]["BTC"]["total"] == Decimal("1")


@pytest.mark.parametrize("active,error_code,skip", [
    (False, 39108, True), (True, 39108, False), (None, 39108, False),
    (False, 10002, False),
])
def test_phemex_retired_unsupported_settlement_does_not_break_other_wallets(
        derivatives, active, error_code, skip):
    import ccxt

    # Shape and code come from Phemex's account endpoint. Retired market
    # metadata can still name a currency the account API no longer supports.
    obsolete = deepcopy(derivatives.api.markets["BTC/USD:BTC"])
    obsolete.update(symbol="ETH/USD:USD", id="ETHUSD", settle="USD", active=active)
    derivatives.api.markets[obsolete["symbol"]] = obsolete
    derivatives.api.privateGetGAccountsAccountPositions = Mock(return_value={
        "code": 0, "data": {"account": {"accountBalanceRv": "0", "totalUsedBalanceRv": "0"},
                             "positions": []}})
    valid = {"code": 0, "data": {"account": {"accountBalanceEv": 100000000,
                                              "totalUsedBalanceEv": 0}, "positions": []}}

    def legacy(params):
        if params["currency"] == "USD":
            raise ccxt.ExchangeError(
                'phemex {"code":%s,"msg":"Currency not supported.","data":null}' % error_code)
        return valid

    derivatives.api.privateGetAccountsAccountPositions = Mock(side_effect=legacy)
    if skip:
        result = derivatives._phemex_contract_accounts()
        assert set(result["balances"]) == {"BTC", "USDT", "USDC"}
        assert result["balances"]["BTC"]["total"] == Decimal("1")
    else:
        with pytest.raises(ccxt.ExchangeError):
            derivatives._phemex_contract_accounts()


def test_phemex_equity_adds_collateral_and_settlement_pnl_once(derivatives):
    derivatives.api.fetch_balance = Mock(return_value={"total": {"USDT": 100, "BTC": 0.01}})
    derivatives.api.fetch_ticker = Mock(return_value={"last": 20000})
    derivatives._phemex_contract_accounts = Mock(return_value={
        "balances": {"USDT": {"total": Decimal("1000"), "free": Decimal("900"), "used": Decimal("100")},
                     "BTC": {"total": Decimal("0.02"), "free": Decimal("0.01"), "used": Decimal("0.01")}},
        "positions": [{"symbol": "BTC/USDT:USDT", "unrealizedPnl": 20},
                      {"symbol": "BTC/USD:BTC", "unrealizedPnl": 0.001}],
    })

    cash, holdings, equity = derivatives._get_balances_at_broker(Asset("USDT", "crypto"), None)

    assert (cash, holdings, equity) == (1100, 640, 1740)


@pytest.mark.parametrize("symbol", ["BTC/USDT:USDT", "BTC/USDC:USDC"])
def test_phemex_position_mode_uses_same_documented_v2_family(derivatives, symbol):
    derivatives.api.privatePutGPositionsSwitchPosModeSync = Mock(return_value={"code": 0})
    derivatives.set_position_mode(True, Asset(symbol, "crypto_future"))
    derivatives.api.privatePutGPositionsSwitchPosModeSync.assert_called_once_with({
        "symbol": derivatives.get_market(symbol)["id"], "targetPosMode": "Hedged"})


@pytest.mark.parametrize("symbol", ["BTC/USDT:USDT", "BTC/USDC:USDC", "BTC/USD:BTC"])
def test_phemex_native_contract_order_serialization(derivatives, symbol):
    derivatives.api.create_order = type(derivatives.api).create_order.__get__(derivatives.api)
    derivatives.api.request = Mock(return_value={"code": 0, "data": {"orderID": "native-order"}})
    submitted = Order("strategy", derivatives.get_asset(symbol), 4, "buy", limit_price=20000,
                      position_side="short")
    submitted.reduce_only = True

    derivatives._submit_order(submitted)

    assert submitted.identifier == "native-order", submitted.error_message
    request = derivatives.api.request.call_args.args[3]
    market = derivatives.get_market(symbol)
    assert Decimal(str(request["orderQty" if market["inverse"] else "orderQtyRq"])) == 4
    assert request["posSide"] == "Short"
    assert request["reduceOnly"]


def test_cancel_selects_the_original_hedge_leg(derivatives):
    submitted = Order("strategy", derivatives.get_asset("BTC/USDC:USDC"), 4, "buy", position_side="short")
    submitted.pair = submitted.asset.symbol
    derivatives.api.cancel_order = Mock(return_value={"id": submitted.identifier, "status": "canceled"})

    derivatives.cancel_order(submitted)

    assert derivatives.api.cancel_order.call_args.kwargs["params"]["posSide"] == "Short"


def test_amend_uses_unified_api_and_keeps_quantity_and_leg(derivatives):
    submitted = Order("strategy", derivatives.get_asset("BTC/USDC:USDC"), 4, "buy",
                      limit_price=20000, position_side="short")
    submitted.status = "new"
    derivatives.api.edit_order = Mock(return_value={"id": submitted.identifier, "status": "open"})

    derivatives.modify_order(submitted, limit_price=20100)

    args, kwargs = derivatives.api.edit_order.call_args
    assert args[:4] == (submitted.identifier, "BTC/USDC:USDC", "limit", "buy")
    assert Decimal(str(args[4])) == 4
    assert Decimal(str(args[5])) == 20100
    assert kwargs["params"]["posSide"] == "Short"
    assert submitted.limit_price == 20100


def test_single_position_lookup_matches_contract_without_breaking_spot(derivatives):
    derivatives._pull_broker_positions = Mock(return_value=[
        {"currency": "BTC", "total": 1, "free": 1, "used": 0},
        {"symbol": "BTC/USDC:USDC", "contracts": 4, "side": "short"},
    ])

    assert derivatives._pull_broker_position(Asset("BTC", "crypto"))["total"] == 1
    assert derivatives._pull_broker_position(derivatives.get_asset("BTC/USDC:USDC"))["contracts"] == 4


def test_single_position_lookup_requires_leg_only_when_actually_ambiguous(derivatives):
    derivatives._pull_broker_positions = Mock(return_value=[
        {"symbol": "BTC/USDC:USDC", "contracts": 4, "side": "long", "hedged": True},
        {"symbol": "BTC/USDC:USDC", "contracts": 2, "side": "short", "hedged": True},
    ])
    asset = derivatives.get_asset("BTC/USDC:USDC")

    with pytest.raises(ValueError, match="position_side"):
        derivatives._pull_broker_position(asset)
    assert derivatives._pull_broker_position(asset, position_side="short")["contracts"] == 2


@pytest.mark.parametrize("symbol,raw_fee,cost,currency", [
    ("BTC/USDT:USDT", {"cumFeeRv": "0.02"}, 0.02, "USDT"),
    ("BTC/USDC:USDC", {"cumFeeRv": "-0.01"}, -0.01, "USDC"),
    ("BTC/USD:BTC", {"cumFeeEv": "100"}, 0.000001, "BTC"),
])
def test_phemex_cumulative_native_fee_keeps_settlement_and_rebates(derivatives, symbol, raw_fee, cost, currency):
    order = Order("test-strategy", derivatives.get_asset(symbol), 4, "buy")
    order.pair = symbol
    derivatives._apply_ccxt_order_observation(order, {"status": "open", "filled": 0, "info": raw_fee})

    assert order.fees == [{"currency": currency, "cost": cost}]



@pytest.mark.parametrize(
    "info, expected",
    [
        ({"positionIdx": "1"}, "long"),  # hedge-mode buy leg (Bybit style)
        ({"positionIdx": 2}, "short"),  # hedge-mode sell leg
        ({"positionIdx": "0"}, None),  # one-way mode nets: no leg
        ({"holdSide": "short"}, "short"),  # two-way position side (Bitget style)
        ({"holdSide": "buy"}, None),  # one-way value is a direction, not a leg
        ({"positionSide": "BOTH"}, None),
    ],
)
def test_order_import_reads_exchange_hedge_leg_fields(derivatives, info, expected):
    parsed = derivatives._parse_broker_order({
        "id": "contract-order", "symbol": "BTC/USDC:USDC", "type": "market", "side": "sell",
        "amount": 2, "status": "open", "info": info,
    }, "strategy")
    assert parsed.position_side == expected


def test_hedged_order_side_is_never_stored_as_a_position_leg(derivatives):
    # Orders carry buy/sell, not long/short; a hedged flag must not turn "sell" into a leg name.
    parsed = derivatives._parse_broker_order({
        "id": "contract-order", "symbol": "BTC/USDC:USDC", "type": "market", "side": "sell",
        "amount": 2, "status": "open", "hedged": True, "info": {},
    }, "strategy")
    assert parsed.position_side in (None, "long", "short")
    assert parsed.position_side != "sell"


def test_position_snapshot_reads_position_index_legs(derivatives):
    # Unified rows without a hedged flag: the native position index alone names each leg,
    # so neither leg is dropped as a duplicate of the other.
    derivatives.api.fetch_balance = Mock(return_value={"total": {}})
    derivatives._fetch_derivative_positions = Mock(return_value=[
        {"symbol": "BTC/USDC:USDC", "contracts": 3, "side": "long", "contractSize": 1, "info": {"positionIdx": "1"}},
        {"symbol": "BTC/USDC:USDC", "contracts": 1, "side": "short", "contractSize": 1, "info": {"positionIdx": "2"}},
    ])
    positions = derivatives._pull_positions(SimpleNamespace(name="strategy", quote_asset=Asset("USDT", "crypto")))
    assert sorted((p.position_side, p.quantity) for p in positions) == [("long", 3), ("short", -1)]
