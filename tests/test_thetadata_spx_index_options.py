from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from lumibot.backtesting.backtesting_broker import BacktestingBroker
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def test_build_historical_chain_merges_spx_and_spxw_expirations(monkeypatch):
    """SPX 0DTE strategies require SPXW expirations to be present in the chain."""

    strike_requests: list[tuple[str, str]] = []

    def fake_get_request(url: str, headers: dict, querystring: dict, username=None, password=None):
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["expirations"]):
            symbol = querystring["symbol"]
            if symbol == "SPX":
                return {
                    "header": {"format": ["expiration"]},
                    "response": [["2025-02-21"]],
                }
            if symbol == "SPXW":
                return {
                    "header": {"format": ["expiration"]},
                    "response": [["2025-02-14"], ["2025-02-21"]],
                }
            raise AssertionError(f"Unexpected expirations symbol={symbol}")

        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["strikes"]):
            strike_requests.append((querystring["symbol"], querystring["expiration"]))
            return {
                "header": {"format": ["strike"]},
                "response": [[100], [105], [110]],
            }

        raise AssertionError(f"Unexpected url={url}")

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)

    chain = thetadata_helper.build_historical_chain(
        asset=Asset("SPX", asset_type="index"),
        as_of_date=date(2025, 2, 14),
        max_expirations=2,
    )

    assert chain is not None
    assert list(chain["Chains"]["CALL"].keys()) == ["2025-02-14", "2025-02-21"]
    assert strike_requests == [("SPXW", "2025-02-14"), ("SPX", "2025-02-21")]


def test_cash_settle_index_option_retries_underlying_as_index(monkeypatch):
    """Index options can arrive without an underlying_asset and must not crash at settlement."""

    option = Asset(
        symbol="SPX",
        asset_type="option",
        expiration=date(2025, 2, 14),
        strike=6000.0,
        right="CALL",
    )
    option.underlying_asset = None
    option.multiplier = getattr(option, "multiplier", 100) or 100

    position = SimpleNamespace(asset=option, quantity=1)

    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker.IS_BACKTESTING_BROKER = True
    broker.CASH_SETTLED = "CASH_SETTLED"

    get_last_price_calls: list[str] = []

    def fake_get_last_price(asset):
        get_last_price_calls.append(asset.asset_type)
        if asset.asset_type == "stock":
            raise ValueError("[THETA][COVERAGE][TAIL_PLACEHOLDER] asset=SPX/USD (minute) ends with placeholders")
        return 6100.0

    broker.get_last_price = fake_get_last_price
    broker.stream = SimpleNamespace(dispatch=lambda *args, **kwargs: None)

    strategy = SimpleNamespace(
        get_cash=lambda: 0,
        _set_cash_position=lambda value: None,
        create_order=lambda *args, **kwargs: SimpleNamespace(child_orders=[]),
    )

    broker.cash_settle_options_contract(position, strategy)
    assert get_last_price_calls == ["stock", "index"]


def test_cash_settle_index_option_retries_when_stock_price_is_none(monkeypatch):
    option = Asset(
        symbol="SPX",
        asset_type="option",
        expiration=date(2025, 2, 14),
        strike=6000.0,
        right="CALL",
    )
    option.underlying_asset = None
    option.multiplier = getattr(option, "multiplier", 100) or 100

    position = SimpleNamespace(asset=option, quantity=1)

    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker.IS_BACKTESTING_BROKER = True
    broker.CASH_SETTLED = "CASH_SETTLED"

    get_last_price_calls: list[str] = []

    def fake_get_last_price(asset):
        get_last_price_calls.append(asset.asset_type)
        if asset.asset_type == "stock":
            return None
        return 6100.0

    broker.get_last_price = fake_get_last_price
    broker.stream = SimpleNamespace(dispatch=lambda *args, **kwargs: None)

    strategy = SimpleNamespace(
        get_cash=lambda: 0,
        _set_cash_position=lambda value: None,
        create_order=lambda *args, **kwargs: SimpleNamespace(child_orders=[]),
    )

    broker.cash_settle_options_contract(position, strategy)
    assert get_last_price_calls == ["stock", "index"]
