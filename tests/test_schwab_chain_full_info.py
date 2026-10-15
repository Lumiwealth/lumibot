"""The public full-chain API must use Schwab's chain snapshot, not N quotes."""
from copy import deepcopy
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from lumibot.data_sources.schwab_data import SchwabData
from lumibot.entities import Asset


EXPIRY = date(2026, 10, 16)


def contract(strike, right="CALL", **changes):
    row = {
        "symbol": f"ABC   261016{right[0]}{int(strike * 1000):08d}",
        "strikePrice": strike, "putCall": right, "bid": 1.1, "ask": 1.3,
        "last": 1.15, "bidSize": 3, "askSize": 4, "totalVolume": 22,
        "openInterest": 60, "delta": .6, "gamma": .02, "theta": -.03,
        "vega": .1, "rho": .01, "volatility": 25.0,
        "quoteTimeInLong": 1792070400000,
    }
    row.update(changes)
    return row


def source(payload=None, status=200):
    payload = deepcopy(payload) if payload is not None else {
        "callExpDateMap": {"2026-10-16:1": {
            "20.0": [contract(20)], "25.0": [contract(25)],
        }, "2026-10-23:8": {"20.0": [contract(20)]}},
        "putExpDateMap": {"2026-10-16:1": {"20.0": [contract(20, "PUT")]}},
    }
    calls = []
    def get_option_chain(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status_code=status, json=lambda: deepcopy(payload))
    data = SchwabData(auto_create_client=False)
    data.client = SimpleNamespace(
        get_option_chain=get_option_chain,
        Options=SimpleNamespace(ContractType=SimpleNamespace(ALL="ALL"),
                                Strategy=SimpleNamespace(SINGLE="SINGLE")),
    )
    # Inherited per-contract fallback must fail this contract test.
    data.get_chains = lambda *a, **k: (_ for _ in ()).throw(AssertionError("extra chains request"))
    data.get_quote = lambda *a, **k: (_ for _ in ()).throw(AssertionError("individual quote request"))
    return data, calls


def test_one_request_preserves_both_rights_prices_and_market_timestamps():
    data, calls = source()
    frame = data.get_chain_full_info(Asset("ABC"), EXPIRY)
    assert len(calls) == 1
    assert calls[0]["from_date"] == calls[0]["to_date"] == EXPIRY
    assert calls[0]["contract_type"] == "ALL"
    assert len(frame) == 3
    assert set(frame.option_type) == {"CALL", "PUT"}
    row = frame[(frame.option_type == "CALL") & (frame.strike == 20)].iloc[0]
    assert row.bid == 1.1 and row.ask == 1.3 and row["last"] == 1.15
    assert row["greeks.delta"] == .6 and row.open_interest == 60
    assert row.quote_time.timestamp() == 1792070400
    assert row.expiration_date == EXPIRY


@pytest.mark.parametrize("expiry", [EXPIRY, datetime(2026, 10, 16, 12), "2026-10-16"])
def test_inclusive_strike_bounds_and_exact_expiry(expiry):
    data, calls = source()
    frame = data.get_chain_full_info(Asset("ABC"), expiry, strike_min=20, strike_max=20)
    assert len(frame) == 2 and set(frame.strike) == {20}
    assert len(calls) == 1


def test_missing_bid_ask_remain_missing_not_invented_zero_or_midpoint():
    payload = {"callExpDateMap": {"2026-10-16:1": {"20": [contract(20, bid=None, ask=None)]}}}
    data, _ = source(payload)
    row = data.get_chain_full_info(Asset("ABC"), EXPIRY).iloc[0]
    assert row.bid is None and row.ask is None
    assert row["last"] == 1.15


def test_missing_timestamp_is_not_replaced_with_current_time():
    payload = {"callExpDateMap": {"2026-10-16:1": {"20": [contract(20, quoteTimeInLong=None)]}}}
    data, _ = source(payload)
    assert data.get_chain_full_info(Asset("ABC"), EXPIRY).iloc[0].quote_time is None


@pytest.mark.parametrize("status", [401, 429, 500])
def test_failed_provider_response_never_turns_into_fabricated_market_data(status):
    data, calls = source(status=status)
    with pytest.raises(ConnectionError):
        data.get_chain_full_info(Asset("ABC"), EXPIRY)
    assert len(calls) == 1


def test_empty_chain_is_empty_and_supplied_chain_metadata_does_not_trigger_reads():
    data, calls = source({})
    assert data.get_chain_full_info(Asset("ABC"), EXPIRY, chains={"Chains": {}}).empty
    assert len(calls) == 1


def test_bad_expiry_fails_before_network():
    data, calls = source()
    with pytest.raises(TypeError):
        data.get_chain_full_info(Asset("ABC"), 20261016)
    assert not calls


def test_documented_strategy_api_reaches_the_bulk_adapter(monkeypatch):
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "true")
    monkeypatch.setenv("LUMIBOT_LAZY_CREDENTIALS", "true")
    from lumibot.brokers import Broker
    from lumibot.strategies.strategy import Strategy
    data, calls = source()
    broker = SimpleNamespace(data_source=data)
    broker.get_chain_full_info = lambda *a, **k: Broker.get_chain_full_info(broker, *a, **k)
    strategy = SimpleNamespace(
        _sanitize_user_asset=lambda value: value, risk_free_rate=.04, broker=broker,
    )
    frame = Strategy.get_chain_full_info(strategy, Asset("ABC"), "2026-10-16", underlying_price=25)
    assert len(frame) == 3 and len(calls) == 1


@pytest.mark.parametrize("strike_count", [1, 21, 100])
def test_request_count_does_not_grow_with_contract_count(strike_count):
    payload = {"callExpDateMap": {"2026-10-16:1": {
        str(strike): [contract(strike)] for strike in range(1, strike_count + 1)
    }}}
    data, calls = source(payload)
    assert len(data.get_chain_full_info(Asset("ABC"), EXPIRY)) == strike_count
    assert len(calls) == 1


def test_repeated_reads_refresh_snapshot_instead_of_reusing_stale_prices():
    data, calls = source()
    data.get_chain_full_info(Asset("ABC"), EXPIRY)
    data.get_chain_full_info(Asset("ABC"), EXPIRY)
    assert len(calls) == 2


def test_bad_bounds_fail_before_request():
    data, calls = source()
    with pytest.raises(ValueError):
        data.get_chain_full_info(Asset("ABC"), EXPIRY, strike_min=25, strike_max=20)
    assert not calls


def test_delayed_and_adjusted_contract_metadata_is_not_lost():
    payload = {"isDelayed": True, "callExpDateMap": {"2026-10-16:1": {"20": [
        contract(20, nonStandard=True, multiplier=10,
                 optionDeliverablesList=[{"symbol": "ABC", "deliverableUnits": 10}])
    ]}}}
    data, _ = source(payload)
    row = data.get_chain_full_info(Asset("ABC"), EXPIRY).iloc[0]
    assert bool(row.is_delayed) and bool(row.non_standard)
    assert row.multiplier == 10
    assert row.deliverables == [{"symbol": "ABC", "deliverableUnits": 10}]
