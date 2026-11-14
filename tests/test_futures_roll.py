import datetime

import pytz
import pytest

from lumibot.entities import Asset
from lumibot.tools import futures_roll

NY = pytz.timezone("America/New_York")


def _dt(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime.datetime:
    return NY.localize(datetime.datetime(year, month, day, hour, minute))


def test_equity_index_roll_eight_business_days_before_expiry():
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    year, month = futures_roll.determine_contract_year_month(asset.symbol, _dt(2025, 9, 8))
    assert (year, month) == (2025, 9)

    year, month = futures_roll.determine_contract_year_month(asset.symbol, _dt(2025, 9, 10))
    assert (year, month) == (2025, 12)


def test_fallback_mid_month_preserved_for_unknown_symbols():
    asset = Asset("XYZ", asset_type=Asset.AssetType.CONT_FUTURE)

    year, month = futures_roll.determine_contract_year_month(asset.symbol, _dt(2025, 3, 16))
    assert (year, month) == (2025, 6)


def test_resolve_symbols_for_range_produces_sequential_contracts():
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
    start = _dt(2025, 8, 1)
    end = _dt(2025, 12, 31)

    symbols = futures_roll.resolve_symbols_for_range(asset, start, end, year_digits=1)
    assert symbols == ["MESU5", "MESZ5", "MESH6"], symbols


def test_comex_gold_rolls_before_first_notice():
    asset_symbol = "GC"

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 1, 22, 0, 4))
    assert (year, month) == (2025, 2)

    # The former last-trade expectation kept gold into the delivery month.
    # Seven exchange business days before Jan 31 first notice is Jan 22.
    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 1, 22, 0, 6))
    assert (year, month) == (2025, 4)


def test_comex_gold_symbol_sequence_uses_even_month_cycle():
    asset = Asset("GC", asset_type=Asset.AssetType.CONT_FUTURE)
    start = _dt(2025, 1, 1)
    end = _dt(2025, 8, 1)

    symbols = futures_roll.resolve_symbols_for_range(asset, start, end, year_digits=1)
    # By August 1 the August delivery contract has already rolled before notice.
    assert symbols == ["GCG5", "GCJ5", "GCM5", "GCQ5", "GCV5"], symbols


def test_comex_micro_gold_rolls_before_first_notice():
    asset_symbol = "MGC"

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 1, 22, 0, 4))
    assert (year, month) == (2025, 2)

    # Match the corrected pre-notice gold convention; the former date was in delivery.
    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 1, 22, 0, 6))
    assert (year, month) == (2025, 4)


def test_comex_micro_gold_symbol_sequence_uses_even_month_cycle():
    asset = Asset("MGC", asset_type=Asset.AssetType.CONT_FUTURE)
    start = _dt(2025, 1, 1)
    end = _dt(2025, 8, 1)

    symbols = futures_roll.resolve_symbols_for_range(asset, start, end, year_digits=1)
    # By August 1 the August delivery contract has already rolled before notice.
    assert symbols == ["MGCG5", "MGCJ5", "MGCM5", "MGCQ5", "MGCV5"], symbols


def test_nymex_crude_oil_rolls_before_last_trade_date():
    asset_symbol = "CL"

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 2, 10))
    assert (year, month) == (2025, 3)

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 2, 13, 0, 6))
    assert (year, month) == (2025, 4)


def test_nymex_micro_crude_oil_rolls_before_last_trade_date():
    asset_symbol = "MCL"

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 2, 10))
    assert (year, month) == (2025, 3)

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 2, 12, 0, 6))
    assert (year, month) == (2025, 4)


def test_natural_gas_uses_monthly_delivery_contracts():
    assert futures_roll.determine_contract_year_month("NG", _dt(2026, 10, 8)) == (2026, 11)
    asset = Asset("NG", asset_type=Asset.AssetType.CONT_FUTURE)
    assert futures_roll.resolve_symbols_for_range(asset, _dt(2026, 10, 1), _dt(2026, 12, 31), year_digits=2) == [
        "NGX26", "NGZ26", "NGF27", "NGG27",
    ]


def test_natural_gas_rolls_before_prior_month_last_trade_date():
    # November delivery expires October 28; the five-business-day roll
    # convention used for energy futures puts the transition on October 21.
    assert futures_roll.determine_contract_year_month("NG", _dt(2026, 10, 21, 0, 4)) == (2026, 11)
    assert futures_roll.determine_contract_year_month("NG", _dt(2026, 10, 21, 0, 6)) == (2026, 12)


def test_cme_crypto_futures_roll_uses_last_friday_anchor():
    # MBT (Micro Bitcoin) expiries are last-Friday-trading-day; roll occurs 8 business days before.
    # April 2024 last Friday is 2024-04-26 -> roll trigger 2024-04-16 (plus a +5 minute shift).
    asset_symbol = "MBT"

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2024, 4, 15))
    assert (year, month) == (2024, 4)

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2024, 4, 16, 0, 6))
    assert (year, month) == (2024, 5)


@pytest.mark.parametrize("symbol", ["GC", "MGC"])
def test_gold_continuous_history_rolls_before_delivery_notice(symbol):
    # COMEX 706.C: first notice is the last business day before the delivery
    # month. October's contract had no Oct 2 prints in the actual IBKR incident;
    # December did. Keep the seven-day convention but anchor it before notice,
    # rather than keeping the delivery-month contract until its last trade.
    assert futures_roll.determine_contract_year_month(symbol, _dt(2026, 9, 21, 0, 4)) == (2026, 10)
    assert futures_roll.determine_contract_year_month(symbol, _dt(2026, 9, 21, 0, 6)) == (2026, 12)
    assert futures_roll.determine_contract_year_month(symbol, _dt(2026, 10, 2, 9, 30)) == (2026, 12)


@pytest.mark.parametrize("symbol", ["GC", "MGC"])
def test_gold_notice_roll_excludes_exchange_business_holidays(symbol):
    # May 30 is first notice for June 2025. Memorial Day (May 26) is not
    # an exchange business day, so seven prior business days ends May 20.
    assert futures_roll.determine_contract_year_month(symbol, _dt(2025, 5, 20, 0, 4)) == (2025, 6)
    assert futures_roll.determine_contract_year_month(symbol, _dt(2025, 5, 20, 0, 6)) == (2025, 8)


@pytest.mark.parametrize("symbol", ["GC", "MGC"])
def test_gold_roll_keeps_actual_contract_expiration(symbol):
    from lumibot.tools.ibkr_helper import _contract_expiration_date

    assert _contract_expiration_date(symbol, year=2026, month=10) == datetime.date(2026, 10, 28)


@pytest.mark.parametrize("symbol,expected", [("GC", (2026, 12)), ("MGC", (2026, 12)), ("NG", (2026, 11))])
def test_repeated_roll_resolution_reuses_calendar_without_changing_contract(monkeypatch, symbol, expected):
    import pandas_market_calendars as mcal

    for function in (futures_roll._calculate_roll_trigger, futures_roll._ng_last_trade_date):
        if hasattr(function, "cache_clear"):
            function.cache_clear()
    original = mcal.get_calendar
    calls = []

    def counted_calendar(name, *args, **kwargs):
        calls.append(name)
        return original(name, *args, **kwargs)

    monkeypatch.setattr(mcal, "get_calendar", counted_calendar)
    try:
        assert futures_roll.determine_contract_year_month(symbol, _dt(2026, 10, 8)) == expected
        first_count = len(calls)
        assert first_count > 0
        for minute in range(1, 5):
            assert futures_roll.determine_contract_year_month(symbol, _dt(2026, 10, 8, 9, minute)) == expected
        assert len(calls) == first_count, "Each strategy iteration rebuilt the same delivery-month calendar"
    finally:
        for function in (futures_roll._calculate_roll_trigger, futures_roll._ng_last_trade_date):
            if hasattr(function, "cache_clear"):
                function.cache_clear()


def test_roll_cache_keys_include_rule_and_delivery_month():
    original = futures_roll.ROLL_RULES["GC"]
    changed = futures_roll.RollRule(6, original.anchor, original.contract_months, original.roll_anchor)
    assert futures_roll._calculate_roll_trigger(2026, 10, original) == _dt(2026, 9, 21, 0, 5)
    assert futures_roll._calculate_roll_trigger(2026, 10, changed) == _dt(2026, 9, 22, 0, 5)
    assert futures_roll._calculate_roll_trigger(2026, 12, original) == _dt(2026, 11, 18, 0, 5)


def test_calendar_failure_is_not_cached(monkeypatch):
    import pandas_market_calendars as mcal

    futures_roll._ng_last_trade_date.cache_clear()
    original = mcal.get_calendar

    def unavailable(*args, **kwargs):
        raise ValueError("calendar unavailable")

    try:
        monkeypatch.setattr(mcal, "get_calendar", unavailable)
        with pytest.raises(ValueError, match="calendar unavailable"):
            futures_roll._ng_last_trade_date(2026, 11)
        monkeypatch.setattr(mcal, "get_calendar", original)
        assert futures_roll._ng_last_trade_date(2026, 11) == _dt(2026, 10, 28)
    finally:
        futures_roll._ng_last_trade_date.cache_clear()


# ---------------------------------------------------------------------------
# Every commonly traded product has its own roll rule (active months and timing
# follow the exchange contract specifications cited in futures_roll.ROLL_RULES).
# ---------------------------------------------------------------------------
_COMMON_FUTURES = (
    "ES MES NQ MNQ RTY M2K YM MYM GC MGC SI SIL HG MHG PL CL MCL NG MNG RB HO "
    "ZN ZF ZT ZB UB 6E 6J 6B 6A 6C M6E ZC ZS ZW ZL ZM LE HE BTC MBT ETH MET VX"
).split()

_H, _K, _N, _U, _Z = 3, 5, 7, 9, 12
_ACTIVE_CONTRACT_CASES = [
    # Equity index: quarterly, roll 8 business days before the third Friday.
    ("RTY", (2026, 3, 9), (2026, 3)), ("RTY", (2026, 3, 11), (2026, 6)),
    ("M2K", (2026, 7, 15), (2026, 9)), ("M2K", (2026, 12, 15), (2027, 3)),
    # Silver and copper: Mar, May, Jul, Sep, Dec; out before first notice.
    ("SI", (2026, 1, 15), (2026, _H)), ("SI", (2026, 2, 25), (2026, _K)),
    ("SIL", (2026, 1, 15), (2026, _H)), ("SIL", (2026, 4, 25), (2026, _N)),
    ("SIL", (2026, 6, 26), (2026, _U)), ("SIL", (2026, 8, 27), (2026, _Z)),
    ("SIL", (2026, 11, 25), (2027, _H)),
    ("HG", (2026, 1, 15), (2026, _H)), ("HG", (2026, 3, 15), (2026, _K)),
    ("HG", (2026, 6, 26), (2026, _U)), ("HG", (2026, 10, 15), (2026, _Z)),
    ("MHG", (2026, 1, 15), (2026, _H)), ("MHG", (2026, 2, 20), (2026, _K)),
    # Platinum: Jan, Apr, Jul, Oct.
    ("PL", (2026, 2, 10), (2026, 4)), ("PL", (2026, 3, 25), (2026, 7)),
    ("PL", (2026, 8, 15), (2026, 10)), ("PL", (2026, 9, 25), (2027, 1)),
    # Refined products and micro gas: every month, expiring before the contract month.
    ("RB", (2026, 2, 10), (2026, 3)), ("RB", (2026, 2, 23), (2026, 4)),
    ("HO", (2026, 6, 15), (2026, 7)), ("HO", (2026, 12, 28), (2027, 2)),
    ("MNG", (2026, 2, 10), (2026, 3)), ("MNG", (2026, 2, 20), (2026, 4)),
    # Treasuries: quarterly, out before delivery notices.
    ("ZN", (2026, 2, 10), (2026, 3)), ("ZN", (2026, 2, 24), (2026, 6)),
    ("ZF", (2026, 5, 27), (2026, 9)), ("ZT", (2026, 8, 27), (2026, 12)),
    ("ZB", (2026, 4, 15), (2026, 6)), ("UB", (2026, 11, 27), (2027, 3)),
    # FX: quarterly, roll the week before the Monday expiry.
    ("6E", (2026, 3, 6), (2026, 3)), ("6E", (2026, 3, 10), (2026, 6)),
    ("6J", (2026, 6, 9), (2026, 9)), ("6B", (2026, 9, 4), (2026, 9)),
    ("6A", (2026, 9, 8), (2026, 12)), ("6C", (2026, 12, 8), (2027, 3)),
    ("M6E", (2026, 3, 10), (2026, 6)),
    # Grains and oilseeds.
    ("ZC", (2026, 2, 10), (2026, _H)), ("ZC", (2026, 2, 24), (2026, _K)),
    ("ZW", (2026, 6, 25), (2026, _U)), ("ZW", (2026, 10, 15), (2026, _Z)),
    ("ZS", (2026, 7, 15), (2026, 8)), ("ZS", (2026, 8, 26), (2026, 11)),
    ("ZS", (2026, 10, 27), (2027, 1)),
    ("ZL", (2026, 9, 10), (2026, 10)), ("ZM", (2026, 9, 25), (2026, 12)),
    # Livestock.
    ("LE", (2026, 1, 10), (2026, 2)), ("LE", (2026, 1, 27), (2026, 4)),
    ("HE", (2026, 2, 5), (2026, 2)), ("HE", (2026, 2, 9), (2026, 4)),
    ("HE", (2026, 5, 8), (2026, 6)), ("HE", (2026, 12, 20), (2027, 2)),
    # Crypto (Globex roots) and VIX: every month.
    ("BTC", (2026, 3, 16), (2026, 3)), ("ETH", (2026, 3, 18), (2026, 4)),
    ("VX", (2026, 3, 10), (2026, 3)), ("VX", (2026, 3, 12), (2026, 4)),
    ("VX", (2026, 12, 10), (2027, 1)),
]


@pytest.mark.parametrize("symbol,day,expected", _ACTIVE_CONTRACT_CASES)
def test_common_futures_pick_the_active_contract_month(symbol, day, expected):
    assert futures_roll.determine_contract_year_month(symbol, _dt(*day, 12)) == expected


@pytest.mark.parametrize("symbol,contract,expected", [
    ("ES", (2026, 3), (2026, 3, 20)), ("RTY", (2026, 3), (2026, 3, 20)),
    ("6E", (2026, 3), (2026, 3, 16)), ("6C", (2026, 3), (2026, 3, 17)),
    ("ZN", (2026, 3), (2026, 3, 20)), ("ZF", (2026, 3), (2026, 3, 31)),
    ("ZC", (2026, 3), (2026, 3, 13)), ("HE", (2026, 2), (2026, 2, 13)),
    ("RB", (2026, 3), (2026, 2, 27)), ("HG", (2026, 3), (2026, 3, 27)),
    ("MHG", (2026, 3), (2026, 2, 25)), ("VX", (2026, 3), (2026, 3, 18)),
])
def test_common_futures_last_trade_dates(symbol, contract, expected):
    assert futures_roll.contract_last_trade_date(symbol, *contract).date() == datetime.date(*expected)


@pytest.mark.parametrize("symbol", _COMMON_FUTURES)
def test_every_common_future_has_a_rule_and_never_holds_an_expired_contract(symbol):
    rule = futures_roll.ROLL_RULES.get(symbol)
    assert rule is not None, f"{symbol} has no roll rule"
    day = datetime.date(2026, 1, 1)
    previous = None
    while day.year == 2026:
        chosen = futures_roll.determine_contract_year_month(symbol, _dt(day.year, day.month, day.day, 12))
        assert chosen[1] in rule.contract_months
        assert futures_roll.contract_last_trade_date(symbol, *chosen).date() > day
        if previous is not None:
            assert chosen >= previous
            if chosen != previous:
                months = sorted(rule.contract_months)
                step = (months.index(chosen[1]) - months.index(previous[1])) % len(months)
                assert step == 1, f"{symbol} skipped a contract on {day}"
        previous = chosen
        day += datetime.timedelta(days=1)


def test_unknown_symbol_says_once_that_the_default_rule_is_used(monkeypatch):
    messages = []
    monkeypatch.setattr(futures_roll, "_DEFAULT_RULE_NOTED", set(), raising=False)
    monkeypatch.setattr(futures_roll.logger, "warning", lambda *args, **kwargs: messages.append(args), raising=False)
    for _ in range(3):
        assert futures_roll.determine_contract_year_month("QQZ", _dt(2026, 3, 16)) == (2026, 6)
    assert len(messages) == 1 and "QQZ" in messages[0]
