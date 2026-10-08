import datetime

import pytz

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


def test_comex_gold_rolls_on_third_last_business_day_offset():
    asset_symbol = "GC"

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 2, 14))
    assert (year, month) == (2025, 2)

    # Seven business days before the third last business day of February 2025 is Feb 17.
    # Roll triggers are shifted slightly (see futures_roll._calculate_roll_trigger) to avoid
    # edge-case midnight boundary issues.
    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 2, 17, 0, 6))
    assert (year, month) == (2025, 4)


def test_comex_gold_symbol_sequence_uses_even_month_cycle():
    asset = Asset("GC", asset_type=Asset.AssetType.CONT_FUTURE)
    start = _dt(2025, 1, 1)
    end = _dt(2025, 8, 1)

    symbols = futures_roll.resolve_symbols_for_range(asset, start, end, year_digits=1)
    assert symbols == ["GCG5", "GCJ5", "GCM5", "GCQ5"], symbols


def test_comex_micro_gold_rolls_on_third_last_business_day_offset():
    asset_symbol = "MGC"

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 2, 14))
    assert (year, month) == (2025, 2)

    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 2, 17, 0, 6))
    assert (year, month) == (2025, 4)


def test_comex_micro_gold_symbol_sequence_uses_even_month_cycle():
    asset = Asset("MGC", asset_type=Asset.AssetType.CONT_FUTURE)
    start = _dt(2025, 1, 1)
    end = _dt(2025, 8, 1)

    symbols = futures_roll.resolve_symbols_for_range(asset, start, end, year_digits=1)
    assert symbols == ["MGCG5", "MGCJ5", "MGCM5", "MGCQ5"], symbols


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
