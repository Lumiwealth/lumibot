import datetime

import pytz

from lumibot.entities import Asset
from lumibot.tools import futures_roll

NY = pytz.timezone("America/New_York")


def _dt(year: int, month: int, day: int) -> datetime.datetime:
    return NY.localize(datetime.datetime(year, month, day))


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

    # Seven business days before the third last business day of February 2025 is Feb 17
    year, month = futures_roll.determine_contract_year_month(asset_symbol, _dt(2025, 2, 17))
    assert (year, month) == (2025, 4)


def test_comex_gold_symbol_sequence_uses_even_month_cycle():
    asset = Asset("GC", asset_type=Asset.AssetType.CONT_FUTURE)
    start = _dt(2025, 1, 1)
    end = _dt(2025, 8, 1)

    symbols = futures_roll.resolve_symbols_for_range(asset, start, end, year_digits=1)
    assert symbols == ["GCG5", "GCJ5", "GCM5", "GCQ5"], symbols
