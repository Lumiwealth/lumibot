"""
Unit tests for DataBento futures symbology helpers with rolling futures assets.

Notes:
- Auto-expiry futures (`Asset(asset_type='future', auto_expiry=...)`) do not materialize a single
  `expiration`; DataBento formatting should resolve them as rolling contracts.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime

from lumibot.entities.asset import Asset
from lumibot.tools.databento_helper import _format_futures_symbol_for_databento


def test_databento_formats_explicit_future_contract():
    asset = Asset("ES", asset_type=Asset.AssetType.FUTURE, expiration=date(2024, 6, 21))
    assert _format_futures_symbol_for_databento(asset) == "ESM24"


def test_databento_resolves_cont_future_to_specific_contract():
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
    ref = datetime(2026, 2, 1, tzinfo=UTC)
    symbol = _format_futures_symbol_for_databento(asset, reference_date=ref)
    assert re.match(r"^MES[FGHJKMNQUVXZ]\d{1,2}$", symbol)


def test_databento_resolves_auto_expiry_future_to_specific_contract():
    asset = Asset("MES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
    assert asset.expiration is None
    ref = datetime(2026, 2, 1, tzinfo=UTC)
    symbol = _format_futures_symbol_for_databento(asset, reference_date=ref)
    assert re.match(r"^MES[FGHJKMNQUVXZ]\d{1,2}$", symbol)
