from datetime import date

from lumibot.entities import Asset
from lumibot.tools.thetadata_helper import (
    _THETADATA_EXPIRY_MAP,
    _THETADATA_EXPIRY_MAP_LOCK,
    _placeholder_option_cache_needs_provider_expiry_refresh,
    _register_thetadata_expiry_map_from_chain,
    _thetadata_option_query_expiration,
)


def test_thetadata_monthly_expiration_maps_to_occ_saturday() -> None:
    """ThetaData represents standard monthlies using the OCC Saturday expiry date.

    LumiBot strategies typically use the last tradable Friday as the "expiration" date. Without
    mapping, ThetaData history endpoints can return placeholder-only responses (472/empty) even
    when the contract exists.
    """

    # Third Friday of Aug 2013 -> OCC Saturday
    assert _thetadata_option_query_expiration(date(2013, 8, 16)) == date(2013, 8, 17)

    # Third Friday of Feb 2014 -> OCC Saturday
    assert _thetadata_option_query_expiration(date(2014, 2, 21)) == date(2014, 2, 22)


def test_thetadata_weekly_expiration_kept_on_friday() -> None:
    # Weekly expiration (not the 3rd Friday) should stay on Friday.
    assert _thetadata_option_query_expiration(date(2013, 8, 9)) == date(2013, 8, 9)


def test_thetadata_holiday_thursday_expiration_kept() -> None:
    # Some expirations are represented as Thursday due to market holidays (e.g., Good Friday).
    assert _thetadata_option_query_expiration(date(2015, 4, 2)) == date(2015, 4, 2)


def test_thetadata_expiration_mapping_prefers_chain_provider_expiry() -> None:
    """Chain-derived mapping overrides the heuristic when the provider uses Fridays (e.g. CVNA)."""
    with _THETADATA_EXPIRY_MAP_LOCK:
        _THETADATA_EXPIRY_MAP.clear()

    # Provider uses Friday for this underlying (so heuristic Saturday mapping would be wrong).
    cvna_chain = {
        "UnderlyingSymbol": "CVNA",
        "Chains": {"CALL": {"2018-02-16": [20.0]}, "PUT": {}},
    }
    _register_thetadata_expiry_map_from_chain("CVNA", cvna_chain)
    assert _thetadata_option_query_expiration(date(2018, 2, 16), symbol="CVNA") == date(2018, 2, 16)

    with _THETADATA_EXPIRY_MAP_LOCK:
        _THETADATA_EXPIRY_MAP.clear()


def test_thetadata_expiration_mapping_supports_occ_saturday_providers() -> None:
    """Chain-derived mapping supports providers that list monthlies as OCC Saturday (e.g. MELI)."""
    with _THETADATA_EXPIRY_MAP_LOCK:
        _THETADATA_EXPIRY_MAP.clear()

    meli_chain = {
        "UnderlyingSymbol": "MELI",
        "Chains": {"CALL": {"2015-01-17": [100.0]}, "PUT": {}},
    }
    _register_thetadata_expiry_map_from_chain("MELI", meli_chain)
    # Trading model uses the last tradable day (Friday); provider expects the OCC Saturday date.
    assert _thetadata_option_query_expiration(date(2015, 1, 16), symbol="MELI") == date(2015, 1, 17)

    with _THETADATA_EXPIRY_MAP_LOCK:
        _THETADATA_EXPIRY_MAP.clear()


def test_thetadata_expiration_mapping_prefers_exact_friday_when_both_exist() -> None:
    """When both Friday and Saturday keys exist, prefer the exact-Friday provider expiry."""
    with _THETADATA_EXPIRY_MAP_LOCK:
        _THETADATA_EXPIRY_MAP.clear()

    # Both keys normalize to the same tradable Friday. Prefer the Friday provider expiry so
    # symbols like CVNA don't generate 472/empty quote history payloads.
    chain = {
        "UnderlyingSymbol": "CVNA",
        "Chains": {"CALL": {"2018-02-16": [20.0], "2018-02-17": [20.0]}, "PUT": {}},
    }
    _register_thetadata_expiry_map_from_chain("CVNA", chain)
    assert _thetadata_option_query_expiration(date(2018, 2, 16), symbol="CVNA") == date(2018, 2, 16)

    with _THETADATA_EXPIRY_MAP_LOCK:
        _THETADATA_EXPIRY_MAP.clear()


def test_placeholder_option_cache_refetches_when_provider_expiry_mapping_changes() -> None:
    """Legacy no-data placeholders must not block a later correctly mapped Theta quote request."""
    with _THETADATA_EXPIRY_MAP_LOCK:
        _THETADATA_EXPIRY_MAP.clear()

    chain = {
        "UnderlyingSymbol": "LITE",
        "Chains": {"CALL": {"2027-01-15": [920.0]}, "PUT": {}},
    }
    _register_thetadata_expiry_map_from_chain("LITE", chain)

    option = Asset("LITE", "option", expiration=date(2027, 1, 15), strike=920.0, right="CALL")
    remote_payload = {"provider_expiration": date(2027, 1, 15)}

    assert _placeholder_option_cache_needs_provider_expiry_refresh(
        option,
        {"version": 2, "rows": 1, "real_rows": 0, "placeholders": 1},
        remote_payload,
    )
    assert _placeholder_option_cache_needs_provider_expiry_refresh(
        option,
        {"cache_payload": {"provider_expiration": "2027-01-16"}},
        remote_payload,
    )
    assert not _placeholder_option_cache_needs_provider_expiry_refresh(
        option,
        {"cache_payload": {"provider_expiration": "2027-01-15"}},
        remote_payload,
    )

    with _THETADATA_EXPIRY_MAP_LOCK:
        _THETADATA_EXPIRY_MAP.clear()
