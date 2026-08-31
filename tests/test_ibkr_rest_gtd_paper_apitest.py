"""Opt-in IBKR REST GTD capability probe using only the what-if endpoint.

This module is excluded from ordinary CI by the existing ``apitest`` marker.
It probes one candidate Client Portal field without changing LumiBot's
production GTD serialization guard.  A successful paper result is evidence
only: enabling production GTD requires a separate reviewed implementation
decision after an authorized paper probe result is available.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from tests.ibkr_rest_paper_order_safety import ibkr_rest_paper_order_data_source


pytestmark = [pytest.mark.apitest, pytest.mark.ibkr]

_SYMBOL = "SPY"
_REQUEST_TIMEOUT_SECONDS = 5.0
# Client Portal snapshots often need a brief warm-up request before field 31 is
# populated. Keep the paper probe bounded to a 15-second warm-up while allowing
# that subscription to become usable on an otherwise healthy, authenticated gateway.
_MARKET_DATA_ATTEMPTS = 16
_MARKET_DATA_RETRY_SECONDS = 1.0
_MARGIN_RESPONSE_FIELDS = {
    "amount",
    "equity",
    "initial",
    "maintenance",
    "position",
}
_TIF_CAPABILITY_KEYS = {
    "tif",
    "tifs",
    "tiftypes",
    "timeinforce",
    "timeinforcetypes",
    "timeinforcevalues",
}


@dataclass(frozen=True)
class _WhatIfClassification:
    outcome: str
    diagnostic: str


def _request_timeout(data_source) -> float:
    """Keep each live capability request bounded without changing transport safeguards."""
    return min(float(data_source.request_timeout), _REQUEST_TIMEOUT_SECONDS)


def _request_json(data_source, method: str, path: str, payload: dict | None = None):
    """Use the authenticated data source client without logging response content.

    Calling the configured client's HTTP methods preserves its authenticated
    session and TLS verification settings.  This test deliberately does not
    use ``post_to_endpoint`` for what-if requests because its order-confirm
    handling can POST to ``/iserver/reply``; this probe must contact only the
    non-ordering what-if endpoint for its order-shaped request.
    """
    if method not in {"GET", "POST"}:
        pytest.fail("IBKR GTD capability probe permits only GET and POST requests")
    if method == "POST":
        expected_path = f"/iserver/account/{data_source.account_id}/orders/whatif"
        if path != expected_path:
            pytest.fail("IBKR GTD capability probe refuses POST outside /orders/whatif")

    url = f"{data_source.base_url}{path}"
    request = data_source.http_client.get if method == "GET" else data_source.http_client.post
    try:
        if method == "GET":
            response = request(
                url,
                verify=data_source.verify_ssl,
                timeout=_request_timeout(data_source),
            )
        else:
            response = request(
                url,
                json=payload,
                verify=data_source.verify_ssl,
                timeout=_request_timeout(data_source),
            )
        try:
            response_payload = response.json()
        except ValueError:
            response_payload = None
    except Exception:
        pytest.skip("IBKR REST gateway is unavailable for the GTD paper capability probe")
    return response.status_code, response_payload


def _contract_conid(
    data_source,
    *,
    symbol: str = _SYMBOL,
    skip_context: str = "GTD paper capability probe",
) -> int:
    status_code, payload = _request_json(
        data_source,
        "GET",
        f"/iserver/secdef/search?symbol={symbol}",
    )
    if not 200 <= status_code < 300 or not isinstance(payload, list):
        pytest.skip(
            f"IBKR REST gateway did not provide a usable {symbol} contract for the {skip_context}"
        )

    for candidate in payload:
        if not isinstance(candidate, dict):
            continue
        conid = candidate.get("conid")
        if isinstance(conid, bool):
            continue
        try:
            return int(conid)
        except (TypeError, ValueError):
            continue
    pytest.skip(
        f"IBKR REST gateway did not provide a usable {symbol} contract for the {skip_context}"
    )


def _reference_price(
    data_source,
    conid: int,
    *,
    symbol: str = _SYMBOL,
    skip_context: str = "GTD paper capability probe",
) -> float:
    path = f"/iserver/marketdata/snapshot?conids={conid}&fields=31"
    for attempt in range(_MARKET_DATA_ATTEMPTS):
        status_code, payload = _request_json(data_source, "GET", path)
        if 200 <= status_code < 300 and isinstance(payload, list) and payload:
            raw_price = payload[0].get("31") if isinstance(payload[0], dict) else None
            if isinstance(raw_price, str) and raw_price.startswith("C"):
                raw_price = raw_price[1:].strip()
            try:
                price = float(raw_price)
            except (TypeError, ValueError):
                price = 0.0
            if price > 0:
                return price
        if attempt + 1 < _MARKET_DATA_ATTEMPTS:
            time.sleep(_MARKET_DATA_RETRY_SECONDS)
    pytest.skip(
        f"IBKR REST gateway did not provide a usable {symbol} reference price for the {skip_context}"
    )


def _tif_tokens(value: Any) -> set[str]:
    if isinstance(value, str):
        return {token.upper() for token in re.split(r"[\s,;|]+", value) if token}
    if isinstance(value, (list, tuple, set)):
        return {
            str(item).strip().upper()
            for item in value
            if isinstance(item, (str, int)) and str(item).strip()
        }
    return set()


def _gtd_tif_capability(rules: Any) -> bool | None:
    """Return only the GTD capability fact, never the contract response itself."""
    found_tif_field = False

    def walk(value: Any) -> bool:
        nonlocal found_tif_field
        if isinstance(value, dict):
            for key, nested in value.items():
                normalized_key = re.sub(r"[^a-z0-9]", "", str(key).lower())
                if normalized_key in _TIF_CAPABILITY_KEYS:
                    found_tif_field = True
                    if "GTD" in _tif_tokens(nested):
                        return True
                if walk(nested):
                    return True
        elif isinstance(value, list):
            return any(walk(item) for item in value)
        return False

    return True if walk(rules) else (False if found_tif_field else None)


def _candidate_good_till_date() -> str:
    """Serialize a timezone-aware UTC datetime in IBKR's UTC GTD candidate format."""
    future = (datetime.now(timezone.utc) + timedelta(days=7)).replace(microsecond=0)
    return future.strftime("%Y%m%d-%H:%M:%S")


def _candidate_whatif_payload(conid: int, reference_price: float) -> dict:
    # A buy limit five percent below the just-read reference price is designed
    # to remain non-marketable while still being a conventional one-share ticket.
    non_marketable_limit = max(0.01, round(reference_price * 0.95, 2))
    return {
        "orders": [
            {
                "conid": conid,
                "quantity": 1,
                "orderType": "LMT",
                "side": "BUY",
                "tif": "GTD",
                "price": non_marketable_limit,
                "listingExchange": "SMART",
                "goodTillDate": _candidate_good_till_date(),
            }
        ]
    }


def _contains_order_id(value: Any) -> bool:
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).replace("_", "").lower() == "orderid" and nested not in (None, ""):
                return True
            if _contains_order_id(nested):
                return True
    elif isinstance(value, list):
        return any(_contains_order_id(item) for item in value)
    return False


def _rejection_diagnostic(status_code: int, payload: Any) -> str:
    """Classify a rejection without exposing account data or raw broker text."""
    messages: list[str] = []
    if isinstance(payload, dict):
        for key in ("error", "message", "detail", "warn"):
            value = payload.get(key)
            if isinstance(value, str):
                messages.append(value.lower())
    text = " ".join(messages)
    terms = []
    if "gtd" in text:
        terms.append("GTD")
    if "goodtilldate" in text or "good till date" in text:
        terms.append("goodTillDate")
    if "time in force" in text or "tif" in text:
        terms.append("tif")
    return (
        f"http_status={status_code}, "
        f"reason_mentions={','.join(terms) if terms else 'no_gtd_field_or_tif_term'}"
    )


def _classify_whatif_response(status_code: int, payload: Any) -> _WhatIfClassification:
    if _contains_order_id(payload):
        return _WhatIfClassification(
            "ambiguous",
            f"http_status={status_code}, response_contains_order_id",
        )

    if not 200 <= status_code < 300:
        return _WhatIfClassification("rejected", _rejection_diagnostic(status_code, payload))

    if not isinstance(payload, dict):
        return _WhatIfClassification("ambiguous", f"http_status={status_code}, non_object_response")

    if payload.get("error") not in (None, ""):
        return _WhatIfClassification("rejected", _rejection_diagnostic(status_code, payload))

    # A warning requires manual review.  It must not become positive GTD
    # evidence merely because a gateway revision emits a new warning category.
    if payload.get("warn") not in (None, ""):
        return _WhatIfClassification("ambiguous", f"http_status={status_code}, warning_response")

    if _MARGIN_RESPONSE_FIELDS.intersection(payload):
        return _WhatIfClassification("accepted", f"http_status={status_code}, margin_preview")
    return _WhatIfClassification("ambiguous", f"http_status={status_code}, non_preview_response")


def test_ibkr_rest_gtd_tif_capability_on_verified_paper_account(
    ibkr_rest_paper_order_data_source,
    record_property,
):
    """Record the TIF capability fact from info-and-rules without logging it."""
    data_source = ibkr_rest_paper_order_data_source
    conid = _contract_conid(data_source)
    status_code, rules = _request_json(
        data_source,
        "GET",
        f"/iserver/contract/{conid}/info-and-rules",
    )
    if not 200 <= status_code < 300:
        pytest.fail(f"IBKR GTD info-and-rules probe failed: http_status={status_code}")

    capability = _gtd_tif_capability(rules)
    record_property(
        "ibkr_gtd_tif_capability",
        "advertised" if capability is True else "not_advertised" if capability is False else "not_exposed",
    )


def test_ibkr_rest_good_till_date_whatif_on_verified_paper_account(
    ibkr_rest_paper_order_data_source,
    record_property,
):
    """Probe only ``/orders/whatif``; it never calls the order-submission endpoint."""
    data_source = ibkr_rest_paper_order_data_source
    conid = _contract_conid(data_source)
    reference_price = _reference_price(data_source, conid)
    payload = _candidate_whatif_payload(conid, reference_price)

    # This is intentionally the sole order-shaped request in the module.
    status_code, response = _request_json(
        data_source,
        "POST",
        f"/iserver/account/{data_source.account_id}/orders/whatif",
        payload,
    )
    classification = _classify_whatif_response(status_code, response)
    record_property("ibkr_gtd_whatif_result", classification.outcome)
    record_property("ibkr_gtd_whatif_diagnostic", classification.diagnostic)
    record_property("ibkr_gtd_candidate_format", "UTC_YYYYMMDD-HHMMSS")

    assert classification.outcome in {"accepted", "rejected"}, (
        "IBKR GTD what-if response was ambiguous; it is not evidence that "
        "goodTillDate is supported"
    )
    if classification.outcome == "rejected":
        pytest.fail(
            "IBKR GTD what-if rejected the goodTillDate candidate "
            f"({classification.diagnostic}); production GTD remains disabled"
        )

    assert not _contains_order_id(response), "what-if response must not acknowledge a working order"
    assert isinstance(response, dict)
    assert _MARGIN_RESPONSE_FIELDS.intersection(response), "accepted GTD probe must return a margin preview"
