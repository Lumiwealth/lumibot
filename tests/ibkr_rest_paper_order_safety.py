"""Safety gates shared by IBKR REST paper-order API tests.

These helpers intentionally live in ``tests``.  They protect tests that can
change broker state without changing the runtime broker contract.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

import pytest


def _as_bool(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def mask_ibkr_account_id(account_id: object) -> str:
    """Return an account identifier's suffix without exposing the identifier."""
    value = str(account_id or "").strip()
    if not value:
        return "<missing>"
    if len(value) <= 4:
        return "****"
    return f"****{value[-4:]}"


def require_explicit_ibkr_rest_paper_configuration(
    config: Mapping[str, object],
    environment: Mapping[str, str] | None = None,
) -> None:
    """Skip unavailable test setups and reject a non-explicit paper selection.

    ``INTERACTIVE_BROKERS_REST_CONFIG`` defaults ``USE_PAPER_ACCOUNT`` to
    ``true``.  An order-changing test must instead see an explicit raw
    ``IB_USE_PAPER_ACCOUNT=true`` environment setting.
    """
    environment = os.environ if environment is None else environment
    external_gateway = bool(str(config.get("API_URL") or "").strip()) or _as_bool(
        config.get("RUNNING_ON_SERVER")
    )
    if not external_gateway and not (
        str(config.get("IB_USERNAME") or "").strip()
        and str(config.get("IB_PASSWORD") or "").strip()
    ):
        pytest.skip("IBKR REST paper-order test requires configured gateway credentials")

    paper_setting = environment.get("IB_USE_PAPER_ACCOUNT")
    if paper_setting is None or not str(paper_setting).strip():
        pytest.skip(
            "IBKR REST paper-order test requires explicit IB_USE_PAPER_ACCOUNT=true"
        )
    if str(paper_setting).strip().lower() != "true":
        pytest.fail(
            "IBKR REST paper-order test refuses to run unless "
            "IB_USE_PAPER_ACCOUNT is explicitly true"
        )


def require_ibkr_rest_paper_account(
    *,
    configured_account_id: object,
    authenticated_account_id: object,
    data_source_account_id: object | None = None,
) -> str:
    """Reject non-paper or ambiguous account selections before order creation."""
    authenticated = str(authenticated_account_id or "").strip()
    if not authenticated:
        pytest.fail("IBKR REST paper-order test could not determine an authenticated account")
    if not authenticated.upper().startswith("DU"):
        pytest.fail(
            "IBKR REST paper-order test refuses non-paper account "
            f"{mask_ibkr_account_id(authenticated)}; expected the DU paper-account prefix"
        )

    configured = str(configured_account_id or "").strip()
    if configured and configured != authenticated:
        pytest.fail(
            "Configured IB_ACCOUNT_ID does not match the authenticated IBKR account "
            f"(configured {mask_ibkr_account_id(configured)}, "
            f"authenticated {mask_ibkr_account_id(authenticated)})"
        )

    selected_by_data_source = str(data_source_account_id or "").strip()
    if selected_by_data_source and selected_by_data_source != authenticated:
        pytest.fail(
            "InteractiveBrokersRESTData selected a different account than the "
            f"authenticated account (selected {mask_ibkr_account_id(selected_by_data_source)}, "
            f"authenticated {mask_ibkr_account_id(authenticated)})"
        )
    return authenticated


def _authenticated_selected_account_id(data_source) -> str:
    """Read the authenticated selection without logging the response payload."""
    try:
        response = data_source.http_client.get(
            f"{data_source.base_url}/iserver/accounts",
            verify=data_source.verify_ssl,
            timeout=data_source.request_timeout,
        )
        if not 200 <= response.status_code < 300:
            pytest.skip("IBKR REST gateway is unavailable for the paper-order test")
        payload = response.json()
    except Exception:
        pytest.skip("IBKR REST gateway is unavailable for the paper-order test")

    selected_account_id = payload.get("selectedAccount") if isinstance(payload, dict) else None
    if not isinstance(selected_account_id, str) or not selected_account_id.strip():
        pytest.skip("IBKR REST gateway did not expose an authenticated selected account")
    return selected_account_id


def require_authenticated_ibkr_rest_paper_account(
    data_source,
    config: Mapping[str, object],
) -> str:
    """Verify the post-authentication account before an API test creates an order."""
    authenticated_account_id = _authenticated_selected_account_id(data_source)
    return require_ibkr_rest_paper_account(
        configured_account_id=config.get("IB_ACCOUNT_ID"),
        authenticated_account_id=authenticated_account_id,
        data_source_account_id=data_source.account_id,
    )


@pytest.fixture
def ibkr_rest_paper_order_data_source():
    """Yield an authenticated, verified paper data source for order API tests."""
    from lumibot.credentials import INTERACTIVE_BROKERS_REST_CONFIG
    from lumibot.data_sources import InteractiveBrokersRESTData

    config = dict(INTERACTIVE_BROKERS_REST_CONFIG)
    require_explicit_ibkr_rest_paper_configuration(config)
    data_source = None
    try:
        data_source = InteractiveBrokersRESTData(config)
    except Exception:
        pytest.skip("IBKR REST gateway is unavailable for the paper-order test")

    try:
        require_authenticated_ibkr_rest_paper_account(data_source, config)
        yield data_source
    finally:
        if data_source is not None:
            data_source.stop()
