from __future__ import annotations

import os

import pytest
import requests

from lumibot.credentials import get_available_projectx_firms, get_projectx_config

pytestmark = pytest.mark.apitest


def _pick_firm() -> str | None:
    explicit = (os.environ.get("PROJECTX_FIRM") or "").strip()
    if explicit:
        return explicit
    firms = get_available_projectx_firms()
    return firms[0] if firms else None


def test_projectx_can_auth_and_list_accounts():
    """Smoke test for ProjectX connectivity.

    This intentionally does NOT place orders.
    """
    firm = _pick_firm()
    if not firm:
        pytest.skip("Missing ProjectX config. Set PROJECTX_FIRM or PROJECTX_<FIRM>_API_KEY/_USERNAME env vars.")

    config = get_projectx_config(firm)
    username = (config.get("username") or "").strip()
    api_key = (config.get("api_key") or "").strip()
    base_url = (config.get("base_url") or "").strip()

    if not username or not api_key or not base_url:
        pytest.skip(f"Incomplete ProjectX config for firm={firm}. Need username/api_key/base_url.")

    if not base_url.endswith("/"):
        base_url += "/"

    # Auth
    auth = requests.post(
        f"{base_url}api/auth/loginkey",
        json={"userName": username, "apiKey": api_key},
        timeout=10,
    )
    auth.raise_for_status()
    auth_payload = auth.json()
    if not isinstance(auth_payload, dict) or not auth_payload.get("success"):
        pytest.fail(f"ProjectX auth failed: {auth_payload}")

    token = auth_payload.get("token")
    assert token and isinstance(token, str)

    # Account search
    resp = requests.post(
        f"{base_url}api/account/search",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"onlyActiveAccounts": True},
        timeout=10,
    )
    resp.raise_for_status()
    payload = resp.json()
    assert isinstance(payload, dict)
    assert payload.get("success") is True
    assert "accounts" in payload
