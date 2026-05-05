"""
Smoke tests for the Tastytrade broker.

The offline tests in this file are pure unit tests (no network, no
credentials) and run on every CI invocation. The single ``apitest``-marked
test at the bottom hits the Tastytrade sandbox and is gated on real
credentials being present in the environment.
"""

from __future__ import annotations

import asyncio
import os
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Offline tests (no credentials, no network)
# ---------------------------------------------------------------------------

def test_missing_credentials_raises():
    """Missing client_secret / refresh_token / account_number must raise ValueError."""
    from lumibot.brokers.tastytrade import Tastytrade

    # Strip any env vars that might leak in from the dev shell.
    env_keys = (
        "TASTYTRADE_CLIENT_SECRET",
        "TASTYTRADE_REFRESH_TOKEN",
        "TASTYTRADE_ACCOUNT_NUMBER",
        "TASTYTRADE_SANDBOX",
    )
    with patch.dict(os.environ, {k: "" for k in env_keys}, clear=False):
        with pytest.raises(ValueError) as excinfo:
            Tastytrade()
    assert "client_secret" in str(excinfo.value)
    assert "refresh_token" in str(excinfo.value)
    assert "account_number" in str(excinfo.value)


def test_async_bridge_runs_and_returns_value():
    """The asyncio bridge must execute coroutines and return their result."""
    from lumibot.brokers.tastytrade import _AsyncBridge

    bridge = _AsyncBridge()
    try:
        async def _add():
            await asyncio.sleep(0)
            return 42

        assert bridge.run(_add()) == 42
    finally:
        bridge.close()


@patch("lumibot.brokers.tastytrade._TTAccount")
@patch("lumibot.brokers.tastytrade._TTSession")
def test_init_with_kwargs_resolves_account(mock_session_cls, mock_account_cls):
    """Constructor builds a Session, fetches the Account, and stores both."""
    from lumibot.brokers.tastytrade import Tastytrade

    fake_session = MagicMock(name="Session")
    mock_session_cls.return_value = fake_session

    fake_account = MagicMock(name="Account")

    async def _get(_session, _account_number):
        assert _account_number == "ACC123"
        return fake_account

    mock_account_cls.get.side_effect = _get

    broker = Tastytrade(
        client_secret="cs",
        refresh_token="rt",
        account_number="ACC123",
        is_test=True,
        connect_stream=False,
    )
    try:
        mock_session_cls.assert_called_once_with(
            provider_secret="cs",
            refresh_token="rt",
            is_test=True,
        )
        assert broker._session is fake_session
        assert broker._account is fake_account
        assert broker._tt_account_number == "ACC123"
        assert broker._tt_is_test is True
    finally:
        broker._async_bridge.close()


# ---------------------------------------------------------------------------
# Live sandbox smoke (only runs when sandbox credentials are present)
# ---------------------------------------------------------------------------

@pytest.mark.apitest
@pytest.mark.skipif(
    not all(os.environ.get(k) for k in (
        "TASTYTRADE_CLIENT_SECRET",
        "TASTYTRADE_REFRESH_TOKEN",
        "TASTYTRADE_ACCOUNT_NUMBER",
    )),
    reason="Tastytrade sandbox credentials not configured.",
)
def test_live_sandbox_balances_and_positions():
    """Hit the sandbox API for balances + positions; expects no exceptions."""
    from lumibot.brokers.tastytrade import Tastytrade

    broker = Tastytrade(connect_stream=False)
    try:
        cash, positions_value, nlv = broker._get_balances_at_broker(
            quote_asset=None, strategy=None,
        )
        assert isinstance(cash, float)
        assert isinstance(nlv, float)
        positions = broker._pull_positions(strategy=None)
        assert isinstance(positions, list)
    finally:
        broker._async_bridge.close()
