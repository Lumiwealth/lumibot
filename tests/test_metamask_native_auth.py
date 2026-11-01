"""Native OAuth refresh, rotation and restart recovery without wallet operations."""

import base64
import json
from unittest.mock import Mock

import pytest
import requests

from lumibot.tools.metamask.auth import TokenManager
from lumibot.tools.metamask.provider import Credentials, MetaMaskError

ISSUER = "https://oidc.api.cx.metamask.io"
TOKEN_URL = ISSUER + "/oauth2/token"


def token(expiry=100):
    payload = (
        base64.urlsafe_b64encode(json.dumps({"exp": expiry, "jti": "test-id", "sub": "project"}).encode())
        .decode()
        .rstrip("=")
    )
    return "test." + payload + ".test"


def reply(payload, status=200):
    result = Mock(status_code=status)
    result.json.return_value = payload
    return result


def transport(rotated=True):
    session = Mock(spec=requests.Session)
    grant = {"access_token": token(5000), "token_type": "Bearer", "expires_in": 3600}
    if rotated:
        grant["refresh_token"] = "rotated-refresh"
    session.request.side_effect = [
        reply({"issuer": ISSUER, "token_endpoint": TOKEN_URL, "token_endpoint_auth_methods_supported": ["none"]}),
        reply(grant),
        reply({"active": True}),
    ]
    return session


def test_refresh_uses_discovery_public_client_and_atomically_keeps_rotated_tokens(tmp_path):
    session = transport()
    path = tmp_path / "session.json"
    manager = TokenManager(
        Credentials("project", token(), "original-refresh", 100), path=path, session=session, clock=lambda: 1000
    )
    result = manager()
    assert result.refresh_token == "rotated-refresh"
    args, kwargs = session.request.call_args_list[1]
    assert args == ("POST", TOKEN_URL)
    assert kwargs["data"] == {
        "grant_type": "refresh_token",
        "refresh_token": "original-refresh",
        "client_id": "cli-agent-client",
    }
    assert "Authorization" not in kwargs["headers"]
    assert kwargs["allow_redirects"] is False
    stored = json.loads(path.read_text())
    assert stored["refresh_token"] == "rotated-refresh"
    assert stored["access_token"] == result.access_token
    assert stored["refresh_pending"] is False
    assert path.stat().st_mode & 0o777 == 0o600


def test_restart_reads_rotated_credentials_without_using_original_pair_again(tmp_path):
    path = tmp_path / "session.json"
    first = TokenManager(
        Credentials("project", token(), "original-refresh", 100), path=path, session=transport(), clock=lambda: 1000
    )
    current = first()
    session = Mock(spec=requests.Session)
    restored = TokenManager(path=path, session=session, clock=lambda: 1001)
    assert restored().access_token == current.access_token
    session.request.assert_not_called()


def test_unknown_refresh_outcome_is_durable_and_never_blindly_retried(tmp_path):
    path = tmp_path / "session.json"
    session = transport()
    session.request.side_effect = [reply({"issuer": ISSUER, "token_endpoint": TOKEN_URL}), requests.Timeout()]
    manager = TokenManager(
        Credentials("project", token(), "original-refresh", 100), path=path, session=session, clock=lambda: 1000
    )
    with pytest.raises(MetaMaskError, match="REFRESH_OUTCOME_UNKNOWN"):
        manager()
    assert json.loads(path.read_text())["refresh_pending"] is True
    session.request.reset_mock()
    restored = TokenManager(path=path, session=session, clock=lambda: 1001)
    with pytest.raises(MetaMaskError, match="REFRESH_OUTCOME_UNKNOWN"):
        restored()
    session.request.assert_not_called()


def test_missing_rotated_refresh_token_preserves_existing_one(tmp_path):
    manager = TokenManager(
        Credentials("project", token(), "original-refresh", 100),
        path=tmp_path / "session.json",
        session=transport(False),
        clock=lambda: 1000,
    )
    assert manager().refresh_token == "original-refresh"


@pytest.mark.parametrize(
    "metadata",
    [
        {"issuer": "https://different.example", "token_endpoint": TOKEN_URL},
        {"issuer": ISSUER, "token_endpoint": "http://oidc.api.cx.metamask.io/token"},
        {"issuer": ISSUER, "token_endpoint": "https://different.example/token"},
        {"issuer": ISSUER, "token_endpoint": "https://user:password@oidc.api.cx.metamask.io/token"},
    ],
)
def test_invalid_discovery_never_receives_refresh_secret(metadata, tmp_path):
    session = Mock(spec=requests.Session)
    session.request.return_value = reply(metadata)
    manager = TokenManager(
        Credentials("project", token(), "refresh", 100),
        path=tmp_path / "session.json",
        session=session,
        clock=lambda: 1000,
    )
    with pytest.raises(MetaMaskError, match="INVALID_AUTH_METADATA"):
        manager()
    assert session.request.call_count == 1


def test_four_field_browser_token_keeps_only_credentials():
    credentials = Credentials.from_config({"PROJECT_ID": "project", "CLI_TOKEN": "access:refresh:yes:campaign"})
    assert credentials.access_token == "access"
    assert credentials.refresh_token == "refresh"


def test_expired_session_without_refresh_fails_before_network():
    session = Mock(spec=requests.Session)
    manager = TokenManager(Credentials("project", token(), expires_at=100), session=session, clock=lambda: 1000)
    with pytest.raises(MetaMaskError, match="AUTHENTICATION_REQUIRED"):
        manager()
    session.request.assert_not_called()
