import base64
import json
import time

import pytest

from lumibot.brokers.tradier import Tradier
from lumibot.data_sources.tradier_data import TradierData


def _b64url(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_b64url(payload: str) -> dict:
    missing_padding = len(payload) % 4
    if missing_padding:
        payload += "=" * (4 - missing_padding)
    return json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))


def test_oauth_force_refresh_when_not_expired_writes_botspot_rotation_artifact(monkeypatch, tmp_path):
    token_json = {
        "access_token": "old-access",
        "refresh_token": "old-refresh",
        "expires_in": 86400,
        "issued_at": int(time.time() * 1000),
    }
    rotation_path = tmp_path / "tradier_token_rotation.json"
    monkeypatch.setenv("TRADIER_TOKEN", _b64url(token_json))
    monkeypatch.setenv("TRADIER_REFRESH_TOKEN", "old-refresh")
    monkeypatch.setenv("TRADIER_OAUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("TRADIER_OAUTH_CLIENT_SECRET", "secret")
    monkeypatch.setenv("BOTSPOT_TRADIER_TOKEN_ROTATION_PATH", str(rotation_path))
    monkeypatch.setenv("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", "true")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")

    class _Resp:
        ok = True
        status_code = 200
        text = ""

        def json(self):
            return {
                "access_token": "forced-access",
                "refresh_token": "forced-refresh",
                "expires_in": 86400,
                "issued_at": int(time.time() * 1000),
            }

    from lumibot.brokers import tradier as tradier_module

    refresh_calls = []

    def _fake_post(*args, **kwargs):
        refresh_calls.append((args, kwargs))
        return _Resp()

    monkeypatch.setattr(tradier_module.requests, "post", _fake_post)

    broker = Tradier(
        config={"ACCESS_TOKEN": None, "ACCOUNT_NUMBER": "1234", "PAPER": True},
        connect_stream=False,
    )

    assert refresh_calls
    assert broker._tradier_access_token == "forced-access"
    artifact = json.loads(rotation_path.read_text(encoding="utf-8"))
    assert artifact["TRADIER_ACCESS_TOKEN"] == "forced-access"
    assert artifact["TRADIER_REFRESH_TOKEN"] == "forced-refresh"
    decoded_token = _decode_b64url(artifact["TRADIER_TOKEN"])
    assert decoded_token["access_token"] == "forced-access"
    assert decoded_token["refresh_token"] == "forced-refresh"


def test_oauth_force_refresh_updates_tradier_token_path(monkeypatch, tmp_path):
    token_path = tmp_path / "tradier_token.json"
    token_path.write_text(
        json.dumps(
            {
                "access_token": "old-access",
                "refresh_token": "old-refresh",
                "expires_in": 86400,
                "issued_at": int(time.time() * 1000),
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRADIER_TOKEN_PATH", str(token_path))
    monkeypatch.setenv("TRADIER_REFRESH_TOKEN", "old-refresh")
    monkeypatch.setenv("TRADIER_OAUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("TRADIER_OAUTH_CLIENT_SECRET", "secret")
    monkeypatch.setenv("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", "true")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")

    class _Resp:
        ok = True
        status_code = 200
        text = ""

        def json(self):
            return {
                "access_token": "forced-access",
                "refresh_token": "forced-refresh",
                "expires_in": 86400,
                "issued_at": int(time.time() * 1000),
            }

    from lumibot.brokers import tradier as tradier_module

    monkeypatch.setattr(tradier_module.requests, "post", lambda *args, **kwargs: _Resp())

    broker = Tradier(
        config={"ACCESS_TOKEN": None, "ACCOUNT_NUMBER": "1234", "PAPER": True},
        connect_stream=False,
    )

    assert broker._tradier_access_token == "forced-access"
    rewritten = json.loads(token_path.read_text(encoding="utf-8"))
    assert rewritten["access_token"] == "forced-access"
    assert rewritten["refresh_token"] == "forced-refresh"


def test_oauth_force_refresh_fails_if_tradier_token_path_cannot_be_written(monkeypatch, tmp_path):
    token_path = tmp_path / "tradier_token.json"
    token_path.write_text(
        json.dumps(
            {
                "access_token": "old-access",
                "refresh_token": "old-refresh",
                "expires_in": 86400,
                "issued_at": int(time.time() * 1000),
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRADIER_TOKEN_PATH", str(token_path))
    monkeypatch.setenv("TRADIER_REFRESH_TOKEN", "old-refresh")
    monkeypatch.setenv("TRADIER_OAUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("TRADIER_OAUTH_CLIENT_SECRET", "secret")
    monkeypatch.setenv("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", "true")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")

    class _Resp:
        ok = True
        status_code = 200
        text = ""

        def json(self):
            return {
                "access_token": "forced-access",
                "refresh_token": "forced-refresh",
                "expires_in": 86400,
                "issued_at": int(time.time() * 1000),
            }

    from lumibot.brokers import tradier as tradier_module

    monkeypatch.setattr(tradier_module.requests, "post", lambda *args, **kwargs: _Resp())
    monkeypatch.setattr(
        tradier_module.Tradier,
        "_write_json_file_atomic",
        staticmethod(lambda *args, **kwargs: (_ for _ in ()).throw(OSError("disk full"))),
    )

    with pytest.raises(tradier_module.TradierTokenPersistenceError, match="Failed to persist refreshed OAuth token file"):
        Tradier(
            config={"ACCESS_TOKEN": None, "ACCOUNT_NUMBER": "1234", "PAPER": True},
            connect_stream=False,
        )

    unchanged = json.loads(token_path.read_text(encoding="utf-8"))
    assert unchanged["access_token"] == "old-access"


def test_oauth_force_refresh_failure_raises(monkeypatch):
    token_json = {
        "access_token": "old-access",
        "refresh_token": "old-refresh",
        "expires_in": 86400,
        "issued_at": int(time.time() * 1000),
    }
    monkeypatch.setenv("TRADIER_TOKEN", _b64url(token_json))
    monkeypatch.setenv("TRADIER_REFRESH_TOKEN", "old-refresh")
    monkeypatch.setenv("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", "true")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")
    monkeypatch.delenv("TRADIER_OAUTH_CLIENT_ID", raising=False)
    monkeypatch.delenv("TRADIER_OAUTH_CLIENT_SECRET", raising=False)

    from lumibot.brokers import tradier as tradier_module

    monkeypatch.setattr(
        tradier_module.requests,
        "post",
        lambda *args, **kwargs: pytest.fail("Forced-refresh failure test must not call network"),
    )

    with pytest.raises(RuntimeError, match="Forced OAuth token refresh failed"):
        Tradier(
            config={"ACCESS_TOKEN": None, "ACCOUNT_NUMBER": "1234", "PAPER": True},
            connect_stream=False,
        )


def test_force_refresh_is_noop_for_api_token_mode(monkeypatch):
    from lumibot.brokers import tradier as tradier_module

    monkeypatch.setenv("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", "true")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")
    monkeypatch.delenv("TRADIER_TOKEN", raising=False)
    monkeypatch.setattr(
        tradier_module.requests,
        "post",
        lambda *args, **kwargs: pytest.fail("API-token mode must not call OAuth refresh"),
    )

    broker = Tradier(account_number="1234", access_token="api-token", paper=True, connect_stream=False)
    assert broker._tradier_access_token == "api-token"


def test_oauth_force_refresh_updates_external_data_source(monkeypatch):
    token_json = {
        "access_token": "old-access",
        "refresh_token": "old-refresh",
        "expires_in": 86400,
        "issued_at": int(time.time() * 1000),
    }
    monkeypatch.setenv("TRADIER_TOKEN", _b64url(token_json))
    monkeypatch.setenv("TRADIER_REFRESH_TOKEN", "old-refresh")
    monkeypatch.setenv("TRADIER_OAUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("TRADIER_OAUTH_CLIENT_SECRET", "secret")
    monkeypatch.setenv("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", "true")
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")

    class _Resp:
        ok = True
        status_code = 200
        text = ""

        def json(self):
            return {
                "access_token": "forced-access",
                "refresh_token": "forced-refresh",
                "expires_in": 86400,
                "issued_at": int(time.time() * 1000),
            }

    from lumibot.brokers import tradier as tradier_module

    monkeypatch.setattr(tradier_module.requests, "post", lambda *args, **kwargs: _Resp())

    data_source = TradierData(
        account_number="1234",
        access_token="old-access",
        paper=True,
        delay=15,
    )

    broker = Tradier(
        config={"ACCESS_TOKEN": None, "ACCOUNT_NUMBER": "1234", "PAPER": True},
        data_source=data_source,
        connect_stream=False,
    )

    assert broker.data_source is data_source
    assert broker._tradier_access_token == "forced-access"
    assert data_source.api_key == "forced-access"
    assert data_source.tradier.AUTH_TOKEN == "forced-access"
