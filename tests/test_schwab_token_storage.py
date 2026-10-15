"""Synthetic token-storage failures; never authenticate or contact Schwab."""

import base64
import builtins
import json
import os
import stat
import time
from types import SimpleNamespace

import pytest

from lumibot.tools.schwab_helper import SchwabHelper


def encoded(payload):
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


@pytest.mark.parametrize("payload", [[], None, {}, {"access_token": "a", "refresh_token": []},
                                     {"access_token": "", "refresh_token": "r"}])
def test_invalid_token_payload_cannot_replace_existing_credentials(tmp_path, payload):
    path = tmp_path / "token.json"
    path.write_text("preserve-existing-token")
    with pytest.raises(ValueError):
        SchwabHelper._save_payload_str_to_token_file(encoded(payload), path)
    assert path.read_text() == "preserve-existing-token"


def test_failed_metadata_read_preserves_file_and_reports_error(tmp_path):
    path = tmp_path / "token.json"
    path.write_text("{invalid-json")
    with pytest.raises(ValueError, match="metadata"):
        SchwabHelper._ensure_token_metadata(path)
    assert path.read_text() == "{invalid-json"


def test_atomic_token_replace_failure_preserves_old_file_and_removes_temp(monkeypatch, tmp_path):
    path = tmp_path / "token.json"
    path.write_text("preserve-existing-token")

    def fail(*args):
        raise OSError("synthetic replacement failure")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(OSError):
        SchwabHelper._save_payload_str_to_token_file(encoded({"access_token": "a", "refresh_token": "r"}), path)
    assert path.read_text() == "preserve-existing-token"
    assert list(tmp_path.iterdir()) == [path]


def test_saved_token_is_private_preserves_literal_characters_and_uses_seven_day_default(tmp_path):
    path = tmp_path / "token.json"
    SchwabHelper._save_payload_str_to_token_file(
        encoded({"access_token": "synthetic@", "refresh_token": "refresh@", "id_token": "omit"}), path,
    )
    saved = json.loads(path.read_text())
    assert saved["token"]["access_token"] == "synthetic@"
    assert saved["token"]["refresh_token"] == "refresh@"
    assert "id_token" not in saved["token"]
    assert saved["token"]["refresh_token_expires_in"] == 7 * 86400
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_bad_interactive_payload_preserves_preexisting_token(monkeypatch, tmp_path):
    import lumibot.tools.schwab_helper as module

    path = tmp_path / "token.json"
    path.write_text("preserve-existing-token")
    monkeypatch.setattr(module.webbrowser, "open", lambda *args: None)
    monkeypatch.setattr(builtins, "input", lambda *args: "invalid-payload")
    assert SchwabHelper._initiate_schwab_auth_and_get_token_payload("synthetic-app", "https://callback.invalid", path) is False
    assert path.read_text() == "preserve-existing-token"


@pytest.mark.parametrize("contents", ["{invalid-json", '{"access_token":"a","refresh_token":"r"}'])
def test_broker_token_validation_failure_preserves_file_without_opening_login(monkeypatch, tmp_path, contents):
    from lumibot.brokers.broker import Broker
    from lumibot.brokers.schwab import Schwab

    path = tmp_path / "token.json"
    path.write_text(contents)
    monkeypatch.setattr(Broker, "_start_orders_thread", lambda *args: None)
    monkeypatch.delenv("SCHWAB_TOKEN", raising=False)
    monkeypatch.setattr(SchwabHelper, "_initiate_schwab_auth_and_get_token_payload",
                        lambda *args: pytest.fail("A storage failure must not initiate login"))

    def fail(*args):
        raise OSError("synthetic replacement failure")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(ConnectionError, match="saved Schwab token"):
        Schwab(config={"SCHWAB_APP_KEY": "synthetic-app", "SCHWAB_APP_SECRET": "synthetic-secret",
                       "SCHWAB_ACCOUNT_NUMBER": "synthetic-account", "SCHWAB_TOKEN_PATH": str(path)}, connect_stream=False)
    assert path.read_text() == contents


@pytest.fixture
def broker_with_token_updater(monkeypatch, tmp_path):
    import requests_oauthlib

    from lumibot.brokers import schwab as module
    from lumibot.brokers.broker import Broker

    path = tmp_path / "token.json"
    SchwabHelper._save_payload_str_to_token_file(
        encoded({"access_token": "old-access", "refresh_token": "old-refresh", "issued_at": time.time() * 1000}), path,
    )

    class Session:
        def __init__(self, **kwargs):
            self.token = kwargs["token"]
            self.token_updater = kwargs["token_updater"]
            self.auto_refresh_url = kwargs["auto_refresh_url"]

        def register_compliance_hook(self, *args):
            pass

    class Client:
        def __init__(self, *, session, **kwargs):
            self.session = session

        def get_account_numbers(self):
            return SimpleNamespace(status_code=200, json=lambda: [
                {"accountNumber": "synthetic-account", "hashValue": "synthetic-hash"},
            ])

    monkeypatch.setattr(requests_oauthlib, "OAuth2Session", Session)
    monkeypatch.setattr(module, "Client", Client)
    monkeypatch.setattr(Broker, "_start_orders_thread", lambda *args: None)
    monkeypatch.setattr(module.Schwab, "_finish_initialization", lambda *a, **k: None)
    monkeypatch.setattr(module.Schwab, "_get_stream_object", lambda *a: None)
    monkeypatch.setattr(module, "_start_schwab_proactive_token_refresh", lambda *a, **k: None)
    monkeypatch.delenv("SCHWAB_TOKEN", raising=False)
    monkeypatch.delenv("BOTSPOT_FORCE_BROKER_TOKEN_REFRESH", raising=False)
    monkeypatch.delenv("LUMIBOT_OAUTH_REFRESH_MODE", raising=False)
    broker = module.Schwab(config={"SCHWAB_APP_KEY": "synthetic-app", "SCHWAB_APP_SECRET": "synthetic-secret",
                                   "SCHWAB_ACCOUNT_NUMBER": "synthetic-account", "SCHWAB_TOKEN_PATH": str(path)},
                           connect_stream=False)
    yield broker, path
    broker.cleanup_streams()


def test_refresh_persistence_failure_does_not_advance_token_state(monkeypatch, broker_with_token_updater):
    from lumibot.brokers.schwab import SchwabTokenPersistenceError

    broker, path = broker_with_token_updater
    before = path.read_bytes()

    def fail(*args):
        raise OSError("synthetic write failure")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(SchwabTokenPersistenceError):
        broker.client.session.token_updater({"access_token": "new-access", "refresh_token": "new-refresh"})
    assert path.read_bytes() == before
    assert broker.client.session.token["access_token"] == "old-access"
    assert list(path.parent.iterdir()) == [path]


@pytest.mark.parametrize("payload", [{}, {"access_token": []}, {"access_token": None}, {"access_token": " "},
                                     {"access_token": "new-access", "refresh_token": {"private": "invalid"}}])
def test_malformed_refresh_cannot_overwrite_usable_token(broker_with_token_updater, payload):
    from lumibot.brokers.schwab import SchwabTokenPersistenceError

    broker, path = broker_with_token_updater
    before = path.read_bytes()
    with pytest.raises(SchwabTokenPersistenceError) as caught:
        broker.client.session.token_updater(payload)
    assert "private" not in str(caught.value)
    assert path.read_bytes() == before
    assert broker.client.session.token["access_token"] == "old-access"


def test_successful_refresh_persists_private_file_and_preserves_refresh_token(broker_with_token_updater):
    broker, path = broker_with_token_updater
    created = json.loads(path.read_text())["creation_timestamp"]
    broker.client.session.token_updater({"access_token": "new-access", "expires_in": 1800})
    saved = json.loads(path.read_text())
    assert saved["creation_timestamp"] == created
    assert saved["token"]["refresh_token"] == "old-refresh"
    assert saved["token"]["access_token"] == "new-access"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
