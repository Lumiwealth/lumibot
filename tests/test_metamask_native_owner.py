"""Explicit legacy-session import, ownership and pure Python entry points."""

import json
import os
import sqlite3
import subprocess
from dataclasses import asdict
from unittest.mock import Mock

import pytest

from lumibot.tools.metamask.auth import TokenManager
from lumibot.tools.metamask.owner import migrate_session
from lumibot.tools.metamask.provider import Credentials, MetaMaskError
from lumibot.tools.metamask_client import MetaMaskClient


def legacy(tmp_path):
    path = tmp_path / "session.json"
    path.write_text(
        json.dumps(
            {
                "schemaVersion": "1.0.0",
                "data": {"projectId": "project", "cliToken": "access", "cliRefreshToken": "refresh"},
            }
        )
    )
    os.chmod(path, 0o600)
    return path


def test_explicit_import_preserves_source_and_holds_old_service_fence(tmp_path):
    source = legacy(tmp_path)
    before = source.read_bytes()
    manager = migrate_session(source, tmp_path / "native.json")
    try:
        assert manager().access_token == "access"
        owner = sqlite3.connect(tmp_path / "session.owner.sqlite", timeout=0)
        with pytest.raises(sqlite3.OperationalError):
            owner.execute("BEGIN EXCLUSIVE")
        owner.close()
        assert source.read_bytes() == before
        assert (tmp_path / "native.json").stat().st_mode & 0o777 == 0o600
    finally:
        manager.close()
    owner = sqlite3.connect(tmp_path / "session.owner.sqlite", timeout=0)
    owner.execute("BEGIN EXCLUSIVE")
    owner.close()


def test_active_legacy_service_prevents_session_copy(tmp_path):
    source = legacy(tmp_path)
    owner = sqlite3.connect(tmp_path / "session.owner.sqlite")
    owner.executescript("PRAGMA journal_mode=DELETE; CREATE TABLE owner(id INTEGER); BEGIN EXCLUSIVE;")
    try:
        with pytest.raises(MetaMaskError, match="STATE_IN_USE"):
            migrate_session(source, tmp_path / "native.json")
        assert not (tmp_path / "native.json").exists()
    finally:
        owner.close()


def test_import_reuses_rotated_native_credentials_instead_of_old_pair(tmp_path):
    source = legacy(tmp_path)
    destination = tmp_path / "native.json"
    manager = TokenManager(Credentials("project", "rotated-access", "rotated-refresh"), path=destination)
    manager.close()
    restored = migrate_session(source, destination)
    try:
        assert asdict(restored())["refresh_token"] == "rotated-refresh"
    finally:
        restored.close()


def test_public_native_entry_point_never_invokes_a_subprocess(monkeypatch):
    monkeypatch.setattr(subprocess, "Popen", Mock(side_effect=AssertionError("External runtime invoked")))
    with MetaMaskClient({"NETWORK": "testnet"}) as client:
        client._venue = Mock()
        client._venue.price.return_value = 123
        assert client.price("BTC") == 123
        assert client._provider is None and client._journal is None
