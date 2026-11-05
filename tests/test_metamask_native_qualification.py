"""Live qualification cannot bypass its testnet boundary under python -O."""

import importlib
import json
import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.tools.metamask.journal import Journal
from scripts import qualify_metamask_native as qualification


def native_configs(tmp_path):
    return [
        {
            "NETWORK": "testnet",
            "WALLET_ADDRESS": "0x" + str(i + 1) * 40,
            "AGENT_ID": f"agent-{i}",
            "STATE_FILE": str(tmp_path / f"state-{i}.sqlite"),
            "SESSION_FILE": str(tmp_path / f"session-{i}.json"),
            "LEGACY_SESSION_FILE": str(tmp_path / f"legacy-{i}.json"),
        }
        for i in range(2)
    ]


@pytest.mark.parametrize("kind", ["config", "STATE_FILE", "SESSION_FILE", "LEGACY_SESSION_FILE", "receipt", "symlink"])
def test_native_qualification_rejects_evidence_collisions_without_wallet_or_file_changes(tmp_path, monkeypatch, kind):
    configs = native_configs(tmp_path)
    config = tmp_path / "config.json"
    config.write_text(json.dumps(configs))
    target = config if kind == "config" else Path(configs[1][kind]) if kind in configs[1] else tmp_path / kind
    if kind == "symlink":
        target.symlink_to(tmp_path / "future-session")
    elif kind != "config":
        target.write_text("synthetic recovery contents")
    before = {path: path.read_bytes() for path in tmp_path.iterdir() if path.is_file()}
    broker = Mock(side_effect=AssertionError("Evidence refusal must precede broker construction"))
    monkeypatch.setattr(qualification, "MetaMask", broker)
    monkeypatch.setattr(sys, "argv", ["qualify", "--config", str(config), "--evidence", str(target), "--execute"])
    with pytest.raises(ValueError, match="evidence path"):
        qualification.main()
    broker.assert_not_called()
    assert all(path.read_bytes() == contents for path, contents in before.items())
    if kind == "symlink":
        assert target.is_symlink() and not target.exists()


@pytest.mark.parametrize(
    "field,suffix",
    [
        ("STATE_FILE", ""),
        ("SESSION_FILE", ""),
        ("LEGACY_SESSION_FILE", ""),
        ("STATE_FILE", "-wal"),
        ("STATE_FILE", "-shm"),
        ("STATE_FILE", ".owner.sqlite"),
        ("SESSION_FILE", ".lock"),
    ],
)
def test_future_recovery_paths_are_not_evidence_destinations(tmp_path, monkeypatch, field, suffix):
    configs = native_configs(tmp_path)
    config = tmp_path / "config.json"
    config.write_text(json.dumps(configs))
    target = Path(configs[1][field] + suffix)
    monkeypatch.setattr(qualification, "MetaMask", Mock(side_effect=AssertionError("No broker construction")))
    monkeypatch.setattr(sys, "argv", ["qualify", "--config", str(config), "--evidence", str(target), "--execute"])
    with pytest.raises(ValueError, match="evidence path"):
        qualification.main()
    assert not target.exists()


def test_native_qualification_keeps_closing_budget_before_any_submission(tmp_path, monkeypatch):
    configs = native_configs(tmp_path)
    config = tmp_path / "config.json"
    config.write_text(json.dumps(configs))
    now = int(time.time() * 1000)
    journal = Journal(Path(configs[0]["STATE_FILE"]))
    binding = {
        "id": "agent-0",
        "wallet": configs[0]["WALLET_ADDRESS"],
        "network": "testnet",
        "enabled": True,
        "expiresAt": now + 3_600_000,
        "policyDigest": "a" * 64,
        "markets": ["BTC", "ETH"],
        "chains": [421614],
        "assets": [],
        "maxOrderUsd": "25",
        "maxDailyUsd": "300",
        "maxLeverage": 1,
        "maxSlippageBps": 50,
    }
    journal.bind(binding)
    try:
        for i in range(11):
            operation, _ = journal.reserve(
                "agent-0",
                {"id": f"prior-{i}", "kind": "perp"},
                "25" if i < 10 else "19.50",
                expected_binding=binding,
                now=now,
            )
            journal.save({**operation, "state": "canceled"})
        before = journal.operations("agent-0")
        broker = Mock()
        broker.client.verify.return_value = {"wallet": configs[0]["WALLET_ADDRESS"]}
        broker.client.journal = journal
        broker.client.account.return_value = {"positions": []}
        broker.client.venue.info.return_value = []
        broker.client.price.return_value = Decimal("20000")
        broker.client.venue.market.return_value = SimpleNamespace(size_decimals=5)
        broker.submit_order.side_effect = AssertionError("Would submit before preserving closing capacity")
        monkeypatch.setattr(qualification, "MetaMask", Mock(return_value=broker))
        monkeypatch.setattr(
            sys, "argv", ["qualify", "--config", str(config), "--evidence", str(tmp_path / "receipt.json"), "--execute"]
        )
        with pytest.raises(RuntimeError, match="Daily budget"):
            qualification.main()
        broker.submit_order.assert_not_called()
        broker.client.provider.submit_signature.assert_not_called()
        assert journal.operations("agent-0") == before and journal.binding("agent-0") == binding
    finally:
        journal.close()


@pytest.mark.parametrize("room", ["74.99", "75"])
def test_native_three_order_budget_boundary(room):
    journal = Mock()
    journal.operations.return_value = []
    if room == "75":
        assert (
            qualification.cleanup_budget_room(journal, "agent", {"maxDailyUsd": room}, 2000, required_usd=Decimal("75"))
            == 75
        )
    else:
        with pytest.raises(RuntimeError, match="Daily budget"):
            qualification.cleanup_budget_room(journal, "agent", {"maxDailyUsd": room}, 2000, required_usd=Decimal("75"))


def test_fresh_evidence_is_private_and_snapshots_remain_readable(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("[]")
    path = qualification.prepare_evidence_path(tmp_path / "receipts" / "run.json", config, native_configs(tmp_path))
    assert path.stat().st_mode & 0o077 == 0
    for snapshot in ({"complete": False}, {"complete": True}):
        qualification.save_evidence(path, snapshot)
        assert json.loads(path.read_text()) == snapshot and path.stat().st_mode & 0o077 == 0


def test_evidence_reservation_cannot_overwrite_a_file_created_after_preflight(tmp_path, monkeypatch):
    config, target = tmp_path / "config.json", tmp_path / "receipt.json"
    config.write_text("[]")
    original_open = os.open

    def race(path, flags, mode):
        if Path(path) == target:
            target.write_text("another run's receipt")
        return original_open(path, flags, mode)

    monkeypatch.setattr("scripts.qualify_metamask_partial.os.open", race)
    with pytest.raises(FileExistsError):
        qualification.prepare_evidence_path(target, config, native_configs(tmp_path))
    assert target.read_text() == "another run's receipt"


@pytest.mark.parametrize("script", ["native", "partial", "strategy"])
def test_all_qualification_entrypoints_protect_future_wallet_sidecars(tmp_path, monkeypatch, script):
    module = importlib.import_module(f"scripts.qualify_metamask_{script}")
    configs = native_configs(tmp_path)
    for field in ("STATE_FILE", "SESSION_FILE", "LEGACY_SESSION_FILE"):
        configs[1][field] = configs[0][field]
    config = tmp_path / "config.json"
    config.write_text(json.dumps(configs))
    target = Path(configs[0]["STATE_FILE"] + "-wal")
    broker = Mock(side_effect=AssertionError("No wallet use before destination validation"))
    monkeypatch.setattr(module, "MetaMask", broker)
    monkeypatch.setattr(sys, "argv", ["qualify", "--config", str(config), "--evidence", str(target), "--execute"])
    with pytest.raises(ValueError, match="evidence path"):
        module.main()
    broker.assert_not_called()
    assert not target.exists()


@pytest.mark.parametrize("network,distinct", [("mainnet", True), ("testnet", False)])
def test_qualification_rejects_unsafe_configs_before_evidence_or_wallet_use(tmp_path, network, distinct):
    configs = [
        {"NETWORK": network, "WALLET_ADDRESS": "0x" + "1" * 40},
        {"NETWORK": network, "WALLET_ADDRESS": "0x" + ("2" if distinct else "1") * 40},
    ]
    path = tmp_path / "configs.json"
    path.write_text(json.dumps(configs))
    evidence = tmp_path / "evidence.json"
    root = Path(__file__).resolve().parents[1]
    # Module execution resolves the checkout in CI without relying on an editable install.
    result = subprocess.run(
        [
            sys.executable,
            "-O",
            "-m",
            "scripts.qualify_metamask_native",
            "--config",
            str(path),
            "--evidence",
            str(evidence),
            "--execute",
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert ("mainnet is forbidden" if network == "mainnet" else "distinct dedicated wallets") in result.stderr
    assert not evidence.exists()
