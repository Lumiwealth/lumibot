"""Live qualification cannot bypass its testnet boundary under python -O."""

import json
import subprocess
import sys
from pathlib import Path

import pytest


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
