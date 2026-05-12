from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def test_acceptance_strategies_manifest_matches_repo_files() -> None:
    """
    These demo scripts must remain verbatim copies of Strategy Library/Demos.

    We enforce this two ways:
    1) Always (CI + local): a sha256 manifest over the checked-in copies.
    2) Locally (best-effort): compare against the Strategy Library Demos directory if present.
    """
    repo_root = Path(__file__).resolve().parents[2]
    strategies_dir = repo_root / "tests" / "backtest" / "acceptance_strategies"
    manifest_path = strategies_dir / "manifest.json"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest.get("schema_version") == 1

    entries = manifest.get("files") or []
    expected = {e["filename"]: e["sha256"] for e in entries}
    assert expected, f"{manifest_path} is empty"

    actual_py_files = sorted(p.name for p in strategies_dir.glob("*.py"))
    # `acceptance_strategies/` contains:
    # - verbatim Strategy Library demo copies (covered by this manifest), and
    # - additional platform-specific acceptance strategies (e.g., IBKR/crypto/futures) that do
    #   not exist in Strategy Library/Demos.
    missing = sorted(set(expected.keys()) - set(actual_py_files))
    assert not missing, f"Missing manifest-tracked acceptance strategies: {missing}"

    for filename, sha in expected.items():
        path = strategies_dir / filename
        assert _sha256(path) == sha

    demos_dir = (
        os.environ.get("STRATEGY_LIBRARY_DEMOS_DIR")
        or "/Users/robertgrzesik/Documents/Development/Strategy Library/Demos"
    )
    demos_path = Path(demos_dir)
    if demos_path.exists():
        for filename, sha in expected.items():
            demo_file = demos_path / filename
            assert demo_file.exists(), f"Missing Strategy Library demo file: {demo_file}"
            assert _sha256(demo_file) == sha, f"Strategy Library demo differs for {filename}"
    # If Strategy Library isn't present (e.g., GitHub CI), the repo-only manifest check above is still enforced.
