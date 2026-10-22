"""Native inference keeps its original source; unrelated provider edits may reuse it."""
import hashlib
import json
from pathlib import Path

import pytest

from scripts import agent_eval_source_compatibility as scope


def test_scope_rejects_an_agent_or_fixture_change():
    for path in ('lumibot/components/agents/runtime.py', 'scripts/run_agent_evals.py',
                 'scripts/agent_eval_production_fixture.py', 'lumibot/strategies/strategy.py',
                 'requirements_agent_evals.txt'):
        with pytest.raises(ValueError, match='affects model qualification'):
            scope.check_change(path, b'before', b'after')


def test_scope_accepts_only_unused_provider_modules_and_package_version():
    scope.check_change('lumibot/tools/ibkr_helper.py', b'before', b'after')
    scope.check_change('setup.py', b'version="4.6.23",\nrequires=[]', b'version="4.6.24",\nrequires=[]')
    with pytest.raises(ValueError):
        scope.check_change('setup.py', b'version="4.6.23",\nrequires=[]', b'version="4.6.24",\nrequires=["x"]')


def test_source_files_cover_the_existing_runtime_fingerprint(monkeypatch):
    from scripts import run_agent_evals as evals
    paths = []
    monkeypatch.setattr(evals, 'sha256_files', lambda values: paths.extend(values) or 'test')
    evals.runtime_fingerprint()
    assert set(scope.runtime_paths()) == {path.relative_to(evals.REPO_ROOT).as_posix() for path in paths}


def test_reconstructed_hash_preserves_path_order_and_sdk_identity():
    from scripts import run_agent_evals as evals
    blobs = {path: (scope.ROOT / path).read_bytes() for path in scope.runtime_paths()}
    assert scope._fingerprint(blobs, evals.sdk_versions()) == evals.runtime_fingerprint()


def test_validator_reuse_never_accepts_changed_native_validation():
    before = b'def validate_record():\n    return True\n\ndef main():\n    return 1\n'
    scope.check_change('scripts/qualify_agent_evals.py', before, before.replace(b'return 1', b'return 2'))
    with pytest.raises(ValueError):
        scope.check_change('scripts/qualify_agent_evals.py', before, before.replace(b'return True', b'return False'))


def test_compatibility_anchor_cannot_rebind_a_changed_native_receipt(tmp_path):
    receipt = {'runtime_fingerprint': 'old', 'sdk_versions': {}}
    anchor = {'version': 1, 'source_commit': 'a'*40, 'receipt_sha256': '0'*64}
    with pytest.raises(ValueError, match='receipt digest'):
        scope.compatible_runtime(receipt, json.dumps(receipt).encode(), anchor, root=tmp_path)
