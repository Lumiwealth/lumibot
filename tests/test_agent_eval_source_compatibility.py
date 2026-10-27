"""Native inference keeps its original source; unrelated provider edits may reuse it."""
import hashlib
import json

import pytest

from scripts import agent_eval_source_compatibility as scope


def test_scope_rejects_an_agent_or_fixture_change():
    for path in ('lumibot/components/agents/runtime.py', 'scripts/run_agent_evals.py',
                 'scripts/agent_eval_production_fixture.py', 'lumibot/strategies/strategy.py',
                 'lumibot/backtesting/backtesting_broker.py', 'lumibot/data_sources/pandas_data.py',
                 'lumibot/components/agents/builtins.py', 'tests/fixtures/agent_judge_calibration.json',
                 'requirements_agent_evals.txt'):
        with pytest.raises(ValueError, match='affects model qualification'):
            scope.check_change(path, b'before', b'after')


def test_scope_accepts_only_unused_provider_modules_and_package_version():
    scope.check_change('lumibot/tools/ibkr_helper.py', b'before', b'after')
    for path in ('lumibot/trading_builtins/ccxt_stream.py', 'lumibot/brokers/bitunix.py',
                 'lumibot/tools/bitunix_helpers.py'):
        scope.check_change(path, b'before', b'after')
    scope.check_change('setup.py', b'version="4.6.23",\nrequires=[]', b'version="4.6.24",\nrequires=[]')
    with pytest.raises(ValueError):
        scope.check_change('setup.py', b'version="4.6.23",\nrequires=[]', b'version="4.6.24",\nrequires=["x"]')


def test_native_catalog_bindings_use_pandas_backtesting_not_live_provider_adapters():
    from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
    from scripts import run_agent_evals as evals
    from scripts.agent_eval_isolation import fixture_network_boundary
    from scripts.agent_eval_production_fixture import ProductionFixture

    cases = {case['fixture']: case for case in evals.load_cases()}
    with fixture_network_boundary():
        for name, case in cases.items():
            fixture = ProductionFixture(evals.build_fixture(name))
            try:
                assert isinstance(fixture.broker, BacktestingBroker), name
                assert isinstance(fixture.broker.data_source, PandasDataBacktesting), name
                fixture.create_agent(case, None)._ensure_bound_tools()
            finally:
                fixture.close()


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
    with pytest.raises(ValueError):
        scope.check_change('scripts/qualify_agent_evals.py', before, before.replace(b'return 1', b'return 2'))
    with pytest.raises(ValueError):
        scope.check_change('scripts/qualify_agent_evals.py', before, before.replace(b'return True', b'return False'))


def test_fresh_anchor_cannot_retain_an_obsolete_validator_entry_exception(monkeypatch):
    import ast
    before = b'def verify():\n    return True\n\ndef main():\n    return 1\n'
    after = before.replace(b'return 1', b'return 0')
    entries = [next(node for node in ast.parse(source).body
                    if isinstance(node, ast.FunctionDef) and node.name == 'main')
               for source in (before, after)]
    pair = tuple(hashlib.sha256(ast.dump(node).encode()).hexdigest() for node in entries)
    if hasattr(scope, 'entry_digest'):
        pair = tuple(scope.entry_digest(node) for node in entries)
    monkeypatch.setattr(scope, 'VALIDATOR_ENTRY_COMPATIBILITY', pair, raising=False)
    with pytest.raises(ValueError, match='affects model qualification'):
        scope.check_change('scripts/qualify_agent_evals.py', before, after)


def test_compatibility_anchor_cannot_rebind_a_changed_native_receipt(tmp_path):
    receipt = {'runtime_fingerprint': 'old', 'sdk_versions': {}}
    anchor = {'version': 1, 'source_commit': 'a'*40, 'receipt_sha256': '0'*64}
    with pytest.raises(ValueError, match='receipt digest'):
        scope.compatible_runtime(receipt, json.dumps(receipt).encode(), anchor, root=tmp_path)


def test_current_validator_reuse_requires_unchanged_bytes():
    source = (scope.ROOT / 'scripts/qualify_agent_evals.py').read_bytes()
    scope.check_change('scripts/qualify_agent_evals.py', source, source)
    for changed in (source + b'\n', source.replace(b'def main():', b'def main(unused=None):')):
        with pytest.raises(ValueError, match='affects model qualification'):
            scope.check_change('scripts/qualify_agent_evals.py', source, changed)
