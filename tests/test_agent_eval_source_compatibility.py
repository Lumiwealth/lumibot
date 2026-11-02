"""Native inference keeps its original source; unrelated provider edits may reuse it."""
import ast
import json

import pytest

from scripts import agent_eval_source_compatibility as scope


LIVE_METHODS = [
    ('lumibot/brokers/broker.py', 'Broker', 'sync_orders', 'self.IS_BACKTESTING_BROKER'),
    ('lumibot/strategies/strategy_executor.py', 'StrategyExecutor', 'sync_broker', 'self.broker.IS_BACKTESTING_BROKER'),
    ('lumibot/brokers/alpaca.py', 'Alpaca', 'do_polling', None),
]


def test_alpaca_only_benchmark_branch_preserves_pandas_inference():
    path = 'lumibot/strategies/_strategy.py'
    source = (scope.ROOT / path).read_bytes()
    branch = b'elif type(self.broker.data_source) == AlpacaBacktesting:'
    modified = source.replace(branch, branch + b'\n                pass  # report-only change')
    scope.check_change(path, source, modified)
    # An altered dispatch condition, another source, or shared accounting must
    # still invalidate inference. Only the unreachable branch BODY is exempt.
    for before, after in [
        (branch, b'elif True:'),
        (b'def _update_portfolio_value(self):', b'def _update_portfolio_value(self, changed=False):'),
    ]:
        with pytest.raises(ValueError, match='affects model qualification'):
            scope.check_change(path, source, source.replace(before, after))
    with pytest.raises(ValueError, match='affects model qualification'):
        scope.check_change(path, source, source + b'\nraise RuntimeError("changed")\n')


def test_alpaca_provider_is_outside_the_native_pandas_fixture():
    scope.check_change('lumibot/backtesting/alpaca_backtesting.py', b'before', b'after')


@pytest.mark.parametrize('path,owner,method,guard', LIVE_METHODS)
def test_unused_live_method_body_changes_preserve_native_inference(path, owner, method, guard):
    source = (scope.ROOT / path).read_bytes()
    cls = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == owner)
    fn = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == method)
    lines = source.splitlines(keepends=True)
    lines.insert(fn.end_lineno, b'        pass  # native live-path regression proof\n')
    scope.check_change(path, source, b''.join(lines))


@pytest.mark.parametrize('path,owner,method,guard', LIVE_METHODS)
def test_live_body_scope_preserves_every_other_byte_and_signature(path, owner, method, guard):
    source = (scope.ROOT / path).read_bytes()
    for modified in [source + b'\nraise RuntimeError("shared change")\n',
                     source.replace(('def ' + method + '(').encode(), ('def changed_' + method + '(').encode(), 1),
                     b'import unqualified_dependency\n' + source]:
        with pytest.raises(ValueError, match='affects model qualification'):
            scope.check_change(path, source, modified)
    if guard:
        for replacement in ('not ' + guard, 'True'):
            modified = source.replace(('if ' + guard + ':').encode(), ('if ' + replacement + ':').encode(), 1)
            assert modified != source
            with pytest.raises(ValueError, match='affects model qualification'):
                scope.check_change(path, source, modified)


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
                assert not any(
                    owner.__module__.startswith('lumibot.example_strategies')
                    for owner in type(fixture.strategy).__mro__
                ), name
                assert fixture.broker.IS_BACKTESTING_BROKER is True, name
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


def test_refreshed_anchor_requires_fresh_inference_for_shared_executor_changes():
    path = 'lumibot/strategies/strategy_executor.py'
    current = (scope.ROOT / path).read_bytes()
    scope.check_change(path, current, current)
    with pytest.raises(ValueError, match='affects model qualification'):
        scope.check_change(path, b'unreviewed prior executor', current)
    with pytest.raises(ValueError, match='affects model qualification'):
        scope.check_change(path, current, current + b'\nraise RuntimeError("changed")\n')


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


def test_refreshed_anchor_rejects_validator_entry_changes():
    import ast
    source = (scope.ROOT / 'scripts/qualify_agent_evals.py').read_bytes()
    main = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == 'main')
    assert scope.entry_digest(main)
    scope.check_change('scripts/qualify_agent_evals.py', source, source)
    early_success = source.replace(b'    cases, runtime_hash, versions =', b'    return 0\n    cases, runtime_hash, versions =')
    with pytest.raises(ValueError):
        scope.check_change('scripts/qualify_agent_evals.py', source, early_success)


def test_compatibility_anchor_cannot_rebind_a_changed_native_receipt(tmp_path):
    receipt = {'runtime_fingerprint': 'old', 'sdk_versions': {}}
    anchor = {'version': 1, 'source_commit': 'a'*40, 'receipt_sha256': '0'*64}
    with pytest.raises(ValueError, match='receipt digest'):
        scope.compatible_runtime(receipt, json.dumps(receipt).encode(), anchor, root=tmp_path)


def test_entry_digest_ignores_new_empty_ast_fields_only():
    import ast
    node = ast.parse("def main():\n    return True\n").body[0]
    baseline = scope.entry_digest(node)
    node._fields = tuple(field for field in node._fields if field != "type_params") + ("type_params",)
    node.type_params = []
    assert scope.entry_digest(node) == baseline
    node.body[0].value = ast.Constant(value=False)
    assert scope.entry_digest(node) != baseline


def test_example_comment_only_documentation_preserves_native_source():
    source = b'"""Reference example."""\nPROMPT = "Observe the account."\ndef run():\n    return 3\n'
    documented = b'# Run this example: https://example.test/strategy\n' + source
    scope.check_change('lumibot/example_strategies/reference.py', source, documented)


@pytest.mark.parametrize('before,after', [
    (b'Reference example.', b'Changed documentation string.'),
    (b'Observe the account.', b'Buy an asset.'),
    (b'return 3', b'return 4'),
])
def test_example_compatibility_keeps_docstrings_prompts_and_code_bound(before, after):
    source = b'"""Reference example."""\nPROMPT = "Observe the account."\ndef run():\n    return 3\n'
    with pytest.raises(ValueError, match='affects model qualification'):
        scope.check_change('lumibot/example_strategies/reference.py', source, source.replace(before, after))
    with pytest.raises(ValueError, match='affects model qualification'):
        scope.check_change('lumibot/example_strategies/reference.py', source, b'import unqualified_dependency\n' + source)


def test_example_comment_compatibility_does_not_exempt_shared_runtime():
    source = b'def run():\n    return 3\n'
    with pytest.raises(ValueError, match='affects model qualification'):
        scope.check_change('lumibot/components/agents/runtime.py', source, b'# documentation\n' + source)
