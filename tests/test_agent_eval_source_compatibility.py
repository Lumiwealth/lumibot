"""Native inference keeps its original source; unrelated provider edits may reuse it."""
import ast
import hashlib
import json

import pytest

from scripts import agent_eval_source_compatibility as scope

def _author_retention_source(tmp_path, monkeypatch):
    """Real Git bytes and synthetic contract metadata, never model evidence."""
    native = tmp_path / 'lumibot/native.py'
    native.parent.mkdir()
    native.write_bytes(b'VALUE = 1\n')
    paths = ['lumibot/native.py']
    original_hash = scope._fingerprint({paths[0]: native.read_bytes()}, {})
    receipt = {'sdk_versions': {}, 'runtime_fingerprint': original_hash, 'cases': {'contract': {}}}
    raw = json.dumps(receipt).encode()
    stored = tmp_path / 'ci/agent-evals/qualification.json'
    stored.parent.mkdir(parents=True)
    stored.write_bytes(raw)
    scope._git('init', '-q', '-b', 'native-proof', root=tmp_path)
    scope._git('add', 'lumibot/native.py', 'ci/agent-evals/qualification.json', root=tmp_path)
    scope._git('-c', 'user.name=Native Contract', '-c', 'user.email=native@example.invalid',
               'commit', '-qm', 'Record synthetic source contract', root=tmp_path)
    ref = scope._git('rev-parse', 'HEAD', root=tmp_path).decode().strip()
    (native.parent / 'diagnostics.py').write_bytes(b'COUNTER = 0\n')
    monkeypatch.setattr(scope, 'runtime_paths', lambda *, root, source_ref=None:
                        paths if source_ref else sorted(p.relative_to(root).as_posix()
                                                       for p in (root / 'lumibot').glob('*.py')))
    current = {path: (tmp_path / path).read_bytes() for path in scope.runtime_paths(root=tmp_path)}
    anchor = {'version': 2, 'source_commit': ref, 'receipt_sha256': hashlib.sha256(raw).hexdigest(),
              'author_review': {'candidate_runtime_fingerprint': scope._fingerprint(current, {}),
                                'case_ids': ['contract'],
                                'reason': 'Only diagnostic counters changed; retained actor behavior is unchanged.'}}
    return receipt, raw, anchor


def test_author_review_retains_exact_native_source_without_inferring_new_runs(tmp_path, monkeypatch):
    receipt, raw, anchor = _author_retention_source(tmp_path, monkeypatch)
    assert scope.compatible_runtime(receipt, raw, anchor, root=tmp_path) == receipt['runtime_fingerprint']
    assert (tmp_path / 'ci/agent-evals/qualification.json').read_bytes() == raw


def test_component_receipt_retains_distinct_original_and_publication_commits(tmp_path, monkeypatch):
    receipt, raw, anchor = _author_retention_source(tmp_path, monkeypatch)
    path = 'ci/agent-evals/component-qualification.json'
    (tmp_path / path).write_bytes(raw)
    scope._git('add', path, 'lumibot/diagnostics.py', root=tmp_path)
    scope._git('-c', 'user.name=Native Contract', '-c', 'user.email=native@example.invalid',
               'commit', '-qm', 'Publish component evidence after unrelated diagnostics', root=tmp_path)
    published = scope._git('rev-parse', 'HEAD', root=tmp_path).decode().strip()
    anchor.update(version=3, receipt_commit=published, receipt_path=path)
    assert published != anchor['source_commit']
    assert scope.compatible_runtime(receipt, raw, anchor, root=tmp_path) == receipt['runtime_fingerprint']
    # A matching runtime declaration never permits different published receipt bytes.
    (tmp_path / path).write_bytes(raw + b'\n')
    anchor['receipt_sha256'] = hashlib.sha256(raw + b'\n').hexdigest()
    with pytest.raises(ValueError, match='immutable'):
        scope.compatible_runtime(receipt, raw + b'\n', anchor, root=tmp_path)


@pytest.mark.parametrize('field,value', [
    ('candidate_runtime_fingerprint', '0' * 64), ('case_ids', []), ('reason', ''), ('unexpected', True),
])
def test_author_review_rejects_wrong_candidate_cases_or_missing_reason(tmp_path, monkeypatch, field, value):
    receipt, raw, anchor = _author_retention_source(tmp_path, monkeypatch)
    anchor['author_review'][field] = value
    with pytest.raises(ValueError, match='author|Author'):
        scope.compatible_runtime(receipt, raw, anchor, root=tmp_path)


def test_legacy_anchor_requires_author_review_after_source_changes(tmp_path, monkeypatch):
    receipt, raw, anchor = _author_retention_source(tmp_path, monkeypatch)
    anchor.pop('author_review')
    anchor['version'] = 1
    with pytest.raises(ValueError, match='[Aa]uthor'):
        scope.compatible_runtime(receipt, raw, anchor, root=tmp_path)
    (tmp_path / 'lumibot/diagnostics.py').unlink()
    assert scope.compatible_runtime(receipt, raw, anchor, root=tmp_path) == receipt['runtime_fingerprint']


LIVE_METHODS = [
    ('lumibot/brokers/broker.py', 'Broker', 'sync_orders', 'self.IS_BACKTESTING_BROKER'),
    ('lumibot/strategies/strategy_executor.py', 'StrategyExecutor', 'sync_broker', 'self.broker.IS_BACKTESTING_BROKER'),
    ('lumibot/brokers/alpaca.py', 'Alpaca', 'do_polling', None),
    *[('lumibot/tools/projectx_helpers.py', 'ProjectXStreaming', method, None)
      for method in ('__init__', '_on_market_hub_open', '_on_market_hub_close', 'get_market_quote')],
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


def test_unused_schwab_adapter_keeps_synchronous_fixture_proof(monkeypatch):
    scope.check_change('lumibot/brokers/schwab.py', b'before', b'after')

    from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
    from lumibot.brokers.schwab import Schwab
    from lumibot.entities import Asset
    from scripts import run_agent_evals as evals
    from scripts.agent_eval_isolation import fixture_network_boundary
    from scripts.agent_eval_production_fixture import ProductionFixture

    def forbid_live_broker(*_args, **_kwargs):
        pytest.fail('Synchronous native fixtures must not construct Schwab')

    monkeypatch.setattr(Schwab, '__init__', forbid_live_broker)
    with fixture_network_boundary():
        fixture = ProductionFixture(evals.build_fixture('flat_stock_account'))
        try:
            assert isinstance(fixture.broker, BacktestingBroker)
            assert isinstance(fixture.broker.data_source, PandasDataBacktesting)
            fixture.create_agent(evals.load_cases({'stock_price_before_order'})[0], None)._ensure_bound_tools()
            assert fixture.strategy.get_quote(Asset('AAPL')) is not None
        finally:
            fixture.close()


@pytest.mark.parametrize('path', ['lumibot/tools/shared_bar_cache.py', 'lumibot/traders/trader.py'])
def test_unused_cache_and_threaded_runner_keep_synchronous_fixture_proof(path):
    scope.check_change(path, b'before', b'after')


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


def test_projectx_scope_preserves_quote_parser_authentication_and_order_handoff():
    path = 'lumibot/tools/projectx_helpers.py'
    source = (scope.ROOT / path).read_bytes()
    for before, after in (
        (b'prior.update(payload)', b'prior.clear()'),
        (b'def _access_token(self):', b'def changed_access_token(self):'),
        (b'def order_place(', b'def changed_order_place('),
        (b'def get_market_quote(self, contract_id: str, timeout: float = 10):',
         b'def get_market_quote(\n        self, contract_id: str, timeout: float = 11\n    ):'),
    ):
        modified = source.replace(before, after, 1)
        assert modified != source
        with pytest.raises(ValueError, match='affects model qualification'):
            scope.check_change(path, source, modified)


def test_scope_accepts_only_unused_provider_modules_and_package_version():
    scope.check_change('lumibot/tools/ibkr_helper.py', b'before', b'after')
    for path in ('lumibot/trading_builtins/ccxt_stream.py', 'lumibot/brokers/bitunix.py',
                 'lumibot/tools/bitunix_helpers.py'):
        scope.check_change(path, b'before', b'after')
    scope.check_change('setup.py', b'version="4.6.23",\nrequires=[]', b'version="4.6.24",\nrequires=[]')
    with pytest.raises(ValueError):
        scope.check_change('setup.py', b'version="4.6.23",\nrequires=[]', b'version="4.6.24",\nrequires=["x"]')


def test_native_catalog_bindings_use_pandas_backtesting_not_live_provider_adapters():
    from unittest.mock import patch

    from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
    from lumibot.entities import Asset
    from scripts import run_agent_evals as evals
    from scripts.agent_eval_isolation import fixture_network_boundary
    from scripts.agent_eval_production_fixture import ProductionFixture

    cases = {case['fixture']: case for case in evals.load_cases()}
    # The real-model fixtures must never enter the live stream owner whose
    # bootstrap-only bodies are outside their source-compatibility comparison.
    observed_quotes = []
    with fixture_network_boundary(), patch('lumibot.tools.projectx_helpers.ProjectXStreaming',
            side_effect=AssertionError('Native model fixtures cannot construct a live ProjectX stream')), \
            patch('lumibot.traders.Trader.run_all', side_effect=AssertionError('Fixtures run synchronously')), \
            patch('lumibot.tools.shared_bar_cache.read_bars', side_effect=AssertionError('Fixture bars are in memory')), \
            patch('lumibot.tools.shared_bar_cache.read_bars_many', side_effect=AssertionError('Fixture bars are in memory')):
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
                observed_quotes.append(fixture.strategy.get_quote(Asset('AAPL')))
            finally:
                fixture.close()
    assert any(quote is not None for quote in observed_quotes)


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


def test_unexecuted_reference_agent_literal_policy_keeps_shared_runtime_proof():
    source = b'class Reference:\n    def initialize(self):\n        self.agents.create(name="reader", allow_trading=False, system_prompt="Read the source inventory.")\n'
    revised = source.replace(b'Read the source inventory.', b'Account for each source transaction in the inventory.')
    scope.check_change('lumibot/example_strategies/reference.py', source, revised)


@pytest.mark.parametrize('before,after', [
    (b'allow_trading=False', b'allow_trading=True'),
    (b'name="reader"', b'name="executor"'),
    (b'system_prompt="Read the source inventory."', b'system_prompt=fetch_policy()'),
    (b'system_prompt="Read the source inventory."', b'system_prompt=f"{fetch_policy()}"'),
    (b'def initialize(self):', b'def initialize(self):\n        fetch_data()'),
])
def test_reference_policy_compatibility_keeps_executable_connections_bound(before, after):
    source = b'class Reference:\n    def initialize(self):\n        self.agents.create(name="reader", allow_trading=False, system_prompt="Read the source inventory.")\n'
    with pytest.raises(ValueError, match='affects model qualification'):
        scope.check_change('lumibot/example_strategies/reference.py', source, source.replace(before, after))


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


def test_development_worker_declaration_preserves_direct_native_inference():
    before = b"-r requirements.txt\npytest\ncoverage\n"
    after = b"-r requirements.txt\npytest\npytest-xdist==3.8.0\ncoverage\n"
    scope.check_change('requirements_dev.txt', before, after)


@pytest.mark.parametrize('extra', [
    b'litellm==9.0.0\n',
    b'pytest-xdist @ https://example.test/unqualified.whl\n',
    b'pytest-xdist==3.8.0; python_version >= "3.10"\n',
    b'pytest-xdist[unqualified]==3.8.0\n',
    b'-r unqualified.txt\n',
])
def test_development_worker_scope_rejects_other_dependency_changes(extra):
    before = b"-r requirements.txt\npytest\n"
    with pytest.raises(ValueError, match='affects model qualification'):
        scope.check_change('requirements_dev.txt', before, before + extra)


def test_unused_report_directory_creation_preserves_native_inference_only():
    path = 'lumibot/strategies/_strategy.py'
    source = (scope.ROOT / path).read_bytes()
    addition = b'        os.makedirs(logdir, exist_ok=True)\n'
    assert source.count(addition) == 1
    before = source.replace(addition, b'', 1)
    scope.check_change(path, before, source)
    for changed in (
        source.replace(addition, b'        os.makedirs(logdir, exist_ok=False)\n'),
        source.replace(addition, b'        os.makedirs(other_directory, exist_ok=True)\n'),
        source.replace(b'if not self._analyze_backtest:', b'if False:', 1),
        source.replace(b'def _update_portfolio_value(self):', b'def _update_portfolio_value(self, changed=False):', 1),
    ):
        assert changed != source
        with pytest.raises(ValueError, match='affects model qualification'):
            scope.check_change(path, before, changed)


def test_every_native_fixture_keeps_report_directory_creation_unreachable():
    from unittest.mock import patch

    from scripts import run_agent_evals as evals
    from scripts.agent_eval_isolation import fixture_network_boundary
    from scripts.agent_eval_production_fixture import ProductionFixture
    cases = {case['fixture']: case for case in evals.load_cases()}
    with fixture_network_boundary():
        for name, case in cases.items():
            fixture = ProductionFixture(evals.build_fixture(name))
            try:
                assert fixture.strategy._analyze_backtest is False, name
                fixture.create_agent(case, None)._ensure_bound_tools()
                with patch('lumibot.strategies._strategy.os.makedirs',
                           side_effect=AssertionError('Native inference cannot create report directories')):
                    assert fixture.strategy.backtest_analysis(logdir='unused-report-output') is None
            finally:
                fixture.close()
