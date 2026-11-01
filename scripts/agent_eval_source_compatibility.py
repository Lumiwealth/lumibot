"""Verify reuse of unchanged Pandas-fixture inference without rewriting its source.

The native receipt remains byte-identical. Its original full source hash is rebuilt
from Git objects, then every model-relevant byte is compared with the candidate.
Only exact live-provider modules, guarded live-sync method bodies unused by the
maintained Pandas fixtures, package version metadata, and AST-identical comments
in unexecuted examples are outside that comparison.
Any actor, judge, fixture, SDK, case, skill or shared trading change still expires
proof. This is compatibility verification, never new case freshness.
"""
from __future__ import annotations

import ast
import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROVIDER_ONLY_PATHS = frozenset({
    'lumibot/backtesting/routed_backtesting.py',
    'lumibot/backtesting/alpaca_backtesting.py',
    'lumibot/tools/ibkr_equity_basis.py',
    'lumibot/tools/ibkr_helper.py',
    'lumibot/tools/ibkr_history_health.py',
    # The native catalog constructs PandasDataBacktesting and BacktestingBroker,
    # never these live adapters. The fixture/binding contract tests that boundary.
    'lumibot/trading_builtins/ccxt_stream.py',
    'lumibot/brokers/bitunix.py',
    'lumibot/tools/bitunix_helpers.py',
})
NON_PACKAGE_PATHS = (
    'agent_eval_fixtures/research_data.json',
    'scripts/agent_eval_call_budget.py', 'scripts/agent_eval_subscription_budget.py',
    'scripts/agent_eval_rate_pacing.py', 'scripts/agent_eval_isolation.py',
    'scripts/agent_eval_production_fixture.py', 'scripts/agent_eval_research_server.py',
    'scripts/qualify_agent_evals.py', 'scripts/check_agent_judge_calibration.py',
    'tests/fixtures/agent_judge_calibration.json', 'requirements_agent_evals.txt',
    'requirements.txt', 'requirements_dev.txt', 'setup.py', 'scripts/run_agent_evals.py',
)
LIVE_METHOD_SCOPES = {
    'lumibot/brokers/broker.py': ('Broker', 'sync_orders', 'self.IS_BACKTESTING_BROKER'),
    'lumibot/strategies/strategy_executor.py': (
        'StrategyExecutor', 'sync_broker', 'self.broker.IS_BACKTESTING_BROKER'),
    'lumibot/brokers/alpaca.py': ('Alpaca', 'do_polling', None),
}


def _git(*args, root=ROOT, data=None):
    return subprocess.run(['git', *args], cwd=root, input=data, check=True,
                          capture_output=True, timeout=30).stdout


def runtime_paths(*, root=ROOT, source_ref=None):
    if source_ref:
        files = _git('ls-tree', '-r', '--name-only', '-z', source_ref, root=root).decode().split('\0')
    else:
        files = [p.relative_to(root).as_posix() for p in (root / 'lumibot').rglob('*') if p.is_file()]
    return sorted(set(NON_PACKAGE_PATHS) | {p for p in files if p.startswith('lumibot/') and (
        p.endswith('.py') or p.startswith('lumibot/components/agents/skills/'))})


def _blobs(ref, paths, *, root):
    requests = ''.join(f'{ref}:{path}\n' for path in paths).encode()
    output = _git('cat-file', '--batch', root=root, data=requests)
    values, offset = {}, 0
    for path in paths:
        end = output.index(b'\n', offset)
        header = output[offset:end].split()
        if len(header) != 3 or header[1] != b'blob':
            raise ValueError(f'Missing immutable qualification source: {path}')
        size = int(header[2])
        values[path] = output[end + 1:end + 1 + size]
        offset = end + 2 + size
    return values


def _fingerprint(blobs, versions):
    digest = hashlib.sha256()
    for path in sorted(blobs, key=Path):
        digest.update(path.encode())
        digest.update(b'\0')
        digest.update(blobs[path])
        digest.update(b'\0')
    sdk = json.dumps(versions, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256((digest.hexdigest() + sdk).encode()).hexdigest()


def portable_ast(value):
    """Ignore parser-version empty fields, preserving all meaningful AST data."""
    if isinstance(value, ast.AST):
        return {"node": type(value).__name__, **{
            key: portable_ast(item) for key, item in ast.iter_fields(value)
            if item is not None and not (isinstance(item, list) and not item)
        }}
    if isinstance(value, list):
        return [portable_ast(item) for item in value]
    return value


def entry_digest(node):
    return hashlib.sha256(json.dumps(portable_ast(node), sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()


def _without_unused_live_body(source, owner, method, guard):
    """Preserve exact bytes except a body that no native fixture can execute.

    Shared methods retain their signature, docstring, and unchanged immediate
    backtesting return. The catalog's real binding test proves every broker is a
    BacktestingBroker. Alpaca is never instantiated by any native fixture.
    """
    try:
        classes = [n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == owner]
        if len(classes) != 1:
            return None
        methods = [n for n in classes[0].body if isinstance(n, ast.FunctionDef) and n.name == method]
        if len(methods) != 1:
            return None
        fn = methods[0]
        body = fn.body
        prefix_end = fn.lineno
        if (isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            prefix_end = body[0].end_lineno
            body = body[1:]
        if guard:
            guards = [n for n in body if isinstance(n, ast.If)
                      and ast.dump(n.test) == ast.dump(ast.parse(guard, mode='eval').body)]
            if len(guards) != 1:
                return None
            first = guards[0]
            if not (len(first.body) == 1 and isinstance(first.body[0], ast.Return)
                    and first.body[0].value is None and not first.orelse):
                return None
            prefix_end = first.end_lineno
        elif fn.decorator_list:
            # No decorators exist on the unused adapter poller; don't allow a
            # newly decorated method to acquire import-time behavior.
            return None
        lines = source.splitlines(keepends=True)
        return b''.join(lines[:prefix_end]) + b'<unused-live-body>\n' + b''.join(lines[fn.end_lineno:])
    except (SyntaxError, IndexError, ValueError):
        return None


def check_change(path, before, after):
    if before == after or path in PROVIDER_ONLY_PATHS:
        return
    if path == 'setup.py':
        # Dependency/package changes remain incompatible; only the release
        # version field may differ. Actual model SDK versions are checked too.
        version = rb'version\s*=\s*([\'\"])[0-9]+\.[0-9]+\.[0-9]+\1'
        old, old_count = re.subn(version, b'version="<release>"', before)
        new, new_count = re.subn(version, b'version="<release>"', after)
        if old_count == new_count == 1 and old == new:
            return
    if path.startswith('lumibot/example_strategies/') and path.endswith('.py'):
        # The maintained catalog instantiates its own Pandas-fixture strategies,
        # never an example. Retain only comment/formatting edits whose complete
        # executable AST, including every docstring and prompt, is unchanged.
        try:
            old_tree = ast.parse(before, type_comments=True)
            new_tree = ast.parse(after, type_comments=True)
            if ast.dump(old_tree, include_attributes=False) == ast.dump(new_tree, include_attributes=False):
                return
        except (SyntaxError, ValueError, UnicodeError):
            pass
    if path in LIVE_METHOD_SCOPES:
        old = _without_unused_live_body(before, *LIVE_METHOD_SCOPES[path])
        new = _without_unused_live_body(after, *LIVE_METHOD_SCOPES[path])
        if old is not None and old == new:
            return
    if path == 'lumibot/strategies/_strategy.py':
        old = _without_alpaca_benchmark_body(before)
        new = _without_alpaca_benchmark_body(after)
        if old is not None and old == new:
            return
    raise ValueError(f'Source change affects model qualification: {path}')


def _without_alpaca_benchmark_body(source):
    """Exclude only reporting dispatched to the unused Alpaca provider.

    Keep the dispatch condition and every surrounding byte, including the
    Pandas/default branch. Native fixture construction proves this exact type
    condition is false; shared valuation/trading methods are never excluded.
    """
    try:
        owner = next(n for n in ast.parse(source).body
                     if isinstance(n, ast.ClassDef) and n.name == '_Strategy')
        method = next(n for n in owner.body
                      if isinstance(n, ast.FunctionDef) and n.name == '_dump_benchmark_stats')
        expected = ast.dump(ast.parse('type(self.broker.data_source) == AlpacaBacktesting', mode='eval').body)
        branches = [n for n in ast.walk(method) if isinstance(n, ast.If) and ast.dump(n.test) == expected]
        if len(branches) != 1 or not branches[0].body:
            return None
        branch = branches[0]
        lines = source.splitlines(keepends=True)
        return (b''.join(lines[:branch.body[0].lineno - 1])
                + b'<unused-alpaca-report-body>\n'
                + b''.join(lines[branch.body[-1].end_lineno:]))
    except (SyntaxError, StopIteration, IndexError, ValueError):
        return None


def compatible_runtime(receipt, receipt_bytes, anchor, *, root=ROOT):
    if (not isinstance(anchor, dict) or set(anchor) != {'version', 'source_commit', 'receipt_sha256'}
            or anchor.get('version') != 1):
        raise ValueError('Invalid source compatibility anchor')
    if anchor['receipt_sha256'] != hashlib.sha256(receipt_bytes).hexdigest():
        raise ValueError('Native receipt digest differs from its compatibility anchor')
    ref = anchor['source_commit']
    if not isinstance(ref, str) or not re.fullmatch('[0-9a-f]{40}', ref):
        raise ValueError('Compatibility requires an immutable full Git commit')
    _git('merge-base', '--is-ancestor', ref, 'HEAD', root=root)
    if _git('show', f'{ref}:ci/agent-evals/qualification.json', root=root) != receipt_bytes:
        raise ValueError('Native receipt differs from its immutable original source')
    original = _blobs(ref, runtime_paths(root=root, source_ref=ref), root=root)
    if _fingerprint(original, receipt['sdk_versions']) != receipt['runtime_fingerprint']:
        raise ValueError('Immutable source does not match the native inference fingerprint')
    # Check the complete file set as well as content, so new/deleted shared
    # modules cannot disappear from compatibility selection.
    current_paths = runtime_paths(root=root)
    if set(current_paths) != set(original):
        raise ValueError('Runtime file inventory changed; new native qualification required')
    for path, before in original.items():
        check_change(path, before, (root / path).read_bytes())
    return receipt['runtime_fingerprint']
