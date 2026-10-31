"""Verify reuse of unchanged Pandas-fixture inference without rewriting its source.

The native receipt remains byte-identical. Its original full source hash is rebuilt
from Git objects, then every model-relevant byte is compared with the candidate.
Only exact live-provider modules unused by the maintained Pandas fixtures, package
version metadata are outside that comparison.
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


def _git(*args, root=ROOT, data=None):
    return subprocess.run(['git', *args], cwd=root, input=data, check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30).stdout


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
    raise ValueError(f'Source change affects model qualification: {path}')


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
