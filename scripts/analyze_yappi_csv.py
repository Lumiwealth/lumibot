#!/usr/bin/env python3
"""
Analyze a YAPPI CSV (the LumiBot *_profile_yappi.csv artifact).

This is intentionally heuristic-based: it groups *self time* (`tsub_s`) by module path
patterns so we can quickly answer "is time mostly S3 hydration, compute, or artifacts?"

Important
---------
- We use `tsub_s` (self time) instead of `ttot_s` (inclusive time) because inclusive time
  double-counts heavily (call stacks).
- YAPPI records per-thread stats; production runs have multiple threads, so totals across
  all contexts can exceed wall-clock. We therefore report:
    1) overall `tsub_s` totals (useful for shifts)
    2) dominant context `tsub_s` totals (rough proxy for wall-clock distribution)
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Bucket:
    name: str
    patterns: tuple[re.Pattern, ...]


def _compile(patterns: Iterable[str]) -> tuple[re.Pattern, ...]:
    return tuple(re.compile(p) for p in patterns)


BUCKETS: list[Bucket] = [
    Bucket(
        name="s3_io",
        patterns=_compile(
            [
                r"boto3",
                r"botocore",
                r"s3transfer",
            ]
        ),
    ),
    Bucket(
        name="network_io",
        patterns=_compile(
            [
                r"lumibot/.*/data_downloader_queue_client\.py",
                r"requests/.*\.py",
                r"urllib3/.*\.py",
            ]
        ),
    ),
    Bucket(
        name="thetadata_provider",
        patterns=_compile(
            [
                r"lumibot/.*/thetadata_helper\.py",
                r"lumibot/.*/thetadata_backtesting_pandas\.py",
            ]
        ),
    ),
    Bucket(
        name="options_helper",
        patterns=_compile(
            [
                r"lumibot/.*/options_helper\.py",
            ]
        ),
    ),
    Bucket(
        name="artifacts",
        patterns=_compile(
            [
                r"quantstats",
                r"matplotlib",
                r"seaborn",
                r"plotly",
                r"lumibot/.*/tearsheet",
            ]
        ),
    ),
    Bucket(
        name="progress_logging",
        patterns=_compile(
            [
                r"logging/.*\.py",
                r"lumibot/.*/helpers\.py",
            ]
        ),
    ),
    Bucket(
        name="stdlib_wait",
        patterns=_compile(
            [
                r"/threading\.py",
                r"/queue\.py",
                r"/concurrent/.*\.py",
            ]
        ),
    ),
    Bucket(
        name="pandas_numpy",
        patterns=_compile(
            [
                r"pandas/.*\.py",
                r"numpy/.*\.py",
            ]
        ),
    ),
    Bucket(
        name="lumibot_other",
        patterns=_compile(
            [
                r"lumibot/.*\.py",
            ]
        ),
    ),
]


def _bucket_for(module: str) -> str:
    for bucket in BUCKETS:
        for pat in bucket.patterns:
            if pat.search(module):
                return bucket.name
    return "other"


def analyze(path: Path) -> dict[str, float]:
    totals: dict[str, float] = defaultdict(float)
    totals_by_ctx: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    total = 0.0
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            module = row.get("module") or ""
            try:
                tsub = float(row.get("tsub_s") or "0")
            except ValueError:
                tsub = 0.0
            ctx = row.get("ctx_name") or "unknown"
            total += tsub
            bucket = _bucket_for(module)
            totals[bucket] += tsub
            totals_by_ctx[ctx][bucket] += tsub
    totals["__total__"] = total

    # Add a "dominant context" view (highest self-time sum across buckets).
    ctx_totals = {ctx: sum(b.values()) for ctx, b in totals_by_ctx.items()}
    dominant_ctx = max(ctx_totals.items(), key=lambda kv: kv[1])[0] if ctx_totals else "unknown"
    dominant = totals_by_ctx.get(dominant_ctx, {})

    # Flatten dominant context totals with a prefix so JSON output can include both views.
    for bucket, value in dominant.items():
        totals[f"ctx:{dominant_ctx}:{bucket}"] = value
    totals["__dominant_ctx__"] = dominant_ctx
    totals["__dominant_total__"] = ctx_totals.get(dominant_ctx, 0.0)

    return dict(totals)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze LumiBot YAPPI CSV by rough buckets.")
    parser.add_argument("paths", nargs="+", help="One or more *_profile_yappi.csv files")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = parser.parse_args()

    results = {}
    for p in args.paths:
        path = Path(p).expanduser()
        totals = analyze(path)
        results[str(path)] = totals

    if args.json:
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    for file_path, totals in results.items():
        total = totals.get("__total__", 0.0) or 0.0
        dominant_ctx = totals.get("__dominant_ctx__", "unknown")
        dominant_total = totals.get("__dominant_total__", 0.0) or 0.0
        print(f"\n== {file_path} ==")
        print(f"dominant_ctx={dominant_ctx} dominant_ctx_tsub_s={dominant_total:.3f}s overall_tsub_s={total:.3f}s")
        for key, value in sorted(((k, v) for k, v in totals.items() if (not k.startswith('__') and not k.startswith('ctx:'))), key=lambda kv: kv[1], reverse=True):
            pct = (value / total * 100.0) if total else 0.0
            print(f"{key:20s} {value:10.3f}s  {pct:6.2f}%")

        # Dominant context breakdown
        print("\n-- dominant context breakdown (tsub_s) --")
        dom_prefix = f"ctx:{dominant_ctx}:"
        dom_items = [(k[len(dom_prefix):], v) for k, v in totals.items() if k.startswith(dom_prefix)]
        for key, value in sorted(dom_items, key=lambda kv: kv[1], reverse=True):
            pct = (value / dominant_total * 100.0) if dominant_total else 0.0
            print(f"{key:20s} {value:10.3f}s  {pct:6.2f}%")
        print(f"{'TOTAL':20s} {dominant_total:10.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
