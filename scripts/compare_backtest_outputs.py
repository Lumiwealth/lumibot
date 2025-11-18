#!/usr/bin/env python3
"""
Compare two backtest output directories (typically dev vs feature) while ignoring
known volatile columns (UUID-based identifiers, run ids, etc.). The script exits
with a non-zero status code if any comparison fails so it can guard parity checks.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

VOLATILE_COLUMNS = {"identifier", "strategy_run_id", "order_id"}
TEXT_ONLY_SUFFIXES = ("logs.csv",)


def _locate_file(directory: Path, filename: str) -> Path:
    candidate = directory / filename
    if candidate.exists():
        return candidate
    matches = sorted(directory.glob(f"*{filename}"))
    if not matches:
        raise FileNotFoundError(f"Missing file matching {filename!r} in {directory}")
    if len(matches) > 1:
        raise FileExistsError(
            f"Multiple candidates for {filename!r} in {directory}: "
            + ", ".join(str(match.name) for match in matches)
        )
    return matches[0]


def _load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    drop_cols = [col for col in VOLATILE_COLUMNS if col in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)
    df = df.reindex(sorted(df.columns), axis=1)
    return df


def _compare_text(name: str, dev_file: Path, feature_file: Path) -> bool:
    dev_text = dev_file.read_text()
    feature_text = feature_file.read_text()
    if dev_text == feature_text:
        print(f"[OK] {name}")
        return True
    print(f"[DIFF] {name}: contents differ")
    return False


def compare_file(name: str, dev_file: Path, feature_file: Path) -> bool:
    if any(name.endswith(suffix) for suffix in TEXT_ONLY_SUFFIXES):
        return _compare_text(name, dev_file, feature_file)
    df_dev = _load_csv(dev_file)
    df_feature = _load_csv(feature_file)
    try:
        assert_frame_equal(
            df_dev,
            df_feature,
            check_dtype=False,
            check_like=False,
            atol=0,
            rtol=0,
        )
        print(f"[OK] {name}")
        return True
    except AssertionError as exc:
        print(f"[DIFF] {name}: {exc}")
        return False


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two backtest artifact folders")
    parser.add_argument("dev_dir", type=Path, help="Directory containing dev CSV outputs")
    parser.add_argument("feature_dir", type=Path, help="Directory containing feature CSV outputs")
    parser.add_argument(
        "--files",
        nargs="+",
        default=("stats.csv", "trades.csv"),
        help="Filenames to compare (default: %(default)s)",
    )
    parser.add_argument(
        "--include-logs",
        action="store_true",
        help="Include logs.csv comparison (fails if timestamps differ)",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    file_list = list(args.files)
    if args.include_logs and "logs.csv" not in file_list:
        file_list.append("logs.csv")

    success = True
    for filename in file_list:
        dev_file = _locate_file(args.dev_dir, filename)
        feature_file = _locate_file(args.feature_dir, filename)
        success &= compare_file(filename, dev_file, feature_file)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
