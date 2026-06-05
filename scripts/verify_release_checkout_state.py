#!/usr/bin/env python3
"""Verify LumiBot release checkout state before/after deployment work."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


def run_git(args: list[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout.strip()


def setup_version() -> str:
    text = Path("setup.py").read_text()
    match = re.search(r'version=["\']([^"\']+)["\']', text)
    if not match:
        raise SystemExit("ERROR: setup.py does not contain a package version")
    return match.group(1)


def next_patch(version: str) -> str:
    match = VERSION_RE.match(version)
    if not match:
        raise SystemExit(f"ERROR: invalid released version: {version}")
    major, minor, patch = map(int, match.groups())
    return f"{major}.{minor}.{patch + 1}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify the local LumiBot checkout is on the correct version branch."
    )
    parser.add_argument(
        "--post-release-of",
        help="Released X.Y.Z version. Requires local checkout to be on version/X.Y.(Z+1).",
    )
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Allow dirty/untracked files. For diagnostics only; release completion must not use this.",
    )
    args = parser.parse_args()

    branch = run_git(["branch", "--show-current"])
    version = setup_version()
    expected_version = next_patch(args.post_release_of) if args.post_release_of else version
    expected_branch = f"version/{expected_version}"
    dirty = run_git(["status", "--porcelain=v1"])

    print(f"branch={branch}")
    print(f"setup.py={version}")
    print(f"expected_branch={expected_branch}")
    print(f"expected_setup.py={expected_version}")

    failures: list[str] = []
    if branch != expected_branch:
        failures.append(f"branch must be {expected_branch}, not {branch}")
    if version != expected_version:
        failures.append(f"setup.py must be {expected_version}, not {version}")
    if dirty and not args.allow_dirty:
        failures.append("working tree must be clean before release completion/BotManager deploy")

    try:
        remote_branch = run_git(["ls-remote", "--heads", "origin", expected_branch])
    except subprocess.CalledProcessError as exc:
        failures.append(f"could not verify origin/{expected_branch}: {exc.stderr.strip()}")
    else:
        if not remote_branch:
            failures.append(f"origin/{expected_branch} does not exist")

    if failures:
        print("\nFAILED:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("OK: release checkout state is correct")
    return 0


if __name__ == "__main__":
    sys.exit(main())
