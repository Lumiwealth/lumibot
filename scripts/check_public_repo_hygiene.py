#!/usr/bin/env python3
"""Block new public-repo leaks in changed lines.

This is intentionally a changed-line scanner for CI. LumiBot has historical
docs/tests with local paths and fake credentials; failing the whole repository
would create noise. The gate should stop new leaks from being added anywhere in
the repo while old debt is cleaned up separately.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_DEV_PATH = "Documents" + "/Development"
STRATEGY_LIBRARY = "Strategy Library"
STRATEGY_DEMOS = STRATEGY_LIBRARY + "/Demos"
MAC_USERS_PREFIX = "/" + "Users" + "/"


@dataclass(frozen=True)
class Rule:
    id: str
    description: str
    pattern: re.Pattern[str]
    path_pattern: re.Pattern[str] | None = None

    def applies_to(self, path: str) -> bool:
        return self.path_pattern is None or self.path_pattern.search(path) is not None


TEXT_EXTENSIONS = {
    ".cfg",
    ".css",
    ".env",
    ".html",
    ".ini",
    ".js",
    ".json",
    ".md",
    ".py",
    ".rst",
    ".sh",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}

PUBLIC_DOC_PATH = re.compile(
    r"(^|/)(AGENTS\.md|CLAUDE\.md|SECURITY\.md|README\.md|docs/|docsrc/|\.github/)",
    re.IGNORECASE,
)

RULES = [
    Rule(
        id="personal-absolute-path",
        description="personal absolute filesystem path",
        pattern=re.compile(
            rf"({re.escape(MAC_USERS_PREFIX)}[A-Za-z0-9._-]+/({re.escape(DOCS_DEV_PATH)}|Development|Library|\.aws|\.ssh|\.config|\.claude|\.codex)\b)"
            r"|(/home/[A-Za-z0-9._-]+/(Development|\.aws|\.ssh|\.config)\b)"
            r"|([A-Za-z]:\\Users\\[^\\\s]+\\)"
        ),
    ),
    Rule(
        id="mac-temp-local-path",
        description="Mac local temporary/private path",
        pattern=re.compile(r"/var/folders/[A-Za-z0-9_/.-]+"),
    ),
    Rule(
        id="private-dev-folder",
        description="private local development folder reference",
        pattern=re.compile(rf"{re.escape(DOCS_DEV_PATH)}|{re.escape(STRATEGY_DEMOS)}", re.IGNORECASE),
    ),
    Rule(
        id="internal-email",
        description="internal/company account email in public repo content",
        pattern=re.compile(r"\b[A-Z0-9._%+-]+@(lumiwealth\.com|botspot\.trade)\b", re.IGNORECASE),
    ),
    Rule(
        id="credential-table",
        description="markdown credential table with account/secret field",
        pattern=re.compile(r"\|\s*(username|password|api\s*key|token|secret)\s*\|", re.IGNORECASE),
        path_pattern=PUBLIC_DOC_PATH,
    ),
    Rule(
        id="dev-credential-heading",
        description="dev credential section in public docs/instructions",
        pattern=re.compile(r"\bdev credentials?\b", re.IGNORECASE),
        path_pattern=PUBLIC_DOC_PATH,
    ),
    Rule(
        id="real-local-credential-file",
        description="local credential or env file path",
        pattern=re.compile(
            rf"((\.env|creds?\.txt|credential-file|creds-file).*({re.escape(MAC_USERS_PREFIX)}|{re.escape(DOCS_DEV_PATH)}|{re.escape(STRATEGY_LIBRARY)}))"
            rf"|(({re.escape(MAC_USERS_PREFIX)}|{re.escape(DOCS_DEV_PATH)}|{re.escape(STRATEGY_LIBRARY)}).*(\.env|creds?\.txt|credential-file|creds-file))",
            re.IGNORECASE,
        ),
    ),
]


def run_git(args: list[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout


def is_text_path(path: str) -> bool:
    suffix = Path(path).suffix.lower()
    return suffix in TEXT_EXTENSIONS or Path(path).name in {"AGENTS.md", "CLAUDE.md", "SECURITY.md", "CODEOWNERS"}


def iter_changed_lines(diff_range: str):
    diff = run_git(["diff", "--unified=0", "--no-ext-diff", "--no-renames", diff_range, "--"])
    current_path: str | None = None
    new_line_number: int | None = None

    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            current_path = line[len("+++ b/") :]
            new_line_number = None
            continue
        if line.startswith("+++ /dev/null"):
            current_path = None
            new_line_number = None
            continue
        if line.startswith("@@ "):
            match = re.search(r"\+(\d+)(?:,(\d+))?", line)
            new_line_number = int(match.group(1)) if match else None
            continue
        if current_path is None or new_line_number is None:
            continue
        if not is_text_path(current_path):
            continue
        if line.startswith("+") and not line.startswith("+++"):
            yield current_path, new_line_number, line[1:]
            new_line_number += 1
        elif line.startswith("-") and not line.startswith("---"):
            continue
        else:
            new_line_number += 1


def iter_file_lines(paths: list[str]):
    for path in paths:
        if not is_text_path(path):
            continue
        full_path = REPO_ROOT / path
        if not full_path.exists() or not full_path.is_file():
            continue
        try:
            text = full_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            yield path, line_number, line


def scan_lines(lines):
    violations = []
    for path, line_number, line in lines:
        for rule in RULES:
            if not rule.applies_to(path):
                continue
            if rule.pattern.search(line):
                violations.append((path, line_number, rule.id, rule.description))
    return violations


def print_violations(violations) -> None:
    print("Public repo leak check failed. Remove or replace private details with placeholders.")
    for path, line_number, rule_id, description in violations:
        print(f"  {path}:{line_number}: {rule_id} ({description})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--diff-range", help="Git diff range to scan, for example origin/dev...HEAD")
    group.add_argument("--staged", action="store_true", help="Scan staged added lines")
    group.add_argument("--files", nargs="+", help="Scan full contents of specific files")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.diff_range:
        lines = iter_changed_lines(args.diff_range)
    elif args.staged:
        lines = iter_changed_lines("--cached")
    else:
        lines = iter_file_lines(args.files)

    violations = scan_lines(lines)
    if violations:
        print_violations(violations)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
