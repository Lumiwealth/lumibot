"""Regression checks for public AI instruction files.

These files are tracked in the open-source LumiBot repo, so they must not carry
private local paths or real credential/account material.
"""

from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_INSTRUCTION_FILES = [
    REPO_ROOT / "AGENTS.md",
    REPO_ROOT / "CLAUDE.md",
    REPO_ROOT / "SECURITY.md",
]

FORBIDDEN_PATTERNS = [
    re.compile(r"/Users/[^`\s)]+"),
    re.compile(r"Documents/Development"),
    re.compile(r"\bDev Credentials\b", re.IGNORECASE),
    re.compile(r"\bUsername\s*\|", re.IGNORECASE),
    re.compile(r"\bPassword\s*\|", re.IGNORECASE),
    re.compile(r"\b[A-Z0-9._%+-]+@(lumiwealth|botspot)\.", re.IGNORECASE),
]


def collect_public_instruction_hygiene_violations():
    violations = []

    for path in PUBLIC_INSTRUCTION_FILES:
        text = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_PATTERNS:
            for match in pattern.finditer(text):
                line_number = text.count("\n", 0, match.start()) + 1
                violations.append(f"{path.relative_to(REPO_ROOT)}:{line_number}: {pattern.pattern}")

    return violations


def test_public_instruction_files_do_not_publish_private_paths_or_credentials():
    violations = collect_public_instruction_hygiene_violations()

    assert violations == []


if __name__ == "__main__":
    violations = collect_public_instruction_hygiene_violations()
    if violations:
        print("Public instruction hygiene violations found:")
        for violation in violations:
            print(f"  {violation}")
        raise SystemExit(1)
