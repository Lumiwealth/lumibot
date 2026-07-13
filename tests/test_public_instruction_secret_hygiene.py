"""Regression checks for the public repo leak scanner."""

import importlib.util
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_public_repo_hygiene.py"
SPEC = importlib.util.spec_from_file_location("check_public_repo_hygiene", SCRIPT_PATH)
assert SPEC and SPEC.loader
scanner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = scanner
SPEC.loader.exec_module(scanner)


def test_public_policy_files_do_not_publish_private_paths_or_credentials():
    violations = scanner.scan_lines(
        scanner.iter_file_lines(["AGENTS.md", "CLAUDE.md", "SECURITY.md"])
    )

    assert violations == []


def test_scanner_blocks_personal_paths_in_any_changed_text_file():
    private_path = "/".join(["", "Users", "alice", "Development", "secret-project"])

    violations = scanner.scan_lines([("lumibot/example.py", 10, f'PATH = "{private_path}"')])

    assert violations
    assert violations[0][0] == "lumibot/example.py"
    assert violations[0][2] == "personal-absolute-path"


def test_scanner_blocks_internal_company_email_in_any_changed_text_file():
    internal_email = "rob-dev@" + "lumiwealth.com"

    violations = scanner.scan_lines([("docs/example.md", 5, f"Username: {internal_email}")])

    assert violations
    assert violations[0][2] == "internal-email"


def test_scanner_blocks_credential_tables_in_public_docs():
    violations = scanner.scan_lines([("AGENTS.md", 20, "| Password | example-value |")])

    assert violations
    assert violations[0][2] == "credential-table"
