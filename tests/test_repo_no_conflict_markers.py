"""No tracked file may contain unresolved Git merge-conflict markers.

A merge on version/4.6.53 committed `<<<<<<< HEAD` / `>>>>>>> origin/dev` blocks into
AGENTS.md, so agents read both sides of two conflicting rules. A bare `=======` line is
a valid reStructuredText heading underline, so only the opening and closing markers count.
"""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARKERS = ("<<<<<<< ", ">>>>>>> ")


def test_tracked_files_have_no_conflict_markers():
    files = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True
    ).stdout.split(b"\0")
    found = []
    for name in filter(None, files):
        path = ROOT / name.decode()
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
        for number, line in enumerate(text.splitlines(), 1):
            if line.startswith(MARKERS):
                found.append(f"{name.decode()}:{number}")
    assert not found, f"unresolved merge-conflict markers: {found}"
