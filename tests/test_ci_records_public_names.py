"""Release and qualification records under ci/ are public: they never name private services.

LumiBot is a public framework. A 4.6.54 release cost record named a private downstream
deployment service. Records describe downstream work generically ("downstream deployment").
"""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PRIVATE = re.compile(r"bot[\s_-]?manager", re.IGNORECASE)


def test_ci_records_never_name_a_private_service():
    files = subprocess.run(
        ["git", "ls-files", "-z", "ci"], cwd=ROOT, check=True, capture_output=True
    ).stdout.split(b"\0")
    found = []
    for name in filter(None, files):
        text = (ROOT / name.decode()).read_text(encoding="utf-8", errors="replace")
        for number, line in enumerate(text.splitlines(), 1):
            if PRIVATE.search(line):
                found.append(f"{name.decode()}:{number}")
    assert not found, f"private service name in public ci records: {found}"
