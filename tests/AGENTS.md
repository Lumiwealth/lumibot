# Tests Agent Instructions (Legacy Test Policy)

These rules apply to all files under `tests/`.

## BotSpot integration release qualification

Passing LumiBot unit tests is necessary but does not qualify a combined BotSpot
release. Before any BotSpot Dev branch move or deployment, the exact combined
candidate must pass every formal Playwright test owned by the release scope
locally on Rob's Mac. Focused Playwright tests are repair-only. When Agent is
scoped, mandatory local freshness must select and pass every stale, missing,
failed, incomplete, or incompatible case, with a second unchanged selection of
zero. GitHub reruns are blocking release validation for the exact candidate;
the hosted Dev rerun additionally supplies final environment proof.

## Legacy tests are high-authority

- Treat any test whose **earliest commit date is before 2025-06-01** as **LEGACY**.
- Treat any test whose **earliest commit date is before 2025-01-01** as **FROZEN LEGACY** (effectively “do not change”).

- For LEGACY/FROZEN LEGACY tests, **fix the code, not the test**.
- Only change a LEGACY test when you can clearly justify that:
  - the old expectation was incorrect, or
  - behavior was intentionally changed for correctness (e.g., fixing lookahead bias),
  and you document it in the test file.
- Only change a FROZEN LEGACY test in exceptional cases (and include a clear write-up in the test + PR).

### How to check “how old” a test is

- File-level earliest commit:
  - `git log --follow --reverse --format='%ad %h %s' --date=short -- path/to/test_file.py | head -1`
- Line-level (best signal for a specific assertion):
  - `git blame -L <line>,<line> -- path/to/test_file.py` (check the commit date and message)

## Any test change must be explained

- If you change any expected values or assertions, add a short note near the change
  explaining **why** (what changed, and why the new expectation is correct).
- Prefer making the test more robust (less brittle) over updating magic numbers.

## CI guard for legacy edits

- CI should block edits to LEGACY tests unless the PR is explicitly approved.
  The recommended mechanism is a PR label + required write-up (see `.github/workflows/`).
