# Maintainer triage

The objective is useful decisions and reliable contributions, not zero open PRs.
For each item identify the user problem, reproduction, affected contract, owner,
waiting party, and one next action. Review correctness/security fixes first.

Classify a PR as actionable, waiting for a specific change, superseded, duplicate,
or outside project scope. Close only with a specific explanation and replacement
link when applicable. Preserve credit. Do not close by age alone or automatically
merge from bot approval. Avoid response-time promises without an available owner.

`scripts/github_triage_digest.py` accepts an array of `gh pr view` JSON objects
including author, comments, reviews, timestamps and checks. It is read-only and
creates no jobs, comments, labels, or merges. Fetch every page before preparing
the input; truncated metadata is not a complete review. Missing author association
is unknown, not proof that a person is not a maintainer. Bot chatter is excluded
from substantive maintainer response. A changes-requested review may already have
been addressed; inspect the diff before assigning work.

For each decision retain the relevant source SHA, reproducing test, current check
results and rationale. Any order/clock/thread change requires engine integration
coverage as well as unit tests. Real-provider evidence must identify its provider,
mode, timestamp and limitations without exposing credentials.
