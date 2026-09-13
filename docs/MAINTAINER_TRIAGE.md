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

## Run a read-only snapshot

```bash
gh pr list --repo Lumiwealth/lumibot --state open --limit 100 \
  --json number,title,url,createdAt,isDraft,author,comments,reviews,statusCheckRollup \
  > /tmp/lumibot-prs.json
python scripts/github_triage_digest.py /tmp/lumibot-prs.json > /tmp/lumibot-triage.json
```

This is a metadata snapshot, not a complete engineering review. If the list
reaches the requested limit, fetch the remaining PRs before reporting an inventory.
The CLI may omit author associations and truncate long discussions; use paginated
GraphQL comment/review data with `authorAssociation` to establish maintainer roles
and full discussion coverage. The digest explicitly preserves unknown roles.
Keep downloaded discussion bodies out of public commits. Review the digest locally;
the script sends no comments and changes no labels, PRs, or repository settings.
