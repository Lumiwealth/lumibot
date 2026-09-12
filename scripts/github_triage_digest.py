"""Read-only PR triage. Consumes gh pr view JSON; never writes to GitHub."""
import argparse
import json
from pathlib import Path


def classify(item):
    author = (item.get("author") or {}).get("login")
    maintainer_events, unknown = [], set()
    for event in [*(item.get("comments") or []), *(item.get("reviews") or [])]:
        user = event.get("author") or {}
        login = user.get("login", "")
        if not login or login.endswith("[bot]") or user.get("isBot"):
            continue
        if login == author:
            continue
        role = event.get("authorAssociation")
        if role in {"OWNER", "MEMBER", "COLLABORATOR"}:
            maintainer_events.append(event)
        elif role is None:
            unknown.add(login)
    maintainer_events.sort(key=lambda x: x.get("submittedAt") or x.get("createdAt") or "")
    latest = maintainer_events[-1] if maintainer_events else None
    waiting = "maintainer triage"
    if item.get("isDraft"):
        waiting = "contributor draft"
    elif latest and latest.get("state") == "CHANGES_REQUESTED":
        waiting = "contributor revision; verify whether addressed"
    elif latest:
        waiting = "maintainer decision; inspect discussion"
    return {
        "number": item.get("number"), "title": item.get("title"), "url": item.get("url"),
        "created_at": item.get("createdAt"), "waiting_on": waiting,
        "last_maintainer_response": (latest.get("submittedAt") or latest.get("createdAt")) if latest else None,
        "unknown_human_roles": sorted(unknown),
        "checks": item.get("statusCheckRollup"),
        "next_action": "Inspect diff, reproduction, compatibility and current checks; do not close by age alone.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="JSON array of complete gh pr view responses")
    args = parser.parse_args()
    items = json.loads(args.input.read_text())
    if not isinstance(items, list):
        parser.error("input must be a JSON array; confirm every API page was fetched")
    print(json.dumps([classify(item) for item in items], indent=2))


if __name__ == "__main__":
    main()
