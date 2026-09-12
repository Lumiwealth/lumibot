import importlib.util
from pathlib import Path


def load_digest():
    path = Path(__file__).resolve().parents[1] / "scripts/github_triage_digest.py"
    spec = importlib.util.spec_from_file_location("triage_digest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bot_chatter_does_not_count_as_maintainer_response():
    digest = load_digest()
    item = {"number": 1, "author": {"login": "contributor"}, "createdAt": "2026-01-01T00:00:00Z",
            "comments": [{"author": {"login": "coderabbitai[bot]"}, "authorAssociation": "MEMBER", "createdAt": "2026-09-10T00:00:00Z"}],
            "reviews": [], "isDraft": False}
    result = digest.classify(item)
    assert result["last_maintainer_response"] is None
    assert result["waiting_on"] == "maintainer triage"


def test_unknown_association_is_reported_not_assumed():
    item = {"author": {"login": "alice"}, "comments": [{"author": {"login": "bob"}, "createdAt": "2026-09-10T00:00:00Z"}], "reviews": []}
    assert load_digest().classify(item)["unknown_human_roles"] == ["bob"]


def test_changes_requested_assigns_contributor_next_step():
    item = {"author": {"login": "alice"}, "comments": [], "reviews": [{"author": {"login": "maintainer"}, "authorAssociation": "MEMBER", "state": "CHANGES_REQUESTED", "submittedAt": "2026-09-10T00:00:00Z"}]}
    assert load_digest().classify(item)["waiting_on"] == "contributor revision; verify whether addressed"


def test_github_cli_bot_flag_is_not_classified_as_a_human():
    item = {"author": {"login": "alice"}, "comments": [
        {"author": {"login": "coderabbitai", "is_bot": True},
         "authorAssociation": "MEMBER", "createdAt": "2026-09-10T00:00:00Z"}
    ]}
    result = load_digest().classify(item)
    assert result["last_maintainer_response"] is None
    assert result["unknown_human_roles"] == []
