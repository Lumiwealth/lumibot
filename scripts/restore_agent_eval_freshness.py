#!/usr/bin/env python3
"""Restore the newest usable LumiBot agent-eval freshness artifact.

GitHub Actions caches are branch scoped, so a passing qualification run on a
version branch is not visible to the tag-triggered release workflow. Artifacts
are repository scoped. This helper downloads only successful standalone eval
artifacts; ``run_agent_evals.py`` remains the authority that accepts or rejects
each case by its full runtime/case/model fingerprint and age.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import tempfile
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

API_ROOT = "https://api.github.com"


class _StripCrossHostAuthorization(urllib.request.HTTPRedirectHandler):
    """Keep the GitHub token off the signed artifact-storage redirect."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if (
            redirected is not None
            and urllib.parse.urlparse(req.full_url).netloc != urllib.parse.urlparse(newurl).netloc
        ):
            redirected.remove_header("Authorization")
        return redirected


def _get_json(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _get_bytes(url: str, token: str) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    opener = urllib.request.build_opener(_StripCrossHostAuthorization())
    with opener.open(request, timeout=60) as response:
        return response.read()


def _freshness_from_zip(payload: bytes) -> dict[str, Any] | None:
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            candidates = [
                name for name in archive.namelist() if not name.endswith("/") and Path(name).name == "freshness.json"
            ]
            if not candidates:
                return None
            candidate = sorted(candidates, key=lambda name: (name.count("/"), name))[0]
            value = json.loads(archive.read(candidate))
    except (OSError, ValueError, KeyError, zipfile.BadZipFile, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or not isinstance(value.get("cases"), dict):
        return None
    return value


def _is_ancestor_of_candidate(*, repository: str, token: str, ancestor: str, candidate: str) -> bool:
    if ancestor == candidate:
        return True
    comparison = _get_json(
        f"{API_ROOT}/repos/{repository}/compare/{ancestor}...{candidate}",
        token,
    )
    return comparison.get("status") == "ahead"


def restore(
    *,
    repository: str,
    token: str,
    workflow: str,
    output: Path,
    trusted_commit: str,
    limit: int = 20,
) -> int | None:
    workflow_name = urllib.parse.quote(workflow, safe="")
    runs_url = (
        f"{API_ROOT}/repos/{repository}/actions/workflows/{workflow_name}/runs"
        f"?status=success&event=workflow_dispatch&per_page={limit}"
    )
    runs = _get_json(runs_url, token).get("workflow_runs", [])
    for run in runs:
        run_id = run.get("id")
        if not isinstance(run_id, int) or run.get("conclusion") != "success":
            continue
        head_sha = run.get("head_sha")
        if not isinstance(head_sha, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", head_sha):
            continue
        if not _is_ancestor_of_candidate(
            repository=repository,
            token=token,
            ancestor=head_sha,
            candidate=trusted_commit,
        ):
            continue
        artifacts = _get_json(
            f"{API_ROOT}/repos/{repository}/actions/runs/{run_id}/artifacts?per_page=100",
            token,
        ).get("artifacts", [])
        expected_name = f"lumibot-agent-evals-{run_id}"
        artifact = next(
            (
                item
                for item in artifacts
                if item.get("name") == expected_name
                and item.get("expired") is False
                and isinstance(item.get("archive_download_url"), str)
            ),
            None,
        )
        if artifact is None:
            continue
        freshness = _freshness_from_zip(_get_bytes(artifact["archive_download_url"], token))
        if freshness is None:
            continue
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent, delete=False) as temporary:
            json.dump(freshness, temporary, indent=2, sort_keys=True)
            temporary.write("\n")
            temporary_path = Path(temporary.name)
        temporary_path.replace(output)
        return run_id
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--token", default=os.environ.get("GITHUB_TOKEN", ""))
    parser.add_argument("--workflow", default="agent-evals.yml")
    parser.add_argument("--output", type=Path, default=Path(".ci/agent-evals/freshness.json"))
    parser.add_argument("--trusted-commit", default=os.environ.get("GITHUB_SHA", ""))
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    if not args.repository or "/" not in args.repository:
        parser.error("--repository or GITHUB_REPOSITORY is required")
    if not args.token:
        parser.error("--token or GITHUB_TOKEN is required")
    if not re.fullmatch(r"[0-9a-fA-F]{40}", args.trusted_commit):
        parser.error("--trusted-commit or GITHUB_SHA must be a full 40-character commit SHA")
    if args.limit < 1 or args.limit > 100:
        parser.error("--limit must be between 1 and 100")
    run_id = restore(
        repository=args.repository,
        token=args.token,
        workflow=args.workflow,
        output=args.output,
        trusted_commit=args.trusted_commit,
        limit=args.limit,
    )
    if run_id is None:
        print("No usable prior agent-eval freshness artifact found; the gate will run stale cases.")
    else:
        print(f"Restored agent-eval freshness from successful workflow run {run_id}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
