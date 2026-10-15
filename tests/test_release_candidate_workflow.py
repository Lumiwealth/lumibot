import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


def _workflow():
    return yaml.load(
        (Path(__file__).resolve().parents[1] / ".github/workflows/release.yml").read_text(),
        Loader=yaml.BaseLoader,
    )


def test_manual_release_validation_cannot_publish_or_advance_the_version():
    workflow = _workflow()
    assert "workflow_dispatch" in workflow["on"]
    jobs = workflow["jobs"]
    assert jobs["publish"]["if"] == (
        "${{ github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v') }}"
    )
    assert set(jobs["publish"]["needs"]) == {
        "validate-build", "unit-tests", "backtest-tests", "agent-evals"
    }
    assert jobs["start-next-version"]["needs"] == "publish"
    assert "always()" not in jobs["start-next-version"].get("if", "")


@pytest.mark.parametrize(
    ("event", "ref", "version", "expected_exit"),
    [
        ("workflow_dispatch", "version/4.6.19", "4.6.19", 0),
        ("push", "v4.6.19", "4.6.19", 0),
        ("push", "v4.6.20", "4.6.19", 1),
        ("push", "version/4.6.19", "4.6.19", 1),
        ("pull_request", "version/4.6.19", "4.6.19", 1),
        ("workflow_dispatch", "version/4.6.19", "", 1),
    ],
)
def test_candidate_version_check_preserves_the_tag_boundary(tmp_path, event, ref, version, expected_exit):
    (tmp_path / "setup.py").write_text(f'version="{version}"\n')
    steps = _workflow()["jobs"]["validate-build"]["steps"]
    script = next(step["run"] for step in steps if step.get("name") == "Validate tag or manual candidate version")
    result = subprocess.run(
        ["bash", "-c", script],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", ""),
            "GITHUB_EVENT_NAME": event,
            "GITHUB_REF_NAME": ref,
        },
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == expected_exit, result.stderr
