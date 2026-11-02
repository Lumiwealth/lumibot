import os
import shutil
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
    )
    assert result.returncode == expected_exit, result.stderr


def test_source_ci_and_release_share_the_exact_eval_sdk_constraints():
    root = Path(__file__).resolve().parents[1]
    workflow = yaml.load((root / ".github/workflows/cicd.yaml").read_text(), Loader=yaml.BaseLoader)
    installs = [step["run"] for job in workflow["jobs"].values() for step in job.get("steps", [])
                if "pip install -r requirements_dev.txt" in step.get("run", "")]
    assert len(installs) == 3
    assert all("pip install -r requirements_dev.txt -c requirements_agent_evals.txt" in step for step in installs)


def test_release_and_source_qualification_await_completion_without_shell_deadlines():
    root = Path(__file__).resolve().parents[1]
    for name in ("release.yml", "cicd.yaml"):
        workflow = yaml.load((root / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)
        commands = [step.get("run", "") for job in workflow["jobs"].values() for step in job.get("steps", [])]
        for command in commands:
            for line in command.splitlines():
                assert not line.strip().startswith("timeout "), "Qualification must await completion or a real error"


@pytest.mark.parametrize("workflow_name", ["release.yml", "cicd.yaml"])
@pytest.mark.parametrize("job", ["unit-tests", "backtest-tests"])
def test_shards_preserve_complete_test_names(tmp_path, workflow_name, job):
    """Execute the actual workflow with space-bearing file and parameter names."""
    root = Path(__file__).resolve().parents[1]
    workflow = yaml.load(
        (root / ".github/workflows" / workflow_name).read_text(), Loader=yaml.BaseLoader,
    )
    command = next(step["run"] for step in workflow["jobs"][job]["steps"]
                   if step.get("name", "").startswith("Run ") and "--collect-only" in step.get("run", ""))
    folder = tmp_path / "tests" / ("backtest" if job == "backtest-tests" else "unit") / "interval samples"
    folder.mkdir(parents=True)
    (folder / "test_interval.py").write_text(
        'import pytest\n'
        '@pytest.mark.parametrize("interval", ["1 minute", "1 hour"])\n'
        'def test_interval(interval):\n'
        '    assert interval in {"1 minute", "1 hour"}\n'
    )
    result = subprocess.run(
        ["bash", "-c", command], cwd=tmp_path, capture_output=True, text=True,
        env={**os.environ, "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", ""),
             "SHARD_INDEX": "0", "SHARD_TOTAL": "1", "PYTEST_ADDOPTS": ""},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "2 passed" in result.stdout


@pytest.fixture
def release_checkout_candidate(tmp_path):
    """Real disposable Git refs, with no publication or customer credentials."""
    owned = []

    def create(version="1.2.3"):
        origin = tmp_path / "origin.git"
        repo = tmp_path / "candidate"
        owned.extend([origin, repo])
        subprocess.run(["git", "init", "--bare", str(origin)], check=True, capture_output=True)
        subprocess.run(["git", "init", "-b", f"version/{version}", str(repo)], check=True, capture_output=True)

        def git(*args):
            return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True)

        git("config", "user.name", "Release fixture")
        git("config", "user.email", "release-fixture@example.invalid")
        git("config", "commit.gpgsign", "false")  # Only this synthetic repository.
        (repo / "setup.py").write_text(f'version="{version}"\n')
        git("add", "setup.py")
        git("commit", "-m", "Initial synthetic package")
        git("remote", "add", "origin", str(origin))
        git("push", "-u", "origin", "HEAD")
        return repo, git

    try:
        yield create
    finally:
        for path in owned:
            if path.exists():
                shutil.rmtree(path)


def _verify_checkout(repo, *args):
    script = Path(__file__).resolve().parents[1] / "scripts/verify_release_checkout_state.py"
    return subprocess.run([sys.executable, str(script), *args], cwd=repo, capture_output=True, text=True)


def test_changed_source_cannot_reuse_an_already_tagged_package_version(release_checkout_candidate):
    repo, git = release_checkout_candidate()
    git("tag", "v1.2.3")
    git("push", "origin", "refs/tags/v1.2.3")
    (repo / "change.txt").write_text("A new source change needs a forward package version.\n")
    git("add", "change.txt")
    git("commit", "-m", "Changed source after release tag")
    result = _verify_checkout(repo)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "already tagged" in result.stdout


def test_same_immutable_tagged_source_can_be_reverified(release_checkout_candidate):
    repo, git = release_checkout_candidate()
    git("tag", "v1.2.3")
    git("push", "origin", "refs/tags/v1.2.3")
    result = _verify_checkout(repo)
    assert result.returncode == 0, result.stdout + result.stderr


def test_untagged_forward_package_remains_usable(release_checkout_candidate):
    repo, _ = release_checkout_candidate("1.2.4")
    result = _verify_checkout(repo)
    assert result.returncode == 0, result.stdout + result.stderr


def test_post_release_checkout_still_accepts_the_next_patch(release_checkout_candidate):
    repo, _ = release_checkout_candidate("1.2.4")
    result = _verify_checkout(repo, "--post-release-of", "1.2.3")
    assert result.returncode == 0, result.stdout + result.stderr


def test_portable_gate_keeps_offline_tradier_checks_and_excludes_live_api(tmp_path):
    """Collect with configured placeholders; no broker request is executed."""
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/test_tradier_data.py",
         "--collect-only", "-q", "-m", "not apitest", "-o", "addopts=",
         "--basetemp", str(tmp_path / "collection")],
        cwd=root,
        env={**os.environ, "LUMIBOT_DISABLE_DOTENV": "1", "PYTEST_ADDOPTS": "",
             "TRADIER_TEST_ACCESS_TOKEN": "collection-only-not-a-credential",
             "TRADIER_TEST_ACCOUNT_NUMBER": "collection-only"},
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    selected = [line for line in result.stdout.splitlines() if "::TestTradierData::" in line]
    assert set(selected) == {
        "tests/test_tradier_data.py::TestTradierData::test_basics",
        "tests/test_tradier_data.py::TestTradierData::test_sanitize_base_and_quote_asset",
        "tests/test_tradier_data.py::TestTradierData::test_default_delay_value",
    }, "The portable gate must preserve offline checks without running the live broker API."
    # The network assertions remain runnable as API integration tests.
    api_command = list(result.args)
    api_command[api_command.index("not apitest")] = "apitest"
    api_command[-1] = str(tmp_path / "api-collection")
    api_result = subprocess.run(
        api_command, cwd=root,
        env={**os.environ, "LUMIBOT_DISABLE_DOTENV": "1", "PYTEST_ADDOPTS": "",
             "TRADIER_TEST_ACCESS_TOKEN": "collection-only-not-a-credential",
             "TRADIER_TEST_ACCOUNT_NUMBER": "collection-only"},
        capture_output=True, text=True,
    )
    assert api_result.returncode == 0, api_result.stdout + api_result.stderr
    api_selected = [line for line in api_result.stdout.splitlines() if "::TestTradierData::" in line]
    assert len(api_selected) == 10
    assert "tests/test_tradier_data.py::TestTradierData::test_get_last_price_stock" in api_selected
