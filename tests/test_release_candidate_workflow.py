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


def test_manual_validation_publishes_only_an_explicit_existing_recovery_tag():
    workflow = _workflow()
    assert workflow["on"]["workflow_dispatch"]["inputs"]["recover_tag"]["required"] == "false"
    jobs = workflow["jobs"]
    assert jobs["publish"]["if"] == (
        "${{ (github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v')) || (github.event_name == 'workflow_dispatch' && inputs.recover_tag != '') }}"
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


def test_backtest_shard_preserves_parameterized_nodeids_with_spaces(tmp_path):
    import json
    nodeid = 'tests/backtest/test_synthetic.py::test_bars[1 minute]'
    (tmp_path / 'shard_nodeids.txt').write_text(nodeid + '\n')
    capture = tmp_path / 'argv.json'
    executable = tmp_path / 'python'
    executable.write_text('#!' + sys.executable + '\nimport json,sys\nfrom pathlib import Path\nPath(' + repr(str(capture)) + ').write_text(json.dumps(sys.argv[1:]))\n')
    executable.chmod(0o700)
    steps = _workflow()['jobs']['backtest-tests']['steps']
    script = next(step['run'] for step in steps if step.get('name') == 'Run backtest tests')
    tail = script.rsplit('          PY', 1)[-1] if '          PY' in script else script.rsplit('\nPY\n', 1)[-1]
    result = subprocess.run(['bash', '-c', tail], cwd=tmp_path,
        env={**os.environ, 'PATH': str(tmp_path) + os.pathsep + os.environ.get('PATH', ''), 'PYTEST_MARKERS': 'not apitest'},
        capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(capture.read_text())[-1:] == [nodeid]


def test_recovery_uses_the_same_tag_for_every_package_gate():
    jobs = _workflow()['jobs']
    for name in ('validate-build', 'unit-tests', 'backtest-tests', 'agent-evals'):
        checkout = next(step for step in jobs[name]['steps'] if step.get('name') == 'Checkout')
        assert checkout['with']['ref'] == '${{ inputs.recover_tag || github.ref }}'
    assert jobs['publish']['steps'][-1]['with']['tag_name'] == '${{ inputs.recover_tag || github.ref_name }}'


@pytest.mark.parametrize('recovery_tag,expected_exit', [('v1.2.3',0),('v1.2.4',1),('version/1.2.3',1)])
def test_manual_publication_recovery_requires_the_exact_existing_tag(release_checkout_candidate,recovery_tag,expected_exit):
    repo,git=release_checkout_candidate('1.2.3')
    git('tag','v1.2.3')
    script=next(step['run'] for step in _workflow()['jobs']['validate-build']['steps']
        if step.get('name') == 'Validate tag or manual candidate version')
    result=subprocess.run(['bash','-c',script],cwd=repo,
        env={**os.environ,'PATH':str(Path(sys.executable).parent)+os.pathsep+os.environ.get('PATH',''),
             'GITHUB_EVENT_NAME':'workflow_dispatch','GITHUB_REF_NAME':'dev','RECOVER_TAG':recovery_tag},
        capture_output=True,text=True)
    assert result.returncode == expected_exit,result.stderr
