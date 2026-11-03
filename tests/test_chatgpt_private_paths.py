"""Local path attacks use only synthetic credentials and victim files."""
import os
import time

import pytest

from lumibot.components.agents import chatgpt_auth as auth

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX ownership and directory permissions")


def profile(path):
    auth.save_private(path, {"status": "connected", "scopes": [auth.PLAN_SCOPE],
                             "access_token": "synthetic", "saved_at": time.time(), "expires_in": 3600})


@pytest.mark.parametrize("kind", ["hardlink", "symlink", "writable"])
def test_profile_lock_rejects_unsafe_file_without_truncating_victim(tmp_path, kind):
    path = tmp_path / "profile.json"
    profile(path)
    victim = tmp_path / "victim"
    victim.write_text("must survive")
    victim.chmod(0o600)
    lock = path.with_name(path.name + ".lock")
    if kind == "hardlink":
        os.link(victim, lock)
    elif kind == "symlink":
        lock.symlink_to(victim)
    else:
        lock.write_text("unsafe")
        lock.chmod(0o666)
    with pytest.raises(auth.ChatGPTAuthorizationError):
        auth.access_token(path)
    assert victim.read_text() == "must survive"


@pytest.mark.parametrize("kind", ["hardlink", "symlink"])
def test_private_profile_rejects_linked_credentials(tmp_path, kind):
    victim = tmp_path / "original.json"
    profile(victim)
    path = tmp_path / "profile.json"
    if kind == "hardlink":
        os.link(victim, path)
    else:
        path.symlink_to(victim)
    with pytest.raises(auth.ChatGPTAuthorizationError):
        auth.read_private(path)


def test_profile_rejects_writable_ancestor_before_opening_lock(tmp_path):
    ancestor = tmp_path / "shared"
    path = ancestor / "private" / "profile.json"
    profile(path)
    ancestor.chmod(0o777)
    with pytest.raises(auth.ChatGPTAuthorizationError):
        auth.access_token(path)
    assert not path.with_name(path.name + ".lock").exists()


@pytest.mark.parametrize("shared_writable", [False, True])
def test_symlink_chain_checks_intermediate_directories(tmp_path, shared_writable):
    target = tmp_path / "private" / "profile.json"
    profile(target)
    shared = tmp_path / "intermediate"
    shared.mkdir(mode=0o700)
    (shared / "entry").symlink_to(target.parent, target_is_directory=True)
    alias = tmp_path / "alias"
    alias.symlink_to(shared / "entry", target_is_directory=True)
    path = alias / target.name
    if shared_writable:
        shared.chmod(0o777)
        with pytest.raises(auth.ChatGPTAuthorizationError):
            auth.access_token(path)
    else:
        assert auth.access_token(path) == "synthetic"


def test_owner_controlled_legacy_readable_lock_still_works(tmp_path):
    path = tmp_path / "profile.json"
    profile(path)
    lock = path.with_name(path.name + ".lock")
    lock.touch(mode=0o644)
    assert auth.access_token(path) == "synthetic"


def test_private_profile_rejects_fifo_before_open(tmp_path, monkeypatch):
    path = tmp_path / "profile.json"
    os.mkfifo(path, 0o600)
    with monkeypatch.context() as patch:
        patch.setattr(auth.os, "open", lambda *a, **k: pytest.fail("Must not open a FIFO"))
        with pytest.raises(auth.ChatGPTAuthorizationError):
            auth.read_private(path)


def test_pool_lock_rejects_linked_file_before_recording_cooldown(tmp_path, monkeypatch):
    monkeypatch.setenv("LUMIBOT_CHATGPT_HOME", str(tmp_path))
    path = tmp_path / "profile.json"
    profile(path)
    victim = tmp_path / "victim"
    victim.write_text("must survive")
    victim.chmod(0o600)
    os.link(victim, tmp_path / "pool.lock")
    with pytest.raises(auth.ChatGPTAuthorizationError):
        auth.mark_exhausted(path)
    assert victim.read_text() == "must survive"
    assert not (tmp_path / "pool.json").exists()


@pytest.mark.parametrize("target", ["lock", "ledger"])
def test_run_files_are_rechecked_after_budget_creation(tmp_path, target):
    from lumibot.components.agents.chatgpt_run import RunBudget
    path = tmp_path / "run.json"
    auth.save_private(path, {"credential_kind": "run", "run_id": "synthetic",
        "status": "connected", "scopes": [auth.PLAN_SCOPE], "access_token": "synthetic",
        "expires_at": time.time() + 600, "model": "gpt-6-luna",
        "limits": {"calls": 2, "input_tokens": 1000, "output_tokens": 1000}})
    budget = RunBudget(path)
    victim = tmp_path / "victim"
    original = budget.path.read_bytes() if target == "ledger" else b"must survive"
    victim.write_bytes(original)
    victim.chmod(0o600)
    attacked = budget.path if target == "ledger" else budget.path.with_name(budget.path.name + ".lock")
    attacked.unlink()
    os.link(victim, attacked)
    before = budget.path.read_bytes()
    with pytest.raises(auth.ChatGPTAuthorizationError):
        budget.reserve("gpt-6-luna", input_bound=100, output_bound=100)
    assert victim.read_bytes() == original
    assert budget.path.read_bytes() == before
