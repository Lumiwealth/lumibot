"""The optional BotSpot research hint and `lumibot connect botspot`.

Rob, 2026-10-20: LumiBot should tell users, and the coding agents reading
their logs, that the free BotSpot research connection exists. It must show
once per process, stay silent in CI, honor an off switch, and carry a tracked
link so signups from it can be counted.
"""

import os
import stat

import pytest

from lumibot import cli
from lumibot.components.agents import AgentManager
from lumibot.components.agents import botspot_hint

_QUIET_ENVS = (
    "LUMIBOT_NO_BOTSPOT_HINT",
    "DO_NOT_TRACK",
    "CI",
    "GITHUB_ACTIONS",
    "GITLAB_CI",
    "BUILDKITE",
    "CIRCLECI",
    "JENKINS_URL",
    "TF_BUILD",
    "BOTSPOT_RESEARCH_MCP_URL",
    "BOTSPOT_RESEARCH_MCP_TOKEN",
    "BOTSPOT_RESEARCH_MCP_RENEW_URL",
)


@pytest.fixture(autouse=True)
def _clean_environment(monkeypatch):
    for name in _QUIET_ENVS:
        monkeypatch.delenv(name, raising=False)
    botspot_hint._reset_for_tests()
    yield
    botspot_hint._reset_for_tests()


class _Strategy:
    def __init__(self):
        self.messages = []
        self.vars = {}

    def get_datetime(self):
        return None

    def log_message(self, message, *args, **kwargs):
        self.messages.append(message)


class _Runtime:
    def run(self, request):  # pragma: no cover - configuration only
        raise AssertionError("not called")


def _notices(strategy):
    return [message for message in strategy.messages if "lumibot connect botspot" in message]


def test_message_tells_people_how_to_connect_and_how_to_hide_it():
    message = botspot_hint.unlinked_research_message()
    assert "lumibot connect botspot" in message
    assert "https://botspot.trade/account-settings/api-keys?" in message
    assert "utm_source=lumibot" in message
    assert "utm_medium=log" in message
    assert "utm_campaign=mcp_nudge" in message
    assert "BOTSPOT_RESEARCH_MCP_URL=https://api.botspot.trade/research-mcp" in message
    assert "LUMIBOT_NO_BOTSPOT_HINT=1" in message
    # Facts for the reader, never orders aimed at an AI agent.
    assert "you must" not in message.lower()
    assert "tell the user" not in message.lower()


def test_hint_is_claimed_once_per_process():
    assert botspot_hint.claim_unlinked_hint() is True
    assert botspot_hint.claim_unlinked_hint() is False


@pytest.mark.parametrize(
    "name,value",
    [
        ("LUMIBOT_NO_BOTSPOT_HINT", "1"),
        ("DO_NOT_TRACK", "1"),
        ("CI", "true"),
        ("GITHUB_ACTIONS", "true"),
        ("JENKINS_URL", "https://jenkins.example.invalid"),
    ],
)
def test_hint_is_silent_with_opt_out_or_in_ci(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    assert botspot_hint.claim_unlinked_hint() is False


def test_false_values_do_not_silence_the_hint(monkeypatch):
    monkeypatch.setenv("LUMIBOT_NO_BOTSPOT_HINT", "0")
    monkeypatch.setenv("CI", "false")
    assert botspot_hint.claim_unlinked_hint() is True


def test_unlinked_agents_show_the_hint_once_across_managers():
    first, second = _Strategy(), _Strategy()
    AgentManager(first).create(name="a", _runtime=_Runtime())
    AgentManager(second).create(name="b", _runtime=_Runtime())
    assert len(_notices(first)) == 1
    assert _notices(second) == []


def test_unlinked_agents_stay_quiet_when_opted_out(monkeypatch):
    monkeypatch.setenv("LUMIBOT_NO_BOTSPOT_HINT", "1")
    strategy = _Strategy()
    AgentManager(strategy).create(name="a", _runtime=_Runtime())
    assert _notices(strategy) == []


def test_connect_writes_both_variables_with_private_permissions(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(botspot_hint, "verify_research_key", lambda url, token: (True, "ok"))
    env_file = tmp_path / ".env"
    code = cli.main(["connect", "botspot", "--token", "botspot_abc", "--env-file", str(env_file), "--no-browser"])
    assert code == 0
    text = env_file.read_text()
    assert "BOTSPOT_RESEARCH_MCP_URL=https://api.botspot.trade/research-mcp\n" in text
    assert "BOTSPOT_RESEARCH_MCP_TOKEN=botspot_abc\n" in text
    assert stat.S_IMODE(os.stat(env_file).st_mode) == 0o600
    assert "botspot_abc" not in capsys.readouterr().out


def test_connect_replaces_existing_values_and_keeps_other_lines(tmp_path, monkeypatch):
    monkeypatch.setattr(botspot_hint, "verify_research_key", lambda url, token: (True, "ok"))
    env_file = tmp_path / ".env"
    env_file.write_text("ALPACA_API_KEY=keep\nBOTSPOT_RESEARCH_MCP_TOKEN=old\n")
    assert cli.main(["connect", "botspot", "--token", "new", "--env-file", str(env_file), "--no-browser"]) == 0
    text = env_file.read_text()
    assert "ALPACA_API_KEY=keep\n" in text
    assert "old" not in text
    assert text.count("BOTSPOT_RESEARCH_MCP_TOKEN=") == 1


def test_connect_rejected_key_writes_nothing(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(botspot_hint, "verify_research_key", lambda url, token: (False, "HTTP 401"))
    env_file = tmp_path / ".env"
    assert cli.main(["connect", "botspot", "--token", "bad", "--env-file", str(env_file), "--no-browser"]) == 1
    assert not env_file.exists()
    assert "HTTP 401" in capsys.readouterr().err


def test_connect_without_token_and_no_terminal_explains_the_steps(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False, raising=False)
    env_file = tmp_path / ".env"
    assert cli.main(["connect", "botspot", "--env-file", str(env_file), "--no-browser"]) == 2
    out = capsys.readouterr()
    combined = out.out + out.err
    assert "https://botspot.trade/account-settings/api-keys?" in combined
    assert "--token" in combined
    assert not env_file.exists()
