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
from lumibot.components.agents import AgentManager, botspot_hint

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
    "LUMIBOT_HIDE_BOTSPOT_TIP",
    "BOTSPOT_API_KEY",
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


def test_connect_paste_without_terminal_explains_the_steps(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin.isatty", lambda: False, raising=False)
    env_file = tmp_path / ".env"
    assert cli.main(["connect", "botspot", "--paste", "--env-file", str(env_file), "--no-browser"]) == 2
    out = capsys.readouterr()
    combined = out.out + out.err
    assert "https://botspot.trade/account-settings/api-keys?" in combined
    assert "--token" in combined
    assert not env_file.exists()



# --- Startup tip (Rob, 2026-10-23) -------------------------------------------
# When LumiBot starts a backtest or live run and BotSpot is not connected, show
# one clean block that says the BotSpot MCP is free and how to connect it.
# Once per run, never crash, LUMIBOT_HIDE_BOTSPOT_TIP=1 silences it.


class _Log:
    def __init__(self):
        self.lines = []

    def warning(self, message, *args, **kwargs):
        self.lines.append(message)


def test_startup_tip_says_free_and_how_to_connect_and_hide():
    message = botspot_hint.startup_tip_message()
    assert "BotSpot MCP is not connected" in message
    assert "free" in message.lower()
    assert "https://botspot.trade/agents?" in message
    assert "utm_source=lumibot" in message and "utm_content=startup_tip" in message
    assert "lumibot connect botspot" in message
    assert "LUMIBOT_HIDE_BOTSPOT_TIP=1" in message
    assert "paid plan" not in message.lower()
    assert "you must" not in message.lower()


def test_startup_tip_shows_once_per_run():
    log = _Log()
    assert botspot_hint.show_startup_tip(log) is True
    assert botspot_hint.show_startup_tip(log) is False
    assert len(log.lines) == 1


def test_startup_tip_replaces_the_agent_hint_in_the_same_run():
    botspot_hint.show_startup_tip(_Log())
    strategy = _Strategy()
    AgentManager(strategy).create(name="a", _runtime=_Runtime())
    assert _notices(strategy) == []


@pytest.mark.parametrize(
    "name,value",
    [
        ("LUMIBOT_HIDE_BOTSPOT_TIP", "1"),
        ("LUMIBOT_NO_BOTSPOT_HINT", "1"),
        ("CI", "true"),
        ("BOTSPOT_RESEARCH_MCP_TOKEN", "botspot_key"),
        ("BOTSPOT_RESEARCH_MCP_RENEW_URL", "https://example.invalid/renew"),
        ("BOTSPOT_API_KEY", "botspot_key"),
    ],
)
def test_startup_tip_is_silent_when_hidden_in_ci_or_connected(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    log = _Log()
    assert botspot_hint.show_startup_tip(log) is False
    assert log.lines == []


def test_startup_tip_never_crashes():
    class _Broken:
        def warning(self, *args, **kwargs):
            raise RuntimeError("logger is broken")

    assert botspot_hint.show_startup_tip(_Broken()) is False


def test_trader_run_all_shows_the_startup_tip(monkeypatch):
    from lumibot.traders import Trader

    calls = []
    monkeypatch.setattr(botspot_hint, "show_startup_tip", lambda *a, **k: calls.append(1) or True)
    with pytest.raises(RuntimeError, match="No strategies to run"):
        Trader(backtest=True).run_all()
    assert calls == [1]
# ---- device login: no website trip, no copy and paste (Rob, 2026-10-24) ----


class _FakeDeviceServer:
    """Answers like BotSpot's device endpoints, without a network."""

    def __init__(self, pending_polls=1, final=None):
        self.pending_polls = pending_polls
        self.final = final or {
            "api_key": "botspot_devicekey123",
            "key_prefix": "botspot_devi",
            "preset": "lumibot_agent",
            "research_mcp_url": "https://api.botspot.trade/research-mcp",
        }
        self.started = []
        self.polls = 0

    def start(self, api_base, client_name):
        self.started.append((api_base, client_name))
        return {
            "device_code": "device-secret-abc",
            "user_code": "BCDF-GHJK",
            "verification_uri": "https://botspot.trade/account-settings/api-keys",
            "verification_uri_complete": "https://botspot.trade/account-settings/api-keys?device=BCDF-GHJK",
            "expires_in": 600,
            "interval": 5,
        }

    def poll(self, api_base, device_code):
        assert device_code == "device-secret-abc"
        self.polls += 1
        if self.polls <= self.pending_polls:
            return {"error": "authorization_pending"}
        return self.final


def _use_fake_device(monkeypatch, server):
    monkeypatch.setattr(botspot_hint, "start_device_login", server.start)
    monkeypatch.setattr(botspot_hint, "poll_device_login", server.poll)
    sleeps = []
    monkeypatch.setattr(botspot_hint, "_sleep", sleeps.append)
    return sleeps


def test_connect_without_token_logs_in_with_a_device_code(tmp_path, monkeypatch, capsys):
    server = _FakeDeviceServer(pending_polls=2)
    sleeps = _use_fake_device(monkeypatch, server)
    env_file = tmp_path / ".env"
    assert cli.main(["connect", "botspot", "--env-file", str(env_file), "--no-browser"]) == 0
    out = capsys.readouterr().out
    # The person sees the code, the page, and that their AI app can approve it.
    assert "BCDF-GHJK" in out
    assert "https://botspot.trade/account-settings/api-keys?device=BCDF-GHJK" in out
    assert "approve" in out.lower()
    # The key goes straight to .env and is never printed.
    text = env_file.read_text()
    assert "BOTSPOT_RESEARCH_MCP_TOKEN=botspot_devicekey123\n" in text
    assert "BOTSPOT_RESEARCH_MCP_URL=https://api.botspot.trade/research-mcp\n" in text
    assert "botspot_devicekey123" not in out
    assert stat.S_IMODE(os.stat(env_file).st_mode) == 0o600
    assert server.started == [("https://api.botspot.trade", "LumiBot CLI")]
    assert server.polls == 3
    assert sleeps == [5, 5, 5]


def test_device_login_honors_slow_down(tmp_path, monkeypatch):
    server = _FakeDeviceServer(pending_polls=0)
    answers = iter([{"error": "slow_down"}, server.final])
    monkeypatch.setattr(botspot_hint, "start_device_login", server.start)
    monkeypatch.setattr(botspot_hint, "poll_device_login", lambda base, code: next(answers))
    sleeps = []
    monkeypatch.setattr(botspot_hint, "_sleep", sleeps.append)
    assert cli.main(["connect", "botspot", "--env-file", str(tmp_path / ".env"), "--no-browser"]) == 0
    assert sleeps == [5, 10]


def test_device_login_denied_or_expired_writes_nothing(tmp_path, monkeypatch, capsys):
    for error, word in (("access_denied", "denied"), ("expired_token", "expired")):
        server = _FakeDeviceServer(pending_polls=0, final={"error": error})
        _use_fake_device(monkeypatch, server)
        env_file = tmp_path / f"{error}.env"
        assert cli.main(["connect", "botspot", "--env-file", str(env_file), "--no-browser"]) == 1
        assert not env_file.exists()
        assert word in capsys.readouterr().err.lower()


def test_device_login_ignores_a_research_url_on_another_host(tmp_path, monkeypatch):
    server = _FakeDeviceServer(
        pending_polls=0,
        final={"api_key": "botspot_k", "research_mcp_url": "https://evil.example/research-mcp"},
    )
    _use_fake_device(monkeypatch, server)
    env_file = tmp_path / ".env"
    assert cli.main(["connect", "botspot", "--env-file", str(env_file), "--no-browser"]) == 0
    assert "BOTSPOT_RESEARCH_MCP_URL=https://api.botspot.trade/research-mcp\n" in env_file.read_text()


def test_device_login_uses_a_configured_botspot_api(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTSPOT_API_URL", "https://api.dev.botspot.trade/")
    server = _FakeDeviceServer(
        pending_polls=0,
        final={"api_key": "botspot_k", "research_mcp_url": "https://api.dev.botspot.trade/research-mcp"},
    )
    _use_fake_device(monkeypatch, server)
    env_file = tmp_path / ".env"
    assert cli.main(["connect", "botspot", "--env-file", str(env_file), "--no-browser"]) == 0
    assert server.started[0][0] == "https://api.dev.botspot.trade"
    assert "BOTSPOT_RESEARCH_MCP_URL=https://api.dev.botspot.trade/research-mcp\n" in env_file.read_text()


def test_device_http_helpers_speak_rfc8628_json():
    """The real request code against a local HTTP server: JSON in, JSON out, 400 errors parsed."""
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.append((self.path, body))
            if self.path.endswith("/device/code"):
                status, answer = 200, {"device_code": "d", "user_code": "BCDF-GHJK", "interval": 5}
            else:
                status, answer = 400, {"error": "authorization_pending"}
            data = json.dumps(answer).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    httpd = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{httpd.server_address[1]}"
        started = botspot_hint.start_device_login(base, "LumiBot CLI")
        assert started["user_code"] == "BCDF-GHJK"
        assert botspot_hint.poll_device_login(base, "d") == {"error": "authorization_pending"}
    finally:
        httpd.shutdown()
    assert seen == [
        ("/mcp-api-keys/device/code", {"client_name": "LumiBot CLI", "preset": "lumibot_agent"}),
        ("/mcp-api-keys/device/token", {"device_code": "d"}),
    ]
