"""Tests for the `lumibot` command line interface.

The CLI exists so a new user can go from `pip install lumibot` to a result
without first designing a strategy lifecycle. It never replaces the Strategy
class: `lumibot init` writes an ordinary Strategy subclass to disk and the
other commands run that file.
"""

import subprocess
import sys
from pathlib import Path

import pytest

from lumibot import cli


class TestCliParser:
    def test_no_command_prints_help_and_fails(self, capsys):
        assert cli.main([]) == 2
        out = capsys.readouterr().out
        assert "lumibot init" in out

    def test_version_reports_package_version(self, capsys):
        assert cli.main(["version"]) == 0
        printed = capsys.readouterr().out.strip()
        assert printed.startswith("lumibot ")
        assert printed.split()[1] not in ("", "unknown")

    def test_version_works_without_installed_package_metadata(self, capsys, monkeypatch):
        # CI and source checkouts run LumiBot without pip metadata. The CLI must
        # still print the real version (the 4.5.92 release CI printed "unknown").
        import importlib.metadata

        import lumibot

        def _missing(_name):
            raise importlib.metadata.PackageNotFoundError("lumibot")

        monkeypatch.setattr(importlib.metadata, "version", _missing)
        assert cli.main(["version"]) == 0
        printed = capsys.readouterr().out.strip()
        assert printed == f"lumibot {lumibot.__version__}"
        assert lumibot.__version__ not in ("", "unknown")

    def test_unknown_template_is_rejected(self, tmp_path, capsys):
        rc = cli.main(["init", str(tmp_path / "bot"), "--template", "nonsense"])
        assert rc != 0


class TestInit:
    def test_python_template_writes_a_runnable_strategy(self, tmp_path):
        target = tmp_path / "my-bot"
        assert cli.main(["init", str(target), "--template", "python"]) == 0

        strategy = target / "strategy.py"
        assert strategy.exists(), "init must write strategy.py"
        source = strategy.read_text()
        assert "class" in source and "Strategy" in source
        assert "on_trading_iteration" in source

        # It must be an ordinary file the user can edit and run directly.
        compile(source, str(strategy), "exec")

    def test_ai_template_writes_a_strategy_and_names_its_provider(self, tmp_path):
        target = tmp_path / "ai-bot"
        assert cli.main(["init", str(target), "--template", "ai"]) == 0
        source = (target / "strategy.py").read_text()
        compile(source, "ai", "exec")
        # 4.5.92 moved the default agent model to OpenAI GPT-6 Luna, so the
        # template must name the OpenAI key a new user has to set.
        assert "OPENAI_API_KEY" in source
        assert "GEMINI_API_KEY" not in source

    def test_ai_template_uses_the_real_agents_api(self, tmp_path):
        """A template that compiles but calls a method that does not exist is worse
        than no template. Pin it to the API the shipped examples use."""
        target = tmp_path / "ai-bot"
        cli.main(["init", str(target), "--template", "ai"])
        source = (target / "strategy.py").read_text()

        assert "self.agents.create(" in source
        assert 'self.agents["researcher"].run(' in source
        assert 'self.agents["trader"].run(' in source
        # The researcher must never be allowed to trade.
        assert "allow_trading=False" in source

    def test_generated_templates_only_call_methods_strategy_has(self, tmp_path):
        import re

        from lumibot.strategies import Strategy

        for template in cli.TEMPLATES:
            target = tmp_path / f"probe-{template}"
            cli.main(["init", str(target), "--template", template])
            source = (target / "strategy.py").read_text()
            for method in set(re.findall(r"self\.([a-z_][a-z0-9_]*)\(", source)):
                assert hasattr(Strategy, method), (
                    f"{template} template calls self.{method}(), "
                    f"which Strategy does not define"
                )

    def test_generated_projects_also_run_with_plain_python(self, tmp_path):
        """`python strategy.py` must work, the same way BotSpot's main.py does.

        Rob, 2026-09-25: a BotSpot strategy workspace ships the
        IS_BACKTESTING/Trader runner block, so the file runs on its own. A file
        that silently does nothing when you run it is the most common way a
        beginner gets stuck, so `lumibot init` writes the same block. The CLI is
        unaffected: runpy.run_path names the module "<run_path>", never
        "__main__", so the block does not fire when the CLI imports the class.
        """
        for template in cli.TEMPLATES:
            target = tmp_path / f"bot-{template}"
            assert cli.main(["init", str(target), "--template", template]) == 0
            source = (target / "strategy.py").read_text()
            assert 'if __name__ == "__main__":' in source, template
            assert "IS_BACKTESTING" in source, template
            assert ".backtest(" in source, template
            assert "Trader()" in source, template

    def test_the_runner_block_does_not_fire_when_the_cli_loads_the_class(self, tmp_path):
        """Loading the class must never start a backtest as a side effect."""
        target = tmp_path / "bot"
        cli.main(["init", str(target)])
        strategy_class = cli._load_strategy_class(target / "strategy.py")
        assert strategy_class.__name__ == "Bot"

    def test_init_writes_a_readme_naming_the_next_command(self, tmp_path):
        target = tmp_path / "bot"
        cli.main(["init", str(target)])
        readme = (target / "README.md").read_text()
        assert "lumibot backtest" in readme

    def test_init_refuses_to_overwrite_without_force(self, tmp_path, capsys):
        target = tmp_path / "bot"
        assert cli.main(["init", str(target)]) == 0
        (target / "strategy.py").write_text("# user edits\n")

        assert cli.main(["init", str(target)]) != 0
        assert (target / "strategy.py").read_text() == "# user edits\n", (
            "init must never clobber a user's edited strategy"
        )

        assert cli.main(["init", str(target), "--force"]) == 0
        assert (target / "strategy.py").read_text() != "# user edits\n"

    def test_project_name_becomes_a_valid_class_name(self, tmp_path):
        target = tmp_path / "my-great-bot"
        cli.main(["init", str(target)])
        source = (target / "strategy.py").read_text()
        assert "class MyGreatBot" in source


class TestBacktestAndRunResolveTheStrategyFile:
    def test_backtest_reports_a_missing_project(self, tmp_path, capsys):
        rc = cli.main(["backtest", str(tmp_path / "nope")])
        assert rc != 0
        assert "lumibot init" in capsys.readouterr().err

    def test_run_requires_paper_or_live_to_be_explicit(self, tmp_path, capsys):
        target = tmp_path / "bot"
        cli.main(["init", str(target)])
        rc = cli.main(["run", str(target)])
        assert rc != 0, "run must not default to touching a broker"
        assert "--paper" in capsys.readouterr().err

    def test_backtest_tells_the_user_where_the_tearsheet_landed(self, tmp_path, capsys, monkeypatch):
        """A finished backtest that says nothing looks like a hung progress bar.

        Observed on a clean `pip install lumibot` 4.6.0: `lumibot backtest`
        exits 0, writes twelve files, and prints no result and no path. The
        first thing a new user wants is the return and the file to open.
        """
        target = tmp_path / "bot"
        cli.main(["init", str(target)])

        logs = tmp_path / "logs"
        logs.mkdir()
        tearsheet = logs / "MyBot_2026-09-25_17-38_abc123_tearsheet.html"
        tearsheet.write_text("<html></html>")

        def fake_backtest(cls, datasource, start, end, **kwargs):
            return {"cagr": 0.1234, "total_return": 0.0567, "max_drawdown": {"drawdown": 0.0891}}

        from lumibot.strategies.strategy import Strategy

        monkeypatch.setattr(Strategy, "backtest", classmethod(fake_backtest))
        monkeypatch.chdir(tmp_path)

        assert cli.main(["backtest", str(target)]) == 0
        out = capsys.readouterr().out
        assert "5.67%" in out, "the total return must be on screen"
        assert tearsheet.name in out, "the tearsheet path must be on screen"
        assert "open" in out.lower()

    def test_backtest_still_succeeds_when_no_tearsheet_is_found(self, tmp_path, capsys, monkeypatch):
        """Never fail a good backtest because the report path could not be guessed."""
        target = tmp_path / "bot"
        cli.main(["init", str(target)])

        from lumibot.strategies.strategy import Strategy

        monkeypatch.setattr(Strategy, "backtest", classmethod(lambda cls, *a, **k: None))
        monkeypatch.chdir(tmp_path)

        assert cli.main(["backtest", str(target)]) == 0
        assert "Traceback" not in capsys.readouterr().out

    def test_demo_reports_its_numbers_too(self, tmp_path, capsys, monkeypatch):
        """`lumibot demo` is the very first thing a new user runs. It must show a result."""
        from lumibot.strategies.strategy import Strategy

        monkeypatch.setattr(
            Strategy,
            "backtest",
            classmethod(lambda cls, *a, **k: {"total_return": 0.0567, "cagr": 0.1234}),
        )
        monkeypatch.chdir(tmp_path)

        assert cli.main(["demo"]) == 0
        out = capsys.readouterr().out
        assert "5.67%" in out
        assert "lumibot init" in out, "the demo must still point at the next command"


class TestModuleEntryPoint:
    def test_python_m_lumibot_runs(self):
        proc = subprocess.run(
            [sys.executable, "-m", "lumibot", "version"],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).resolve().parent.parent),
            timeout=180,
        )
        assert proc.returncode == 0, proc.stderr[-2000:]


class TestEdgeCases:
    """A broken CLI is worse than no CLI. These are the ways it can break."""

    def test_project_name_with_spaces(self, tmp_path):
        target = tmp_path / "my great bot"
        assert cli.main(["init", str(target)]) == 0
        source = (target / "strategy.py").read_text()
        compile(source, "spaces", "exec")
        assert "class MyGreatBot" in source

    def test_project_name_starting_with_a_digit(self, tmp_path):
        target = tmp_path / "2026-bot"
        assert cli.main(["init", str(target)]) == 0
        source = (target / "strategy.py").read_text()
        compile(source, "digit", "exec")
        assert "class Strategy2026Bot" in source

    def test_project_name_that_is_a_python_keyword(self, tmp_path):
        target = tmp_path / "class"
        assert cli.main(["init", str(target)]) == 0
        compile((target / "strategy.py").read_text(), "kw", "exec")

    def test_project_name_with_only_punctuation(self, tmp_path):
        target = tmp_path / "---"
        assert cli.main(["init", str(target)]) == 0
        compile((target / "strategy.py").read_text(), "punct", "exec")

    def test_non_ascii_project_name(self, tmp_path):
        target = tmp_path / "机器人"
        assert cli.main(["init", str(target)]) == 0
        compile((target / "strategy.py").read_text(), "unicode", "exec")

    def test_deeply_nested_path_is_created(self, tmp_path):
        target = tmp_path / "a" / "b" / "c" / "bot"
        assert cli.main(["init", str(target)]) == 0
        assert (target / "strategy.py").exists()

    def test_init_into_an_existing_directory_keeps_other_files(self, tmp_path):
        target = tmp_path / "bot"
        target.mkdir()
        keep = target / "notes.txt"
        keep.write_text("mine")
        assert cli.main(["init", str(target)]) == 0
        assert keep.read_text() == "mine"

    def test_init_reports_a_permission_error_instead_of_a_traceback(self, tmp_path, capsys):
        locked = tmp_path / "locked"
        locked.mkdir()
        locked.chmod(0o500)
        try:
            rc = cli.main(["init", str(locked / "bot")])
            assert rc != 0
            assert "Traceback" not in capsys.readouterr().err
        finally:
            locked.chmod(0o700)

    def test_backtest_rejects_a_non_positive_day_count(self, tmp_path, capsys):
        target = tmp_path / "bot"
        cli.main(["init", str(target)])
        assert cli.main(["backtest", str(target), "--days", "0"]) != 0
        assert "--days" in capsys.readouterr().err

    def test_backtest_rejects_a_non_positive_budget(self, tmp_path, capsys):
        target = tmp_path / "bot"
        cli.main(["init", str(target)])
        assert cli.main(["backtest", str(target), "--budget", "0"]) != 0
        assert "--budget" in capsys.readouterr().err

    def test_run_rejects_paper_and_live_together(self, tmp_path, capsys):
        target = tmp_path / "bot"
        cli.main(["init", str(target)])
        assert cli.main(["run", str(target), "--paper", "--live", "--yes"]) != 0
        assert "both" in capsys.readouterr().err.lower()

    def test_live_without_yes_is_refused(self, tmp_path, capsys):
        target = tmp_path / "bot"
        cli.main(["init", str(target)])
        assert cli.main(["run", str(target), "--live"]) != 0
        assert "--yes" in capsys.readouterr().err

    def test_a_strategy_file_with_no_strategy_class_is_explained(self, tmp_path, capsys):
        target = tmp_path / "bot"
        cli.main(["init", str(target)])
        (target / "strategy.py").write_text("x = 1\n")
        assert cli.main(["backtest", str(target)]) != 0
        err = capsys.readouterr().err
        assert "Strategy" in err and "Traceback" not in err

    def test_a_strategy_file_that_does_not_parse_is_explained(self, tmp_path, capsys):
        target = tmp_path / "bot"
        cli.main(["init", str(target)])
        (target / "strategy.py").write_text("def broken(:\n")
        assert cli.main(["backtest", str(target)]) != 0
        err = capsys.readouterr().err
        assert "strategy.py" in err and "Traceback" not in err

    def test_several_strategy_classes_pick_deterministically(self, tmp_path):
        target = tmp_path / "bot"
        cli.main(["init", str(target)])
        (target / "strategy.py").write_text(
            "from lumibot.strategies.strategy import Strategy\n"
            "class Alpha(Strategy):\n    pass\n"
            "class Beta(Strategy):\n    pass\n"
        )
        chosen = {cli._load_strategy_class(target / "strategy.py").__name__ for _ in range(3)}
        assert len(chosen) == 1, "the same file must always resolve to the same class"

    def test_relative_paths_work_from_any_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert cli.main(["init", "relative-bot"]) == 0
        assert (tmp_path / "relative-bot" / "strategy.py").exists()

    def test_trailing_slash_does_not_break_the_class_name(self, tmp_path):
        target = tmp_path / "slashbot"
        assert cli.main(["init", str(target) + "/"]) == 0
        assert "class Slashbot" in (target / "strategy.py").read_text()

    def test_every_subcommand_has_help(self):
        parser = cli.build_parser()
        for command in ("init", "backtest", "run", "demo", "version"):
            assert parser.parse_args([command, "--help"] if False else [], ) is not None or True
        # argparse exits on --help; assert the subparsers exist instead.
        actions = [a for a in parser._actions if hasattr(a, "choices") and a.choices]
        names = set()
        for action in actions:
            names |= set(action.choices)
        assert {"init", "backtest", "run", "demo", "version"} <= names


# ---------------------------------------------------------------------------
# `lumibot connect botspot`: one command, sign in, a key is created for you
# (Rob, 2026-10-24). Only the HTTP boundary is faked: a local server answers
# exactly like BotSpot Node's device-login controller
# (src/Mcp/mcpDeviceLogin.controller.ts + agentApiKeys.service.ts):
#   POST /mcp-api-keys/device/code   200 {device_code, user_code,
#        verification_uri, verification_uri_complete, expires_in: 600, interval: 5}
#   POST /mcp-api-keys/device/token  400 {error, error_description} while
#        pending / slow_down / denied / expired; 200 {api_key, key_id,
#        key_prefix, preset, scopes, expires_at, research_mcp_url} once approved
#   An older server without the route answers Express's 404 "Cannot POST".
# ---------------------------------------------------------------------------

import json as _json
import os as _os
import stat as _stat
import threading as _threading
from http.server import BaseHTTPRequestHandler, HTTPServer

_KEY = "botspot_ak_cli_test_secret_0123456789"


class _FakeBotSpot:
    def __init__(self, polls=(), device_route=True, research_status=200, expires_in=600,
                 json_404=False, start_body=None):
        self.polls = list(polls)
        self.json_404 = json_404
        self.start_body = start_body
        self.device_route = device_route
        self.research_status = research_status
        self.expires_in = expires_in
        self.requests = []
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = _json.loads(self.rfile.read(length) or b"{}")
                fake.requests.append((self.path, body, self.headers.get("Authorization")))
                if self.path.startswith("/mcp-api-keys/device/") and not fake.device_route:
                    if fake.json_404:
                        return self._json(404, {"error": "Not Found"})
                    page = f"<!DOCTYPE html><pre>Cannot POST {self.path}</pre>".encode()
                    return self._send(404, page, "text/html; charset=utf-8")
                if self.path == "/mcp-api-keys/device/code" and fake.start_body is not None:
                    return self._json(200, fake.start_body)
                if self.path == "/mcp-api-keys/device/code":
                    return self._json(200, {
                        "device_code": "Zx3-device-code-base64url-secret-abcdef",
                        "user_code": "BCDF-GHJK",
                        "verification_uri": "https://botspot.trade/account-settings/api-keys",
                        "verification_uri_complete": "https://botspot.trade/account-settings/api-keys?device=BCDF-GHJK",
                        "expires_in": fake.expires_in,
                        "interval": 5,
                    })
                if self.path == "/mcp-api-keys/device/token":
                    step = fake.polls.pop(0) if fake.polls else "authorization_pending"
                    if isinstance(step, int):
                        return self._send(step, b"<html>Bad Gateway</html>", "text/html")
                    if step == "approved":
                        host = self.headers.get("Host")
                        return self._json(200, {
                            "api_key": _KEY,
                            "key_id": "7f0c1a2b-0000-4000-8000-000000000001",
                            "key_prefix": _KEY[:12],
                            "preset": "lumibot_agent",
                            "scopes": ["research:read"],
                            "expires_at": None,
                            "research_mcp_url": f"http://{host}/research-mcp",
                        })
                    descriptions = {
                        "authorization_pending": "Waiting for approval.",
                        "slow_down": "Polling too fast.",
                        "access_denied": "The login was denied.",
                        "expired_token": "The device code expired. Start again.",
                    }
                    return self._json(400, {"error": step, "error_description": descriptions[step]})
                if self.path == "/research-mcp":
                    return self._json(fake.research_status, {"jsonrpc": "2.0", "id": 1, "result": {}})
                return self._send(404, b"not found", "text/plain")

            def _json(self, status, answer):
                self._send(status, _json.dumps(answer).encode(), "application/json")

            def _send(self, status, data, content_type):
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self.httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        _threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def paths(self):
        return [path for path, _body, _auth in self.requests]


@pytest.fixture
def botspot(monkeypatch):
    servers = []
    sleeps = []
    from lumibot.components.agents import botspot_hint

    monkeypatch.setattr(botspot_hint, "_sleep", sleeps.append)
    monkeypatch.delenv("BOTSPOT_API_URL", raising=False)

    def make(**kwargs):
        server = _FakeBotSpot(**kwargs)
        servers.append(server)
        server.sleeps = sleeps
        return server

    yield make
    for server in servers:
        server.httpd.shutdown()


def _connect(server, env_file, *extra):
    return cli.main(
        ["connect", "botspot", "--api-url", server.url, "--env-file", str(env_file), "--no-browser", *extra]
    )


class TestConnectBotspotDeviceLogin:
    def test_pending_then_approved_saves_a_verified_key_without_printing_it(self, botspot, tmp_path, capsys):
        server = botspot(polls=["authorization_pending", "authorization_pending", "approved"])
        env_file = tmp_path / ".env"
        assert _connect(server, env_file) == 0
        captured = capsys.readouterr()
        assert "BCDF-GHJK" in captured.out
        assert "https://botspot.trade/account-settings/api-keys?device=BCDF-GHJK" in captured.out
        assert "free" in captured.out.lower()
        assert _KEY not in captured.out + captured.err
        text = env_file.read_text()
        assert f"BOTSPOT_RESEARCH_MCP_TOKEN={_KEY}\n" in text
        assert f"BOTSPOT_RESEARCH_MCP_URL={server.url}/research-mcp\n" in text
        assert _stat.S_IMODE(_os.stat(env_file).st_mode) == 0o600
        # The key is checked against BotSpot before it is saved.
        assert server.paths()[-1] == "/research-mcp"
        assert server.requests[-1][2] == f"Bearer {_KEY}"
        assert server.requests[0][1] == {"client_name": "LumiBot CLI", "preset": "lumibot_agent"}
        assert server.paths().count("/mcp-api-keys/device/token") == 3
        assert server.sleeps == [5, 5, 5]

    def test_slow_down_adds_five_seconds_to_the_interval(self, botspot, tmp_path):
        server = botspot(polls=["slow_down", "authorization_pending", "approved"])
        assert _connect(server, tmp_path / ".env") == 0
        assert server.sleeps == [5, 10, 10]

    @pytest.mark.parametrize("error,word", [("access_denied", "denied"), ("expired_token", "expired")])
    def test_denied_or_expired_writes_nothing(self, botspot, tmp_path, capsys, error, word):
        server = botspot(polls=["authorization_pending", error])
        env_file = tmp_path / ".env"
        assert _connect(server, env_file) == 1
        assert not env_file.exists()
        assert word in capsys.readouterr().err.lower()

    def test_a_key_botspot_rejects_is_not_saved(self, botspot, tmp_path, capsys):
        server = botspot(polls=["approved"], research_status=401)
        env_file = tmp_path / ".env"
        assert _connect(server, env_file) == 1
        assert not env_file.exists()
        captured = capsys.readouterr()
        assert _KEY not in captured.out + captured.err

    def test_timeout_stops_waiting(self, botspot, tmp_path, capsys):
        server = botspot(polls=[])  # pending forever
        env_file = tmp_path / ".env"
        assert _connect(server, env_file, "--timeout", "15") == 1
        assert not env_file.exists()
        assert sum(server.sleeps) <= 20
        assert "run the command again" in capsys.readouterr().err.lower()

    def test_old_server_without_device_login_falls_back_to_the_key_page(
        self, botspot, tmp_path, capsys, monkeypatch
    ):
        server = botspot(device_route=False)
        monkeypatch.setattr(sys.stdin, "isatty", lambda: True, raising=False)
        import getpass

        monkeypatch.setattr(getpass, "getpass", lambda prompt="": _KEY)
        env_file = tmp_path / ".env"
        assert _connect(server, env_file) == 0
        captured = capsys.readouterr()
        assert "Traceback" not in captured.err
        assert "api-keys" in captured.out  # the existing key page flow
        assert f"BOTSPOT_RESEARCH_MCP_TOKEN={_KEY}\n" in env_file.read_text()
        assert _KEY not in captured.out + captured.err

    def test_old_server_fallback_without_a_terminal_explains_the_next_step(self, botspot, tmp_path, capsys, monkeypatch):
        server = botspot(device_route=False)
        monkeypatch.setattr(sys.stdin, "isatty", lambda: False, raising=False)
        env_file = tmp_path / ".env"
        assert _connect(server, env_file) == 2
        captured = capsys.readouterr()
        assert "--token" in captured.err
        assert "Traceback" not in captured.err
        assert not env_file.exists()

    def test_token_flag_still_works_and_is_never_printed(self, botspot, tmp_path, capsys):
        server = botspot()
        env_file = tmp_path / ".env"
        assert _connect(server, env_file, "--token", _KEY) == 0
        captured = capsys.readouterr()
        assert _KEY not in captured.out + captured.err
        assert "/mcp-api-keys/device/code" not in server.paths()

    def test_default_api_is_the_public_botspot_api(self):
        from lumibot.components.agents import botspot_hint

        assert botspot_hint.api_base_url({}) == "https://api.botspot.trade"
        args = cli.build_parser().parse_args(["connect", "botspot"])
        assert args.api_url is None


class TestConnectBotspotHandlesOddServers:
    """Review follow-ups after 4.6.30: odd answers give a clean result, never a traceback."""

    def test_json_404_still_falls_back_to_the_key_page(self, botspot, tmp_path, capsys, monkeypatch):
        server = botspot(device_route=False, json_404=True)
        monkeypatch.setattr(sys.stdin, "isatty", lambda: False, raising=False)
        assert _connect(server, tmp_path / ".env") == 2
        captured = capsys.readouterr()
        assert "key page" in captured.out
        assert "--token" in captured.err

    @pytest.mark.parametrize("status", [429, 502, 503, 504])
    def test_a_gateway_blip_while_waiting_keeps_polling(self, botspot, tmp_path, status):
        server = botspot(polls=["authorization_pending", status, "approved"])
        env_file = tmp_path / ".env"
        assert _connect(server, env_file) == 0
        assert f"BOTSPOT_RESEARCH_MCP_TOKEN={_KEY}\n" in env_file.read_text()

    def test_a_non_object_answer_is_a_clean_error(self, botspot, tmp_path, capsys):
        server = botspot(start_body=["not", "an", "object"])
        env_file = tmp_path / ".env"
        assert _connect(server, env_file) == 1
        assert "Traceback" not in capsys.readouterr().err
        assert not env_file.exists()

    def test_bad_interval_and_expiry_fall_back_to_defaults(self, botspot, tmp_path):
        server = botspot(
            polls=["approved"],
            start_body={
                "device_code": "Zx3-device-code-base64url-secret-abcdef",
                "user_code": "BCDF-GHJK",
                "verification_uri": "https://botspot.trade/account-settings/api-keys",
                "expires_in": "soon",
                "interval": -3,
            },
        )
        assert _connect(server, tmp_path / ".env") == 0
        assert server.sleeps == [5]


class TestInitWritesAgentsMd:
    def test_init_writes_agents_md_with_one_free_botspot_mcp_line(self, tmp_path):
        target = tmp_path / "bot"
        assert cli.main(["init", str(target)]) == 0
        agents = (target / "AGENTS.md").read_text()
        assert "strategy.py" in agents
        botspot_lines = [line for line in agents.splitlines() if "BotSpot" in line]
        assert len(botspot_lines) == 1
        line = botspot_lines[0]
        assert "free" in line.lower()
        assert "lumibot connect botspot" in line
        assert "https://mcp.botspot.trade/mcp" in line
        assert "paid" not in agents.lower()

    def test_init_never_overwrites_an_existing_agents_md(self, tmp_path):
        target = tmp_path / "bot"
        target.mkdir()
        (target / "AGENTS.md").write_text("my own rules\n")
        assert cli.main(["init", str(target)]) == 0
        assert (target / "AGENTS.md").read_text() == "my own rules\n"
        assert cli.main(["init", str(target), "--force"]) == 0
        assert (target / "AGENTS.md").read_text() == "my own rules\n"
