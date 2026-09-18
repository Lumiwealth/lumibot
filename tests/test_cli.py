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
        assert "GEMINI_API_KEY" in source

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
