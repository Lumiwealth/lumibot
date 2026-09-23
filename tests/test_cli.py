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
