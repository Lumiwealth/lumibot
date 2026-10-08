"""Copyable AI examples must not silently override the documented Luna default."""

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "lumibot" / "example_strategies"
LUNA = "openai/gpt-6-luna"
SOURCES = sorted(path for path in EXAMPLES.glob("*.py") if path.name.startswith(("ai_", "agent_")))


def _default_model(value, assignments):
    if isinstance(value, ast.Constant):
        return value.value
    if isinstance(value, ast.Name):
        return _default_model(assignments[value.id], assignments)
    if isinstance(value, ast.Call) and ast.unparse(value.func) == "os.environ.get":
        # Explicit user overrides stay supported; the no-override path is the contract.
        return _default_model(value.args[1], assignments)
    raise AssertionError(f"Unreviewed example model selection: {ast.unparse(value)}")


@pytest.mark.parametrize("path", SOURCES, ids=lambda path: path.name)
def test_every_explicit_example_model_defaults_to_luna(path):
    tree = ast.parse(path.read_text())
    assignments = {
        target.id: node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
        for keyword in call.keywords:
            if keyword.arg in {"model", "default_model"}:
                assert _default_model(keyword.value, assignments) == LUNA, path.name


@pytest.mark.parametrize("variant", ["", "_leveraged"])
def test_ray_dalio_examples_match_challenge_reasoning(variant):
    tree = ast.parse((EXAMPLES / f"ai_trading_team_ray_dalio_idea_meritocracy{variant}.py").read_text())
    agents = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call) and ast.unparse(node.func) == "self.agents.create"
    ]
    assert len(agents) == 5
    for call in agents:
        settings = {keyword.arg: keyword.value for keyword in call.keywords}
        assert ast.literal_eval(settings["model"]) == LUNA
        assert "reasoning_effort" in settings, "Challenge defaults need explicit high reasoning"
        assert ast.literal_eval(settings["reasoning_effort"]) == "high"


@pytest.mark.parametrize("page", ["citadel_sector_pods", "ray_dalio_idea_meritocracy"])
def test_team_run_instructions_require_openai_not_gemini(page):
    text = (ROOT / "docsrc" / f"agents_example_{page}.rst").read_text()
    instructions = text.split("Run it yourself\n", 1)[1]
    assert "OPENAI_API_KEY" in instructions
    assert "GEMINI_API_KEY" not in instructions


def test_documented_prompt_cache_probe_also_defaults_to_luna():
    tree = ast.parse((ROOT / "scripts" / "run_agent_prompt_cache_probe.py").read_text())
    arguments = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "add_argument" and node.args
        and isinstance(node.args[0], ast.Constant) and node.args[0].value == "--model"
    ]
    assert len(arguments) == 1
    default = next(keyword.value for keyword in arguments[0].keywords if keyword.arg == "default")
    assert _default_model(default, {}) == LUNA


def test_legacy_m2_filenames_are_not_documented_as_different_model_defaults():
    text = (ROOT / "docsrc" / "agents_canonical_demos.rst").read_text()
    m2_entry = next(line for line in text.splitlines() if line.startswith("- **M2 Liquidity**"))
    assert "same bot on other AI models" not in m2_entry
    assert "Luna" in m2_entry
