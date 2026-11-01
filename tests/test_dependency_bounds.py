from __future__ import annotations

import ast
from pathlib import Path
from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_NUMPY_REQUIREMENT = "numpy>=1.20.0,<2.5.0"


def _install_requires_from_setup_py() -> list[str]:
    tree = ast.parse((ROOT / "setup.py").read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        is_setup_call = (
            (isinstance(func, ast.Attribute) and func.attr == "setup")
            or (isinstance(func, ast.Name) and func.id == "setup")
        )
        if not is_setup_call:
            continue
        for keyword in node.keywords:
            if keyword.arg == "install_requires":
                return ast.literal_eval(keyword.value)
    raise AssertionError("setup.py does not define install_requires")


def test_numpy_dependency_cap_prevents_market_calendar_warning_flood():
    install_requires = _install_requires_from_setup_py()
    requirements = [
        line.strip()
        for line in (ROOT / "requirements.txt").read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    assert EXPECTED_NUMPY_REQUIREMENT in install_requires
    assert EXPECTED_NUMPY_REQUIREMENT in requirements


def test_litellm_and_mcp_requirements_share_the_supported_sdk_generation():
    for values in [_install_requires_from_setup_py(), (ROOT / "requirements.txt").read_text().splitlines()]:
        dependencies = {req.name: req for value in values if value.strip() and not value.lstrip().startswith("#") for req in [Requirement(value.split("#", 1)[0].strip())]}
        # LiteLLM 1.106 imports MCP 2 catalog types on the Responses path. LumiBot
        # currently uses the MCP 1 SDK; both clients must resolve together.
        assert "1.105.0" in dependencies["litellm"].specifier
        assert "1.106.0" not in dependencies["litellm"].specifier
        assert "1.30.0" in dependencies["mcp"].specifier
