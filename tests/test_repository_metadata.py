"""Public repository and PyPI metadata that discovery surfaces rely on.

GitHub shows a "Cite this repository" button only when CITATION.cff exists,
the Sponsor button reads .github/FUNDING.yml, and PyPI renders project_urls in
the sidebar. These checks keep those surfaces pointing at live, current pages.
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _setup_kwargs():
    tree = ast.parse((ROOT / "setup.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        func = getattr(node, "func", None)
        name = getattr(func, "id", None) or getattr(func, "attr", None)
        if isinstance(node, ast.Call) and name == "setup":
            return {kw.arg: kw.value for kw in node.keywords}
    raise AssertionError("setup() call not found")


def test_citation_file_names_the_project_and_repository():
    citation = (ROOT / "CITATION.cff").read_text(encoding="utf-8")

    assert "cff-version: 1.2.0" in citation
    assert "title: LumiBot" in citation
    assert "repository-code: https://github.com/Lumiwealth/lumibot" in citation
    assert "license: GPL-3.0" in citation


def test_funding_link_does_not_point_at_a_frozen_version_branch():
    funding = (ROOT / ".github" / "FUNDING.yml").read_text(encoding="utf-8")

    assert "/blob/version/" not in funding
    assert "https://lumibot.lumiwealth.com/PARTNERSHIPS.html" in funding


def test_pypi_project_urls_include_changelog():
    urls = ast.literal_eval(_setup_kwargs()["project_urls"])

    assert urls["Changelog"] == "https://github.com/Lumiwealth/lumibot/blob/dev/CHANGELOG.md"
    assert urls["Documentation"] == "https://lumibot.lumiwealth.com/"


def test_pypi_keywords_cover_ai_agent_and_crypto_exchange_searches():
    keywords = ast.literal_eval(_setup_kwargs()["keywords"])

    for keyword in ("ai-agents", "agentic-trading", "llm", "ccxt"):
        assert keyword in keywords
