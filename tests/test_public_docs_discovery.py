from pathlib import Path
from types import SimpleNamespace

import importlib.util


def _load_docs_config():
    config_path = Path(__file__).resolve().parents[1] / "docsrc" / "conf.py"
    spec = importlib.util.spec_from_file_location("lumibot_docs_conf", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sitemap_lastmod_uses_committed_source_date(monkeypatch, tmp_path):
    docs_config = _load_docs_config()
    source = tmp_path / "guide.rst"
    source.write_text("Guide\n=====\n", encoding="utf-8")

    monkeypatch.setattr(
        docs_config.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout="2026-07-01\n"),
    )

    assert docs_config._source_lastmod(source, tmp_path) == "2026-07-01"


def test_comparison_docs_are_publicly_discoverable():
    repo_root = Path(__file__).resolve().parents[1]
    hub = (repo_root / "docsrc" / "ai_trading_project_comparison.rst").read_text(encoding="utf-8")
    lean = (repo_root / "docsrc" / "lumibot_vs_lean.rst").read_text(encoding="utf-8")

    assert "lumibot_vs_lean" in hub
    assert "QuantConnect LEAN documentation" in lean
    assert "Capabilities on this page were checked" in lean
    assert "July 28, 2026" in lean
