import importlib.util
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace


def _load_docs_config():
    config_path = Path(__file__).resolve().parents[1] / "docsrc" / "conf.py"
    sitemap_path = config_path.parent / "_extra" / "sitemap.xml"
    original_sitemap = sitemap_path.read_bytes()
    spec = importlib.util.spec_from_file_location("lumibot_docs_conf", config_path)
    module = importlib.util.module_from_spec(spec)
    original_modules = sys.modules.copy()
    try:
        spec.loader.exec_module(module)
    finally:
        for module_name in set(sys.modules) - set(original_modules):
            sys.modules.pop(module_name, None)
        sys.modules.update(original_modules)
        sitemap_path.write_bytes(original_sitemap)
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


def test_sitemap_lastmod_mtime_fallback_uses_utc(monkeypatch, tmp_path):
    docs_config = _load_docs_config()
    source = tmp_path / "guide.rst"
    source.write_text("Guide\n=====\n", encoding="utf-8")
    timestamp = datetime(2026, 7, 1, 0, 30, tzinfo=timezone.utc).timestamp()
    os.utime(source, (timestamp, timestamp))

    def fail_git_lookup(*_args, **_kwargs):
        raise subprocess.CalledProcessError(1, "git")

    monkeypatch.setattr(docs_config.subprocess, "run", fail_git_lookup)

    assert docs_config._source_lastmod(source, tmp_path) == "2026-07-01"


def test_comparison_docs_are_publicly_discoverable():
    repo_root = Path(__file__).resolve().parents[1]
    hub = (repo_root / "docsrc" / "ai_trading_project_comparison.rst").read_text(encoding="utf-8")
    lean = (repo_root / "docsrc" / "lumibot_vs_lean.rst").read_text(encoding="utf-8")

    assert "lumibot_vs_lean" in hub
    assert "QuantConnect LEAN documentation" in lean
    assert "Capabilities on this page were checked" in lean
    assert "July 28, 2026" in lean
    assert "https://github.com/HKUDS/Vibe-Trading" in hub
    assert "https://github.com/HKUDS/AI-Trader" in hub
    assert "https://github.com/OpenBB-finance/OpenBB" in hub
    assert "https://github.com/microsoft/qlib" in hub
