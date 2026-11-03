import os
from pathlib import Path

import pytest

from scripts.agent_eval_isolation import assert_fixture_request, configure_fixture_environment, fixture_network_boundary


def test_fixture_process_keeps_only_inference_credentials(monkeypatch, tmp_path):
    monkeypatch.setattr(
        os,
        "environ",
        {
            "PATH": "/bin",
            "GEMINI_API_KEY": "synthetic",
            "POLYMARKET_PRIVATE_KEY": "must-not-leave",
            "AWS_ACCESS_KEY_ID": "must-not-leave",
            "LUMIBOT_AI_GATEWAY_TOKEN": "must-not-leave",
        },
    )
    configure_fixture_environment(tmp_path)
    assert set(os.environ) == {
        "PATH",
        "LUMIBOT_DISABLE_DOTENV",
        "IS_BACKTESTING",
        "LITELLM_LOCAL_MODEL_COST_MAP",
    }
    assert os.environ["LUMIBOT_DISABLE_DOTENV"] == "true"


def test_fixture_process_strips_paid_openai_keys(monkeypatch, tmp_path):
    monkeypatch.setattr(
        os,
        "environ",
        {"PATH": "/bin", "OPENAI_API_KEY": "synthetic-openai", "AWS_ACCESS_KEY_ID": "must-not-leave"},
    )
    configure_fixture_environment(tmp_path)
    assert set(os.environ) == {
        "PATH",
        "LUMIBOT_DISABLE_DOTENV",
        "IS_BACKTESTING",
        "LITELLM_LOCAL_MODEL_COST_MAP",
    }


def test_fixture_cache_and_traces_use_producer_owned_output(monkeypatch, tmp_path):
    monkeypatch.setattr(os, "environ", {"PATH": "/bin", "LUMIBOT_CACHE_FOLDER": "/shared-cache"})
    output = tmp_path / "owned-output"
    configure_fixture_environment(tmp_path, artifact_root=output)
    assert os.environ["LUMIBOT_CACHE_FOLDER"] == str(output / "runtime_cache")


def test_openai_responses_inference_is_allowed_but_other_openai_paths_are_not():
    assert_fixture_request("https://api.openai.com/v1/responses", "POST")
    for url, method in (
        ("https://api.openai.com/v1/chat/completions", "POST"),
        ("https://api.openai.com/v1/files", "POST"),
        ("https://api.openai.com/v1/responses", "GET"),
        ("http://api.openai.com/v1/responses", "POST"),
        ("https://api.openai.com.evil.test/v1/responses", "POST"),
    ):
        with pytest.raises(RuntimeError, match="external boundary"):
            assert_fixture_request(url, method)


@pytest.mark.parametrize(
    "url",
    [
        "https://clob.polymarket.com/data/orders?secret=hidden",
        "https://paper-api.alpaca.markets/v2/orders",
        "https://generativelanguage.googleapis.com.evil.test/v1:generateContent",
        "http://generativelanguage.googleapis.com/v1:generateContent",
        "https://api.botspot.trade/anything",
    ],
)
def test_external_brokers_and_unknown_hosts_fail_before_transport(url):
    import requests

    with fixture_network_boundary(), pytest.raises(RuntimeError, match="external boundary") as error:
        requests.post(url)
    assert url not in str(error.value)


def test_paid_provider_inference_and_count_endpoints_are_rejected():
    for path in ("generateContent", "streamGenerateContent", "countTokens"):
        with pytest.raises(RuntimeError, match="external boundary"):
            assert_fixture_request(f"https://generativelanguage.googleapis.com/v1beta/models/gemini:{path}", "POST")


def test_pytest_bootstrap_respects_explicit_dotenv_isolation():
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    code = (
        "from unittest.mock import patch; import runpy; "
        "with_patch = patch('dotenv.load_dotenv', side_effect=RuntimeError('dotenv must not load')); "
        "with_patch.start(); runpy.run_path('tests/conftest.py')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=root,
        env={"PATH": os.environ.get("PATH", ""), "LUMIBOT_DISABLE_DOTENV": "true"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_production_fixture_serves_builtin_sec_tools_offline(monkeypatch, tmp_path):
    """The fixture must not read a developer's SEC cache or reach sec.gov.

    On GitHub the built-in get_filings hit the network boundary; locally it read
    ~/.lumibot/cache/sec. Both runs must see the same recorded SEC data.
    """
    from scripts.agent_eval_production_fixture import ProductionFixture
    from scripts.run_agent_evals import build_fixture

    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("LUMIBOT_SEC_CACHE_DIR", raising=False)
    production = ProductionFixture(build_fixture("research_available"))
    try:
        cache_dir = Path(production.strategy.fundamentals.cache_dir)
        assert production.root in cache_dir.parents
        with fixture_network_boundary():
            result = production.strategy.fundamentals.get_filings("ACME", form="10-Q", as_of="2026-08-11")
    finally:
        production.close()

    assert result["available"] is False
    assert result["reason"] == "no_sec_cik"
    assert result["filings"] == []


def test_fixture_process_uses_litellm_bundled_price_map(monkeypatch, tmp_path):
    # litellm >= 1.102 downloads its model price map from GitHub on import. The
    # fixture boundary correctly rejects that GET, which made every GPT-6 Luna
    # eval call error in CI (release run for v4.5.92 and agent-evals run
    # 35964520979). The eval process must use litellm's bundled map instead.
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test-key")
    monkeypatch.delenv("LITELLM_LOCAL_MODEL_COST_MAP", raising=False)
    configure_fixture_environment(tmp_path)
    assert os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] == "True"


def test_boundary_lets_an_in_memory_fixture_website_answer_but_not_the_real_web():
    # The web_documents eval serves a fake filing site from memory. Those
    # requests never leave the process; a real HTTP transport stays blocked.
    import httpx

    from lumibot.components.agents.web_tools import WebClient

    fake_site = WebClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, text="index")),
        resolver=lambda host: ["93.184.216.34"],
    )
    real_web = WebClient(resolver=lambda host: ["93.184.216.34"])
    with fixture_network_boundary():
        assert fake_site.request("GET", "https://filings.example.gov/2026FD.zip")["text"] == "index"
        with pytest.raises(RuntimeError, match="external boundary"):
            real_web.request("GET", "https://filings.example.gov/2026FD.zip")


def test_fixture_search_uses_empty_external_response_without_optional_backend(monkeypatch):
    from lumibot.components.agents import builtins

    def unavailable():
        raise RuntimeError("Optional external search backend is not installed")
    monkeypatch.setattr(builtins, "_web_search_backend", unavailable)
    with fixture_network_boundary():
        result = builtins._run_web_search(query="site:notes.example.net widget demand")
    assert result == {"query": "site:notes.example.net widget demand", "region": "wt-wt", "count": 0, "results": []}
    assert builtins._web_search_backend is unavailable
