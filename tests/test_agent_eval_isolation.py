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
        "GEMINI_API_KEY",
        "GOOGLE_API_KEY",
        "LUMIBOT_DISABLE_DOTENV",
        "IS_BACKTESTING",
        "LITELLM_LOCAL_MODEL_COST_MAP",
    }
    assert os.environ["LUMIBOT_DISABLE_DOTENV"] == "true"


def test_fixture_process_keeps_the_openai_key_for_luna_without_requiring_gemini(monkeypatch, tmp_path):
    monkeypatch.setattr(
        os,
        "environ",
        {"PATH": "/bin", "OPENAI_API_KEY": "synthetic-openai", "AWS_ACCESS_KEY_ID": "must-not-leave"},
    )
    configure_fixture_environment(tmp_path)
    assert set(os.environ) == {
        "PATH",
        "OPENAI_API_KEY",
        "LUMIBOT_DISABLE_DOTENV",
        "IS_BACKTESTING",
        "LITELLM_LOCAL_MODEL_COST_MAP",
    }


def test_openai_responses_inference_is_allowed_but_other_openai_paths_are_not():
    assert_fixture_request("https://api.openai.com/v1/responses", "POST")
    assert_fixture_request("https://api.openai.com/v1/chat/completions", "POST")
    for url, method in (
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


def test_inference_and_count_endpoints_remain_available():
    for path in ("generateContent", "streamGenerateContent", "countTokens"):
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
