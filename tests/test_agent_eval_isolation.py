import os

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
    assert set(os.environ) == {"PATH", "GEMINI_API_KEY", "GOOGLE_API_KEY", "LUMIBOT_DISABLE_DOTENV", "IS_BACKTESTING"}
    assert os.environ["LUMIBOT_DISABLE_DOTENV"] == "true"


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
