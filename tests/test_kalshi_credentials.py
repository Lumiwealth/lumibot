"""Exercise eager/lazy standard credentials loading in isolated interpreters."""

import os
import subprocess
import sys

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


@pytest.mark.parametrize("lazy", ["true", "false"])
def test_standard_environment_constructs_kalshi_without_network(lazy):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    env = dict(os.environ)
    env.update(
        TRADING_BROKER="KALSHI",
        DATA_SOURCE="KALSHI",
        IS_BACKTESTING="false",
        KALSHI_API_KEY_ID="generated-unit-test",
        KALSHI_PRIVATE_KEY=pem,
        KALSHI_IS_DEMO="true",
        KALSHI_SUBACCOUNT="0",
        LUMIBOT_CONNECT_STREAM="false",
        LUMIBOT_LAZY_CREDENTIALS=lazy,
        LUMIBOT_DISABLE_DOTENV="1",
        LUMIBOT_DISABLE_DOTENV_LOCAL="1",
    )
    code = """
from unittest.mock import patch
with patch('httpx.Client.request', side_effect=AssertionError('unexpected network call')):
    import lumibot.credentials as credentials
    from lumibot.brokers import Kalshi
    from lumibot.data_sources import KalshiData
    broker = credentials.BROKER
    assert isinstance(broker, Kalshi)
    assert isinstance(credentials.DATA_SOURCE, KalshiData)
    assert broker.data_source is credentials.DATA_SOURCE
    assert broker._client.is_demo
    assert not hasattr(broker, 'stream')
    broker.cleanup_streams()
"""
    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_public_lazy_exports_do_not_import_kalshi_until_requested():
    # Other providers already have lazy-export tests. This isolates the new names.
    code = """
import sys
import lumibot.brokers as brokers
import lumibot.data_sources as data
assert 'Kalshi' in dir(brokers)
assert 'KalshiData' in dir(data)
assert 'lumibot.brokers.kalshi' not in sys.modules
assert 'lumibot.data_sources.kalshi_data' not in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
