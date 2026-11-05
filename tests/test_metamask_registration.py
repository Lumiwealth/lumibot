"""Exercise public exports and both real environment-selection startup paths."""

import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest


@pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
@pytest.mark.parametrize("mode", ["perp", "spot"])
@pytest.mark.parametrize("selection", ["broker", "broker-and-data", "data-only"])
def test_native_environment_selection_owns_one_client(tmp_path, lazy, mode, selection):
    env = {key: os.environ[key] for key in ("PATH", "SYSTEMROOT", "TMPDIR", "TEMP") if key in os.environ}
    env.update(
        HOME=str(tmp_path),
        LUMIBOT_DISABLE_DOTENV="1",
        LUMIBOT_LAZY_CREDENTIALS=str(int(lazy)),
        LUMIBOT_CONNECT_STREAM="0",
        LUMIBOT_START_ORDERS_THREAD="0",
        IS_BACKTESTING="false",
        METAMASK_NETWORK="testnet",
        METAMASK_MODE=mode,
        METAMASK_STATE_FILE=str(tmp_path / "broker.sqlite"),
        METAMASK_SESSION_FILE=str(tmp_path / "session.json"),
    )
    if selection != "data-only":
        env["TRADING_BROKER"] = "metamask"
    if selection != "broker":
        env["DATA_SOURCE"] = "metamask"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            dedent("""
                from contextlib import closing
                import os
                from pathlib import Path
                import requests

                def unexpected_network(*args, **kwargs):
                    raise AssertionError("Credential selection must not contact providers")

                requests.sessions.Session.request = unexpected_network
                import lumibot.credentials as credentials
                from lumibot.brokers import MetaMask
                from lumibot.data_sources import MetaMaskData
                from lumibot.tools.metamask.journal import Journal

                assert credentials.MetaMask is MetaMask
                broker, source = credentials.get_default_broker_and_data_source()
                assert credentials.get_default_broker() is broker
                assert credentials.get_default_data_source() is source
                if os.environ.get("TRADING_BROKER"):
                    assert isinstance(broker, MetaMask)
                    assert broker.mode == os.environ["METAMASK_MODE"]
                    assert isinstance(broker.data_source, MetaMaskData)
                    client = broker.client
                    assert broker.data_source.client is client, "Broker and data must share the native client"
                    if os.environ.get("DATA_SOURCE"):
                        assert source is broker.data_source
                    else:
                        assert source is None
                else:
                    assert broker is None
                    assert isinstance(source, MetaMaskData)
                    client = source.client
                assert client.network == "testnet"
                assert client.config["MODE"] == os.environ["METAMASK_MODE"]
                assert client._journal is None and client._provider is None
                assert not Path(os.environ["METAMASK_SESSION_FILE"]).exists()
                state = Path(os.environ["METAMASK_STATE_FILE"])
                assert not state.exists(), "Startup must not create or renew authority"
                try:
                    journal = client.journal
                    if broker:
                        assert broker.data_source.client.journal is journal
                finally:
                    broker.close() if broker else client.close()
                # Shutdown must release ownership so a later strategy can resume.
                with closing(Journal(state)):
                    pass
            """),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
