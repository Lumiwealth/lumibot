import os
import subprocess
import sys

import pytest

from lumibot.tools.webull_helpers import paper_mode


@pytest.mark.parametrize("value,expected", [(None, True), ("YES", True), ("off", False), (False, False)])
def test_paper_mode_has_an_explicit_boolean_parser(value, expected):
    assert paper_mode(value) is expected


def test_invalid_paper_mode_cannot_fall_back_to_live():
    with pytest.raises(ValueError, match="WEBULL_PAPER"):
        paper_mode("maybe")


@pytest.mark.parametrize("deferred", ["true", "false"])
def test_webull_scheduled_native_selection_is_lazy(deferred):
    result = subprocess.run([
        sys.executable, "-c", """
import sys
from lumibot.credentials import get_default_broker_and_data_source
from lumibot.brokers import Webull
from lumibot.data_sources import WebullData
broker, data = get_default_broker_and_data_source()
assert isinstance(broker, Webull)
assert isinstance(data, WebullData)
assert broker.data_source is data
assert broker.is_paper
assert broker.account_id == "offline-synthetic"
assert not getattr(broker, "stream", None)
assert "webull.core.client" not in sys.modules
"""
    ], env={
        "PATH": os.environ["PATH"], "IS_BACKTESTING": "false", "LUMIBOT_DISABLE_DOTENV": "1",
        "TRADING_BROKER": "webull", "DATA_SOURCE": "webull", "WEBULL_ACCOUNT_ID": "offline-synthetic",
        "LUMIBOT_SCHEDULED_EXECUTION": "true", "LUMIBOT_LAZY_CREDENTIALS": deferred,
    }, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_webull_initializes_sdk_with_private_per_app_and_environment_token_directory(monkeypatch, tmp_path):
    import stat
    import sys
    from pathlib import Path
    from types import SimpleNamespace
    from unittest.mock import Mock

    from lumibot.tools.webull_helpers import make_client

    api = Mock()
    token_paths = []
    api.set_token_dir.side_effect = lambda value: token_paths.append(Path(value))
    monkeypatch.setitem(sys.modules, "webull.core.client", SimpleNamespace(ApiClient=lambda *a, **kw: api))
    monkeypatch.setitem(sys.modules, "webull.trade.trade_client", SimpleNamespace(TradeClient=lambda client: client))
    monkeypatch.setitem(sys.modules, "webull.data.data_client", SimpleNamespace(DataClient=lambda client: client))
    monkeypatch.setenv("HOME", str(tmp_path))
    config = {"APP_KEY": "dummy-paper-sentinel", "APP_SECRET": "never-write-this", "PAPER": True}
    make_client(config, trading=True)
    make_client(config, trading=False)
    make_client({**config, "PAPER": False}, trading=True)
    make_client({**config, "APP_KEY": "dummy-other"}, trading=True)
    assert len(token_paths) == 4
    assert token_paths[0] == token_paths[1]
    assert len(set(token_paths)) == 3
    assert all(path.is_relative_to(tmp_path) and stat.S_IMODE(path.stat().st_mode) == 0o700
               and "sentinel" not in str(path) for path in token_paths)

    writable = tmp_path / "shared"
    writable.mkdir()
    writable.chmod(0o777)
    with pytest.raises(ValueError, match="private"):
        make_client({**config, "TOKEN_DIR": writable}, trading=True)


def test_webull_token_path_rejects_replaceable_parent(tmp_path):
    from lumibot.tools.webull_helpers import _token_directory

    shared = tmp_path / "shared"
    shared.mkdir()
    shared.chmod(0o777)
    token_path = shared / "owned"
    token_path.mkdir(mode=0o700)
    with pytest.raises(ValueError, match="parent"):
        _token_directory(token_path, True, "dummy")
