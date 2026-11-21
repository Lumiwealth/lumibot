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
