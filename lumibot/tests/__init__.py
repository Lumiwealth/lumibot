import pytest

from lumibot.traders import Trader


@pytest.fixture(scope="function")
def trader():
    return Trader()
