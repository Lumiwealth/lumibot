import pytest
from traders import Trader


@pytest.fixture(scope="function")
def trader():
    return Trader()
