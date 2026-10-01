"""Alpaca API tests must not depend on a different vendor's credentials."""

from types import SimpleNamespace

import pytest

from tests.conftest import pytest_runtest_setup


def test_alpaca_api_smoke_needs_only_its_own_credentials(monkeypatch):
    for key in ("THETADATA_USERNAME", "THETADATA_PASSWORD", "POLYGON_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("ALPACA_TEST_API_KEY", "fixture-key")
    monkeypatch.setenv("ALPACA_TEST_API_SECRET", "fixture-secret")
    item = SimpleNamespace(get_closest_marker=lambda name: object() if name in {"apitest", "alpaca"} else None)
    try:
        pytest_runtest_setup(item)
    except pytest.skip.Exception as error:
        pytest.fail(str(error))
