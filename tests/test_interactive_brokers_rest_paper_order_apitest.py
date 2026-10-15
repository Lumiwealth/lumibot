"""Read-only safety scaffold for future IBKR REST paper-order API tests."""

import pytest

from tests.ibkr_rest_paper_order_safety import ibkr_rest_paper_order_data_source


pytestmark = [pytest.mark.apitest, pytest.mark.ibkr]


def test_authenticated_paper_order_gate(ibkr_rest_paper_order_data_source):
    """Verify the gate before a future test constructs or submits an order."""
    assert ibkr_rest_paper_order_data_source.account_id.upper().startswith("DU")
