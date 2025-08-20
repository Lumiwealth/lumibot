import os
import time
import pytest
from unittest.mock import MagicMock

from lumibot.entities import Asset, Order
from lumibot.brokers.projectx import ProjectX
from lumibot.data_sources.projectx_data import ProjectXData

pytestmark = [pytest.mark.projectx]

FIRM_ENV = "PROJECTX_FIRM"
REQUIRED_VARS = [
    "PROJECTX_TOPONE_API_KEY",
    "PROJECTX_TOPONE_USERNAME",
    "PROJECTX_TOPONE_PREFERRED_ACCOUNT_NAME",
]

skip_reason = "Missing ProjectX TOPONE credential env vars; set them in .env to enable live-ish integration test"
needs_creds = any(os.environ.get(v) is None for v in REQUIRED_VARS)

@pytest.mark.skipif(needs_creds, reason=skip_reason)
def test_projectx_order_lifecycle_smoke(caplog):
    """Smoke test: place a tiny limit order then cancel.
    This does not assert fill (requires market conditions) but validates:
      - order submission returns ID
      - broker tracking moves to submitted
      - polling dispatch doesn't error
    """
    firm = os.environ.get(FIRM_ENV, "TOPONE")
    # Fetch full config using existing helper to ensure URLs present
    from lumibot.credentials import get_projectx_config
    full_cfg = get_projectx_config(firm)
    if not full_cfg or not full_cfg.get("base_url"):
        pytest.skip("ProjectX config incomplete (base_url missing)")
    config = full_cfg
    data = ProjectXData(config)
    broker = ProjectX(config, data_source=data, connect_stream=False)

    # Fake connect quickly if account selection requires API; skip if failure
    connected = broker.connect()
    assert connected is True, "Failed to connect to ProjectX test account"

    # Build order (micro MES contract) continuous future symbol
    asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
    order = Order(
        asset=asset,
        quantity=1,
        order_type="limit",
        side="buy",
        limit_price=1,  # Intentionally unrealistic to avoid fill, just for submission path
        strategy="TestStrat"
    )
    submitted = broker._submit_order(order)
    assert submitted.id is not None, f"Order ID missing; error={submitted.error}"
    assert submitted.status in ("submitted", "new", "open"), f"Unexpected status {submitted.status}"

    # Cancel to exercise cancel path
    broker.cancel_order(submitted)
    assert submitted.status in ("cancelled", "canceled"), "Order not cancelled"

    # Check logs contain lifecycle messages
    lifecycle_hits = [r for r in caplog.records if "Order" in r.message and "ProjectX" in r.message]
    assert lifecycle_hits, "Expected lifecycle log lines not found"
