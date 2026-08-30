import pytest

from lumibot.entities import Asset
from tests.ibkr_rest_paper_order_safety import (
    _authenticated_selected_account_id,
    _construct_paper_test_data_source,
    mask_ibkr_account_id,
    require_explicit_ibkr_rest_paper_configuration,
    require_ibkr_rest_paper_account,
)
from tests.test_ibkr_rest_advanced_orders_paper_apitest import (
    _BrokerTrafficProbe,
    _bounded_reference_price,
)
from tests.test_ibkr_rest_gtd_paper_apitest import _request_json


def _local_ibeam_config():
    return {"IB_USERNAME": "test-user", "IB_PASSWORD": "test-password"}


def test_missing_ibkr_rest_configuration_skips_before_gateway_startup():
    with pytest.raises(pytest.skip.Exception):
        require_explicit_ibkr_rest_paper_configuration({}, {"IB_USE_PAPER_ACCOUNT": "true"})


def test_absent_explicit_paper_flag_skips_even_when_config_default_would_be_paper():
    config = {"API_URL": "https://gateway.example", "USE_PAPER_ACCOUNT": "true"}

    with pytest.raises(pytest.skip.Exception):
        require_explicit_ibkr_rest_paper_configuration(config, {})


def test_false_explicit_paper_flag_fails_before_account_or_order_handling():
    with pytest.raises(pytest.fail.Exception, match="explicitly true"):
        require_explicit_ibkr_rest_paper_configuration(
            _local_ibeam_config(), {"IB_USE_PAPER_ACCOUNT": "false"}
        )


def test_unavailable_gateway_skips_without_constructing_a_data_source():
    class UnavailableHttpClient:
        def get(self, *_args, **_kwargs):
            raise OSError("unavailable")

    class FakeDataSource:
        base_url = "https://gateway.example/v1/api"
        verify_ssl = True
        request_timeout = 1
        http_client = UnavailableHttpClient()

    with pytest.raises(pytest.skip.Exception):
        _authenticated_selected_account_id(FakeDataSource())


def test_paper_data_source_startup_does_not_suppress_session_warnings(monkeypatch):
    class FakeDataSource:
        def __init__(self, config):
            self.config = config
            self.warning_suppression_calls = 0
            self.suppress_warnings()

        def suppress_warnings(self):
            self.warning_suppression_calls += 1

    data_source = _construct_paper_test_data_source(
        FakeDataSource,
        {"IB_USERNAME": "test-user"},
        monkeypatch,
    )

    assert data_source.warning_suppression_calls == 0


def test_confirmation_acknowledgement_is_ledged_before_outer_submission_returns(monkeypatch):
    class FakeDataSource:
        request_timeout = 30

        def get_from_endpoint(self, *_args, **_kwargs):
            return None

        def post_to_endpoint(self, _url, _json, description="", **_kwargs):
            if description == "Executing order":
                response = self.post_to_endpoint(
                    "https://gateway.example/iserver/reply/fake",
                    {"confirmed": True},
                    description="Confirming Order",
                )
                self.ledger_seen_before_outer_return = list(
                    self.probe.current_scenario.acknowledged_ids
                )
                return response
            return [{"order_id": "broker-order-1"}]

        def delete_to_endpoint(self, *_args, **_kwargs):
            return None

        def delete_order(self, _order):
            return None

    data_source = FakeDataSource()
    probe = _BrokerTrafficProbe(data_source, monkeypatch)
    data_source.probe = probe
    record = probe.begin("confirmation", expected_native_count=1)

    data_source.post_to_endpoint(
        "https://gateway.example/iserver/account/paper/orders",
        {"orders": [{}]},
        description="Executing order",
    )

    assert data_source.ledger_seen_before_outer_return == ["broker-order-1"]
    assert record.acknowledged_ids == ["broker-order-1"]


def test_advanced_paper_reference_price_uses_bounded_configured_snapshot():
    class FakeDataSource:
        base_url = "https://gateway.example/v1/api"

        def __init__(self):
            self.snapshot_calls = 0

        def get_conid_from_asset(self, asset, exchange=None):
            assert asset.symbol == "SPY"
            assert exchange == "SMART"
            return 265598

        def get_from_endpoint(self, url, **kwargs):
            self.snapshot_calls += 1
            assert url.endswith("/iserver/marketdata/snapshot?conids=265598&fields=31")
            assert kwargs["max_retries"] == 0
            return [{"31": "C 100.25"}]

    data_source = FakeDataSource()

    price = _bounded_reference_price(data_source, Asset("SPY"))

    assert price == 100.25
    assert data_source.snapshot_calls == 1


def test_gtd_probe_refuses_post_outside_whatif(monkeypatch):
    class UnexpectedHttpClient:
        def post(self, *_args, **_kwargs):
            pytest.fail("GTD safety guard allowed an unexpected POST")

    class FakeDataSource:
        account_id = "DU1234567"
        base_url = "https://gateway.example/v1/api"
        http_client = UnexpectedHttpClient()
        request_timeout = 1
        verify_ssl = True

    with pytest.raises(pytest.fail.Exception, match="refuses POST outside /orders/whatif"):
        _request_json(
            FakeDataSource(),
            "POST",
            "/iserver/account/DU1234567/orders",
            {"orders": [{}]},
        )


def test_live_style_account_identifier_fails():
    with pytest.raises(pytest.fail.Exception, match="non-paper account"):
        require_ibkr_rest_paper_account(
            configured_account_id=None,
            authenticated_account_id="U1234567",
        )


def test_paper_style_account_identifier_passes():
    account_id = require_ibkr_rest_paper_account(
        configured_account_id="DU1234567",
        authenticated_account_id="DU1234567",
        data_source_account_id="DU1234567",
    )

    assert account_id == "DU1234567"


def test_configured_account_mismatch_fails():
    with pytest.raises(pytest.fail.Exception, match="does not match"):
        require_ibkr_rest_paper_account(
            configured_account_id="DU1234567",
            authenticated_account_id="DU7654321",
        )


def test_masked_account_identifier_never_reveals_the_full_identifier():
    account_id = "DU1234567"
    masked = mask_ibkr_account_id(account_id)

    assert masked == "****4567"
    assert account_id not in masked
    assert mask_ibkr_account_id("DU12") == "****"
