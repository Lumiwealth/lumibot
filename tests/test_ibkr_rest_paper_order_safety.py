import pytest

from lumibot.entities import Asset
from tests import test_ibkr_rest_gtd_paper_apitest as gtd_paper_apitest
from tests.ibkr_rest_paper_order_safety import (
    _authenticated_selected_account_id,
    _construct_paper_test_data_source,
    ibkr_rest_paper_order_data_source,
    ibkr_rest_paper_test_config,
    mask_ibkr_account_id,
    require_explicit_ibkr_rest_paper_configuration,
    require_ibkr_rest_paper_account,
)
from tests.test_ibkr_rest_advanced_orders_paper_apitest import (
    _BrokerTrafficProbe,
    _bounded_reference_price,
    _sanitized_submission_entry_shape,
    _sanitized_warning_category,
)
from tests.test_ibkr_rest_gtd_paper_apitest import _request_json


def _local_ibeam_config():
    return {"IB_USERNAME": "test-user", "IB_PASSWORD": "test-password"}


def test_missing_ibkr_rest_configuration_skips_before_gateway_startup():
    with pytest.raises(pytest.skip.Exception):
        require_explicit_ibkr_rest_paper_configuration({}, {"IB_USE_PAPER_ACCOUNT": "true"})


def test_paper_test_config_uses_existing_rest_environment_names_without_importing_credentials():
    config = ibkr_rest_paper_test_config(
        {
            "IB_USERNAME": "test-user",
            "IB_PASSWORD": "test-password",
            "IB_ACCOUNT_ID": "DU1234567",
            "IB_API_URL": "https://gateway.example",
            "IB_USE_PAPER_ACCOUNT": "true",
            "IB_GATEWAY_PORT": "4243",
        }
    )

    assert config == {
        "IB_USERNAME": "test-user",
        "IB_PASSWORD": "test-password",
        "IB_ACCOUNT_ID": "DU1234567",
        "API_URL": "https://gateway.example",
        "RUNNING_ON_SERVER": None,
        "GATEWAY_PORT": "4243",
        "GATEWAY_INSTANCE_ID": None,
        "USE_PAPER_ACCOUNT": "true",
        "IBEAM_DOCKER_TAG": None,
        "AUTH_TIMEOUT": None,
        "AUTH_POLL_INTERVAL": None,
        "REQUEST_TIMEOUT": None,
        "VERIFY_SSL": None,
    }


def test_paper_data_source_fixture_is_session_scoped_to_reuse_one_gateway():
    assert ibkr_rest_paper_order_data_source._fixture_function_marker.scope == "session"


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
            return [{"order_id": "-1"}, {"order_id": "1001"}]

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

    assert data_source.ledger_seen_before_outer_return == ["1001"]
    assert record.acknowledged_ids == ["1001"]


def test_submission_response_diagnostic_masks_all_broker_values():
    diagnostic = _sanitized_submission_entry_shape(
        {
            "order_id": "-1",
            "local_order_id": "local-secret",
            "parent_order_id": "1234567890",
            "error": "private broker response",
            "message": "private confirmation",
            "warning_message": "private warning",
        }
    )

    assert diagnostic == (
        "id=placeholder_or_nonpositive, local_order_id=present, "
        "parent_order_id=present, error=present, message=present, warning=non_numeric_warning"
    )
    assert "local-secret" not in diagnostic
    assert "1234567890" not in diagnostic
    assert "private" not in diagnostic


def test_submission_warning_diagnostic_exposes_only_exact_short_numeric_codes():
    assert _sanitized_warning_category("399") == "code=399"
    assert _sanitized_warning_category(399) == "code=399"
    assert _sanitized_warning_category("warning 399") == "non_numeric_warning"
    assert _sanitized_warning_category("DU1234567") == "non_numeric_warning"
    assert _sanitized_warning_category(None) == "absent"


def test_advanced_paper_reference_price_uses_bounded_configured_snapshot():
    class FakeResponse:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    class FakeHttpClient:
        def get(self, url, **_kwargs):
            if url.endswith("/iserver/secdef/search?symbol=SPY"):
                return FakeResponse([{"conid": 265598}])
            assert url.endswith("/iserver/marketdata/snapshot?conids=265598&fields=31")
            return FakeResponse([{"31": "C 100.25"}])

    class FakeDataSource:
        base_url = "https://gateway.example/v1/api"
        verify_ssl = True
        request_timeout = 1
        http_client = FakeHttpClient()

    data_source = FakeDataSource()

    price = _bounded_reference_price(data_source, Asset("SPY"))

    assert price == 100.25


def test_gtd_paper_reference_price_allows_a_bounded_snapshot_warmup(monkeypatch):
    class FakeResponse:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    class FakeHttpClient:
        def __init__(self):
            self.snapshot_calls = 0

        def get(self, _url, **_kwargs):
            self.snapshot_calls += 1
            if self.snapshot_calls < gtd_paper_apitest._MARKET_DATA_ATTEMPTS:
                return FakeResponse([])
            return FakeResponse([{"31": "100.25"}])

    class FakeDataSource:
        base_url = "https://gateway.example/v1/api"
        verify_ssl = True
        request_timeout = 1
        http_client = FakeHttpClient()

    sleep_calls = []
    monkeypatch.setattr(gtd_paper_apitest.time, "sleep", sleep_calls.append)

    price = gtd_paper_apitest._reference_price(FakeDataSource(), 265598)

    assert price == 100.25
    assert FakeDataSource.http_client.snapshot_calls == gtd_paper_apitest._MARKET_DATA_ATTEMPTS
    assert sleep_calls == [gtd_paper_apitest._MARKET_DATA_RETRY_SECONDS] * (
        gtd_paper_apitest._MARKET_DATA_ATTEMPTS - 1
    )


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
