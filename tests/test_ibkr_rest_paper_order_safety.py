import pytest

from tests.ibkr_rest_paper_order_safety import (
    _authenticated_selected_account_id,
    mask_ibkr_account_id,
    require_explicit_ibkr_rest_paper_configuration,
    require_ibkr_rest_paper_account,
)


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
