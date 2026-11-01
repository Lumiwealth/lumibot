"""Native managed-wallet protocol checks without a JavaScript runtime or wallet effects."""

from unittest.mock import Mock

import pytest
import requests

from lumibot.tools.metamask.provider import Credentials, MetaMaskError, MetaMaskProvider, MetaMaskTransportError

WALLET = "0x" + "1" * 40
OTHER_WALLET = "0x" + "2" * 40


def response(payload, status=200):
    result = Mock(status_code=status)
    result.json.return_value = payload
    result.headers = {"Content-Type": "application/json"}
    return result


@pytest.fixture
def provider():
    session = Mock(spec=requests.Session)
    client = MetaMaskProvider(Credentials("test-project", "test-access-token"), session=session)
    return client


def test_browser_token_pair_is_not_mistaken_for_one_bearer_token():
    credentials = Credentials.from_config({"PROJECT_ID": "project", "CLI_TOKEN": "access:refresh"})
    assert credentials.access_token == "access"
    assert credentials.refresh_token == "refresh"
    assert "access" not in repr(credentials)
    assert "refresh" not in repr(credentials)


def test_project_is_not_inferred_from_unverified_token_claims():
    with pytest.raises(ValueError, match="PROJECT_ID"):
        Credentials.from_config({"ACCESS_TOKEN": "test-token"})


def test_wallet_identity_must_match_managed_custody_and_exact_address(provider):
    provider.session.request.return_value = response({"wallets": [{"address": OTHER_WALLET, "type": "derived"}]})
    with pytest.raises(MetaMaskError, match="WALLET_MISMATCH"):
        provider.verify_wallet(WALLET)
    provider.session.request.return_value = response({"wallets": [{"address": WALLET, "type": "imported"}]})
    with pytest.raises(MetaMaskError, match="CUSTODY_MISMATCH"):
        provider.verify_wallet(WALLET)


def test_wallet_read_uses_authenticated_remote_https_without_mutations(provider):
    provider.session.request.return_value = response(
        {"wallets": [{"address": WALLET, "type": "derived", "namespace": "evm"}]}
    )
    assert provider.verify_wallet(WALLET)["address"] == WALLET
    args, kwargs = provider.session.request.call_args
    assert args == ("GET", "https://agentic-mimir-service.api.cx.metamask.io/v1/projects/test-project/wallets")
    assert kwargs["headers"]["Authorization"] == "Bearer test-access-token"
    assert kwargs["allow_redirects"] is False
    assert provider.session.trust_env is False
    assert provider.session.request.call_count == 1


def test_redirect_is_not_followed_with_wallet_credentials(provider):
    provider.session.request.return_value = response({}, 302)
    with pytest.raises(MetaMaskError, match="REDIRECT_DENIED"):
        provider.wallets()
    assert provider.session.request.call_count == 1


def test_expired_auth_fails_explicitly_without_retry_or_disclosing_token(provider):
    provider.session.request.return_value = response({"message": "expired"}, 401)
    with pytest.raises(MetaMaskError, match="AUTHENTICATION_REQUIRED") as error:
        provider.wallets()
    assert "test-access-token" not in str(error.value)
    assert provider.session.request.call_count == 1


def test_signature_request_preserves_original_provider_id_and_full_protocol_context(provider):
    action = {"type": "cancel", "cancels": [{"a": 0, "o": 42}]}
    typed = {"domain": {"chainId": 1337}, "types": {}, "primaryType": "Agent", "message": {}}
    provider.session.request.return_value = response(
        {
            "requestId": "saved-request",
            "status": "AWAITING_MFA",
            "walletAddress": WALLET,
            "method": "eth_signTypedData_v4",
            "origin": "lumibot",
            "chainId": 421614,
        }
    )
    job = provider.submit_signature("saved-request", WALLET, "testnet", typed, action)
    assert job.request_id == "saved-request"
    assert job.state == "AWAITING_MFA"
    assert job.signature is None
    sent = provider.session.request.call_args.kwargs["json"]
    assert sent == {
        "requestId": "saved-request",
        "namespace": "evm",
        "chainId": 421614,
        "walletAddress": WALLET,
        "method": "eth_signTypedData_v4",
        "origin": "lumibot",
        "payload": {"typedData": typed, "protocol": "hyperliquid", "protocolPayload": action},
    }
    assert sent["payload"]["typedData"]["domain"]["chainId"] == 1337


def test_approved_without_managed_signature_is_not_signed(provider):
    provider.session.request.return_value = response(
        {
            "requestId": "saved-request",
            "status": "APPROVED",
            "walletAddress": WALLET,
            "method": "eth_signTypedData_v4",
            "origin": "lumibot",
            "chainId": 421614,
        }
    )
    with pytest.raises(MetaMaskError, match="MANAGED_SIGNATURE_UNAVAILABLE"):
        provider.signature("saved-request", WALLET, "testnet")


@pytest.mark.parametrize(
    "payload",
    [
        {"requestId": "different", "status": "SIGNED", "walletAddress": WALLET, "signature": "0x" + "a" * 130},
        {
            "requestId": "saved-request",
            "status": "SIGNED",
            "walletAddress": OTHER_WALLET,
            "signature": "0x" + "a" * 130,
        },
        {"requestId": "saved-request", "status": "SIGNED", "walletAddress": WALLET},
        {"requestId": "saved-request", "status": "NEW_UNKNOWN_STATUS", "walletAddress": WALLET},
    ],
)
def test_malformed_or_mismatched_signature_job_is_rejected(provider, payload):
    provider.session.request.return_value = response(payload)
    with pytest.raises(MetaMaskError):
        provider.signature("saved-request", WALLET, "testnet")


def test_transport_loss_does_not_retry_a_signing_mutation(provider):
    provider.session.request.side_effect = requests.Timeout()
    with pytest.raises(MetaMaskTransportError, match="REQUEST_OUTCOME_UNKNOWN"):
        provider.submit_signature("saved-request", WALLET, "testnet", {}, {"type": "cancel"})
    assert provider.session.request.call_count == 1


def test_missing_guard_mode_or_policy_blocks_authorization(provider):
    provider.session.request.return_value = response(
        {
            "mode": "yolo",
            "projectId": "test-project",
            "walletAddress": WALLET,
            "method": "eth_signTypedData_v4",
            "origin": "lumibot",
            "chainId": 421614,
        }
    )
    with pytest.raises(MetaMaskError, match="GUARD_MODE_REQUIRED"):
        provider.policy(WALLET)
    empty = response(None)
    empty.headers = {"Content-Type": "text/yaml"}
    empty.text = ""
    provider.session.request.side_effect = [
        response(
            {
                "mode": "safe",
                "projectId": "test-project",
                "walletAddress": WALLET,
                "method": "eth_signTypedData_v4",
                "origin": "lumibot",
                "chainId": 421614,
            }
        ),
        empty,
    ]
    with pytest.raises(MetaMaskError, match="INVALID_POLICY"):
        provider.policy(WALLET)


def test_guard_policy_uses_yaml_wire_format_and_exact_text_digest(provider):
    import hashlib

    policy = "version: 1\nrules:\n  - action: allow\n"
    wire = response(None)
    wire.headers = {"Content-Type": "text/yaml; charset=utf-8"}
    wire.text = policy
    provider.session.request.side_effect = [
        response(
            {
                "mode": "safe",
                "projectId": "test-project",
                "walletAddress": WALLET,
                "method": "eth_signTypedData_v4",
                "origin": "lumibot",
                "chainId": 421614,
            }
        ),
        wire,
    ]
    result = provider.policy(WALLET)
    assert result.text == policy
    assert result.digest == hashlib.sha256(policy.encode()).hexdigest()
    assert provider.session.request.call_args.kwargs["headers"]["Accept"] == "text/yaml"


def test_guard_policy_cannot_use_another_wallet_mode_response(provider):
    provider.session.request.return_value = response(
        {"mode": "safe", "projectId": "test-project", "walletAddress": OTHER_WALLET}
    )
    with pytest.raises(MetaMaskError, match="POLICY_IDENTITY_MISMATCH"):
        provider.policy(WALLET)
