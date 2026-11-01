"""Managed-wallet HTTP protocol, independent of JavaScript SDK execution.

Wire contracts follow MetaMask Fox SDK 2.10.0 remote-signing declarations,
distributed under MIT OR Apache-2.0. No Agent SDK implementation is included.
"""

from __future__ import annotations

import hashlib
import math
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from urllib.parse import quote

import requests

SERVER_WALLET_URL = "https://agentic-mimir-service.api.cx.metamask.io"
SIGNING_CONTEXT_CHAIN = {"testnet": 421614, "mainnet": 42161}
_ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")
_IDENTIFIER = re.compile(r"[a-zA-Z0-9_-]{1,128}\Z")
_SIGNATURE = re.compile(r"0x[0-9a-fA-F]{130}\Z")
_SIGNING_STATES = frozenset(
    {
        "EVALUATING",
        "AWAITING_MFA",
        "SIGNING",
        "SIGNED",
        "DENIED",
        "EXPIRED",
        "FAILED",
    }
)


class MetaMaskError(RuntimeError):
    """An explicit provider, authority or recovery error without credential data."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(f"{code}: {message}")


class MetaMaskTransportError(MetaMaskError):
    def __init__(self, mutation: bool):
        self.may_have_reached_provider = mutation
        super().__init__(
            "REQUEST_OUTCOME_UNKNOWN" if mutation else "PROVIDER_UNAVAILABLE",
            "Wallet request lost its response; reconcile the original request ID"
            if mutation
            else "Wallet API is unavailable",
        )


def address(value: object) -> str:
    if not isinstance(value, str) or not _ADDRESS.fullmatch(value):
        raise ValueError("A complete EVM wallet address is required")
    return value.lower()


def identifier(value: object, name: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must contain 1-128 letters, digits, underscores or hyphens")
    return value


def json_object(value: object, name: str = "provider response") -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise MetaMaskError("INVALID_RESPONSE", f"Expected an object for {name}")
    return dict(value)


@dataclass(frozen=True)
class Credentials:
    project_id: str
    access_token: str = field(repr=False)
    refresh_token: str | None = field(default=None, repr=False)
    expires_at: float | None = None

    def __post_init__(self):
        identifier(self.project_id, "PROJECT_ID")
        if not self.access_token or any(character.isspace() for character in self.access_token):
            raise ValueError("ACCESS_TOKEN must be a nonempty provider token")
        if self.expires_at is not None and not math.isfinite(self.expires_at):
            raise ValueError("Credential expiry must be finite Unix seconds")

    @classmethod
    def from_config(cls, config: Mapping[str, object]) -> Credentials:
        project = identifier(config.get("PROJECT_ID"), "PROJECT_ID")
        direct = config.get("ACCESS_TOKEN")
        pair = config.get("CLI_TOKEN")
        if direct and pair:
            raise ValueError("Provide ACCESS_TOKEN or CLI_TOKEN, not both")
        token = direct or pair
        if not isinstance(token, str) or not token.strip():
            raise ValueError("ACCESS_TOKEN or browser-issued CLI_TOKEN is required")
        refresh = config.get("REFRESH_TOKEN")
        if pair:
            pieces = token.strip().split(":")
            if len(pieces) > 4 or not pieces[0] or (len(pieces) > 1 and not pieces[1]):
                raise ValueError("CLI_TOKEN must be an access token or access:refresh token pair")
            token = pieces[0]
            if len(pieces) >= 2:
                refresh = pieces[1]
        if refresh is not None and not isinstance(refresh, str):
            raise ValueError("REFRESH_TOKEN must be text")
        expiry = config.get("EXPIRES_AT")
        if expiry is not None and (isinstance(expiry, bool) or not isinstance(expiry, (int, float))):
            raise ValueError("EXPIRES_AT must be Unix seconds")
        return cls(project, token.strip(), refresh, float(expiry) if expiry is not None else None)


@dataclass(frozen=True)
class SignatureJob:
    request_id: str
    wallet: str
    state: str
    signature: str | None = field(default=None, repr=False)
    approval: dict[str, object] | None = None
    failure_code: str | None = None

    @property
    def terminal(self) -> bool:
        return self.state in {"SIGNED", "DENIED", "EXPIRED", "FAILED"}


@dataclass(frozen=True)
class GuardPolicy:
    text: str
    digest: str


class MetaMaskProvider:
    """Authenticated managed-wallet operations. Mutations are never retried."""

    def __init__(
        self,
        credentials: Credentials | Callable[[], Credentials],
        *,
        session: requests.Session | None = None,
    ):
        self._credentials = credentials
        self._project = self.credentials().project_id
        self.session = session if session is not None else requests.Session()
        self.session.trust_env = False
        self._owns_session = session is None

    def credentials(self) -> Credentials:
        value = self._credentials() if callable(self._credentials) else self._credentials
        if not isinstance(value, Credentials):
            raise ValueError("credentials_provider must return Credentials")
        if hasattr(self, "_project") and value.project_id != self._project:
            raise MetaMaskError("PROJECT_MISMATCH", "Credential refresh cannot change the configured project")
        if value.expires_at is not None and value.expires_at <= time.time():
            raise MetaMaskError(
                "AUTHENTICATION_REQUIRED", "MetaMask credentials have expired; reconnect or refresh them"
            )
        return value

    def _response(
        self, method: str, suffix: str, *, payload=None, params=None, accept="application/json"
    ) -> requests.Response:
        credentials = self.credentials()
        url = f"{SERVER_WALLET_URL}/v1/projects/{quote(self._project, safe='')}/{suffix}"
        try:
            response = self.session.request(
                method,
                url,
                json=payload,
                params=params,
                headers={"Authorization": f"Bearer {credentials.access_token}", "Accept": accept},
                timeout=(5, 45),
                allow_redirects=False,
            )
        except requests.RequestException:
            raise MetaMaskTransportError(method != "GET") from None
        if 300 <= response.status_code < 400:
            raise MetaMaskError("REDIRECT_DENIED", "Wallet API redirects are not followed")
        if response.status_code in (401, 403):
            raise MetaMaskError("AUTHENTICATION_REQUIRED", "MetaMask session is expired or lacks required permission")
        if response.status_code == 404:
            raise MetaMaskError("NOT_FOUND", "Requested wallet operation was not found")
        if not 200 <= response.status_code < 300:
            raise MetaMaskError("PROVIDER_ERROR", f"MetaMask API returned HTTP {response.status_code}")
        return response

    def _request(self, method: str, suffix: str, *, payload=None, params=None) -> dict[str, object]:
        response = self._response(method, suffix, payload=payload, params=params)
        try:
            return json_object(response.json())
        except ValueError as error:
            raise MetaMaskError("INVALID_RESPONSE", "MetaMask API returned invalid JSON") from error

    def wallets(self) -> list[dict[str, object]]:
        result = self._request("GET", "wallets").get("wallets")
        if not isinstance(result, list):
            raise MetaMaskError("INVALID_RESPONSE", "Wallet inventory is missing")
        return [json_object(wallet, "wallet") for wallet in result]

    def verify_wallet(self, wallet: str) -> dict[str, object]:
        expected = address(wallet)
        matches = [item for item in self.wallets() if str(item.get("address", "")).lower() == expected]
        if len(matches) != 1:
            raise MetaMaskError("WALLET_MISMATCH", "Configured managed wallet is absent or ambiguous in this project")
        item = matches[0]
        if item.get("type") != "derived":
            raise MetaMaskError("CUSTODY_MISMATCH", "MetaMask broker requires the configured managed server wallet")
        if item.get("namespace", "evm") != "evm":
            raise MetaMaskError("WALLET_MISMATCH", "Configured wallet is not EVM")
        return item

    def policy(self, wallet: str) -> GuardPolicy:
        segment = f"wallets/{address(wallet)}"
        mode = self._request("GET", f"{segment}/mode")
        if mode.get("projectId") != self._project or str(mode.get("walletAddress", "")).lower() != address(wallet):
            raise MetaMaskError("POLICY_IDENTITY_MISMATCH", "Guard mode response belongs to another project or wallet")
        if mode.get("mode") != "safe":
            raise MetaMaskError("GUARD_MODE_REQUIRED", "Managed wallet must retain provider-enforced Guard Mode")
        policy = self._response("GET", f"{segment}/policy", params={"schema_version": 1}, accept="text/yaml")
        text = policy.text
        content_type = policy.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if (
            content_type not in {"text/yaml", "application/yaml", "application/x-yaml"}
            or not text.strip()
            or len(text) > 1_000_000
        ):
            raise MetaMaskError("INVALID_POLICY", "Provider returned no verifiable Guard policy")
        import yaml

        try:
            document = yaml.safe_load(text)
        except yaml.YAMLError as error:
            raise MetaMaskError("INVALID_POLICY", "Provider returned malformed Guard policy YAML") from error
        if not isinstance(document, dict) or not document:
            raise MetaMaskError("INVALID_POLICY", "Provider Guard policy must be a nonempty YAML object")
        return GuardPolicy(text, hashlib.sha256(text.encode("utf-8")).hexdigest())

    @staticmethod
    def _signature_job(
        payload: dict[str, object],
        request_id: str,
        wallet: str,
        network: str,
        origin: str = "lumibot",
    ) -> SignatureJob:
        if payload.get("requestId") != request_id or str(payload.get("walletAddress", "")).lower() != address(wallet):
            raise MetaMaskError("JOB_MISMATCH", "Signature job identity differs from the persisted request")
        if (
            payload.get("method") != "eth_signTypedData_v4"
            or payload.get("origin") != origin
            or payload.get("chainId") != SIGNING_CONTEXT_CHAIN[network]
        ):
            raise MetaMaskError(
                "JOB_CONTEXT_MISMATCH", "Signature job differs from the persisted method, origin or network"
            )
        state = payload.get("status")
        if state == "APPROVED":
            raise MetaMaskError(
                "MANAGED_SIGNATURE_UNAVAILABLE", "Provider approved evaluation without a managed-wallet signature"
            )
        if not isinstance(state, str) or state not in _SIGNING_STATES:
            raise MetaMaskError("INVALID_SIGNING_STATE", "Unexpected managed signature state")
        signature = payload.get("signature")
        if state == "SIGNED":
            if not isinstance(signature, str) or not _SIGNATURE.fullmatch(signature):
                raise MetaMaskError("INVALID_SIGNATURE", "Signed job did not contain a complete EVM signature")
        elif signature is not None:
            raise MetaMaskError("INVALID_SIGNING_STATE", "Nonterminal signing job unexpectedly returned a signature")
        approval = payload.get("approval")
        failure = payload.get("failureCode")
        if failure is not None and not isinstance(failure, str):
            raise MetaMaskError("INVALID_RESPONSE", "Signing failure code is malformed")
        return SignatureJob(
            request_id,
            address(wallet),
            state,
            signature,
            json_object(approval, "approval") if approval is not None else None,
            failure,
        )

    def submit_signature(
        self,
        request_id: str,
        wallet: str,
        network: str,
        typed_data: dict[str, object],
        action: dict[str, object],
    ) -> SignatureJob:
        identifier(request_id, "request_id")
        if network not in SIGNING_CONTEXT_CHAIN:
            raise ValueError("NETWORK must be explicitly testnet or mainnet")
        body = {
            "requestId": request_id,
            "namespace": "evm",
            "chainId": SIGNING_CONTEXT_CHAIN[network],
            "walletAddress": address(wallet),
            "method": "eth_signTypedData_v4",
            "origin": "lumibot",
            "payload": {"typedData": typed_data, "protocol": "hyperliquid", "protocolPayload": action},
        }
        return self._signature_job(
            self._request("POST", "signature-requests", payload=body), request_id, wallet, network
        )

    def signature(self, request_id: str, wallet: str, network: str, *, origin: str = "lumibot") -> SignatureJob:
        identifier(request_id, "request_id")
        if network not in SIGNING_CONTEXT_CHAIN:
            raise ValueError("NETWORK must be explicitly testnet or mainnet")
        if origin not in {"lumibot", "metamask"}:
            raise ValueError("Signature origin must have verified native or legacy provenance")
        return self._signature_job(
            self._request("GET", f"signature-requests/{request_id}"), request_id, wallet, network, origin
        )

    def approval(self, request_id: str) -> dict[str, object]:
        identifier(request_id, "request_id")
        return self._request("GET", f"approval/{request_id}", params={"id_type": "requestId"})

    @staticmethod
    def _transaction_job(value, request_id, expected):
        job = json_object(value, "transaction job")
        if job.get("requestId") != request_id:
            raise MetaMaskError("JOB_MISMATCH", "Transaction response belongs to another request")
        tx = json_object(job.get("tx"), "transaction identity")
        for key in (
            "from",
            "to",
            "chainId",
            "nonce",
            "data",
            "value",
            "gasLimit",
            "maxFeePerGas",
            "maxPriorityFeePerGas",
        ):
            if key not in expected:
                continue
            actual, wanted = tx.get(key), expected[key]
            if key in {"from", "to", "data"}:
                matches = isinstance(actual, str) and actual.lower() == str(wanted).lower()
            elif key in {"value", "gasLimit", "maxFeePerGas", "maxPriorityFeePerGas"}:
                try:
                    matches = int(str(actual), 16) == int(str(wanted), 16)
                except ValueError:
                    matches = False
            else:
                matches = type(actual) is type(wanted) and actual == wanted
            if not matches:
                raise MetaMaskError("JOB_CONTEXT_MISMATCH", "Transaction job differs from its persisted intent")
        states = {
            "EVALUATING",
            "AWAITING_MFA",
            "SIGNING",
            "BROADCASTING",
            "BROADCASTED",
            "CONFIRMED",
            "DENIED",
            "EXPIRED",
            "FAILED",
            "BROADCAST_FAILED",
            "BROADCAST_TRACKING_EXPIRED",
            "CONFIRMATION_TRACKING_EXPIRED",
        }
        if not isinstance(job.get("status"), str) or job.get("status") not in states:
            raise MetaMaskError("INVALID_TRANSACTION_STATE", "Unexpected managed transaction state")
        tx_hash = job.get("txHash")
        if tx_hash is not None and (not isinstance(tx_hash, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", tx_hash)):
            raise MetaMaskError("INVALID_RESPONSE", "Provider transaction hash is invalid")
        # Store only necessary public evidence, never raw signed transactions.
        return {
            key: job[key]
            for key in ("requestId", "status", "tx", "txHash", "txBlock", "approval", "failureCode")
            if key in job
        }

    def submit_transaction(self, request_id, tx, *, quote_id=None, intent=None):
        identifier(request_id, "request ID")
        body = {"requestId": request_id, "namespace": "evm", "chainId": tx["chainId"], "tx": tx}
        if quote_id is not None:
            body["bridgeQuoteId"] = quote_id
        if intent is not None:
            body["txIntent"] = intent
        return self._transaction_job(self._request("POST", "transaction-requests", payload=body), request_id, tx)

    def transaction(self, request_id, tx):
        identifier(request_id, "request ID")
        return self._transaction_job(self._request("GET", f"transaction-requests/{request_id}"), request_id, tx)

    def close(self):
        if self._owns_session:
            self.session.close()
