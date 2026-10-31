"""Transport for the optional MetaMask Agent SDK service. No wallet keys enter Python."""

from decimal import Decimal
from urllib.parse import urlparse

import requests


class MetaMaskAPIError(RuntimeError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(f"{code}: {message}")


class MetaMaskClient:
    def __init__(self, config):
        self.url = str(config.get("BROKER_URL", "http://127.0.0.1:8766")).rstrip("/")
        parsed = urlparse(self.url)
        if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost") or parsed.path:
            raise ValueError("MetaMask broker must use the loopback service URL (use an SSH tunnel for a remote host)")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Credentials and query strings are not allowed in the broker URL")
        self.wallet = str(config.get("WALLET_ADDRESS", "")).lower()
        self.network = config.get("NETWORK", "testnet")
        self.token = config.get("BROKER_TOKEN")
        if not self.wallet or not self.token or self.network not in ("testnet", "mainnet"):
            raise ValueError("MetaMask requires BROKER_TOKEN, WALLET_ADDRESS, and explicit testnet/mainnet NETWORK")
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        self.identity = None

    def request(self, method, path, *, payload=None, params=None):
        # No transport retries: a timeout after signing is an unknown outcome.
        response = self.session.request(
            method, self.url + path, json=payload, params=params, timeout=(5, 45), allow_redirects=False
        )
        if 300 <= response.status_code < 400:
            raise MetaMaskAPIError("REDIRECT_DENIED", "Broker redirects are not followed")
        try:
            data = response.json()
        except ValueError as exc:
            raise MetaMaskAPIError("INVALID_RESPONSE", "Broker returned invalid JSON") from exc
        if not response.ok:
            error = data.get("error", {}) if isinstance(data, dict) else {}
            raise MetaMaskAPIError(error.get("code", "BROKER_ERROR"), error.get("message", "Broker request failed"))
        return data

    def verify(self):
        identity = self.request("GET", "/v1/identity")
        if identity.get("wallet", "").lower() != self.wallet or identity.get("network") != self.network:
            raise MetaMaskAPIError("IDENTITY_MISMATCH", "Broker wallet or network differs from configuration")
        self.identity = identity
        return identity

    def call(self, method, path, **kwargs):
        self.verify()
        return self.request(method, path, **kwargs)

    def close(self):
        self.session.close()


def decimal_value(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Non-finite amount returned by MetaMask")
    return result


def token_payload(asset):
    if asset.asset_type != "crypto" or getattr(asset, "chain_id", None) is None:
        raise ValueError(
            "Onchain tokens require Asset(..., asset_type='crypto', chain_id=..., token_address=..., decimals=...)"
        )
    return {
        "symbol": asset.symbol,
        "chainId": asset.chain_id,
        "address": asset.token_address,
        "decimals": asset.decimals,
    }
