"""Generic metadata for every exchange the installed CCXT library supports.

LumiBot's ``Ccxt`` broker and ``CcxtData`` source accept any CCXT exchange id.
This module gives every one of them the same environment-variable convention,
so a deployment can select an exchange with ``TRADING_BROKER=<exchange id>``
without a hand-written configuration block per exchange.

Credential fields come from each exchange's own ``requiredCredentials`` and
capabilities come from its ``has`` map, so newly added CCXT exchanges work as
soon as the CCXT package is upgraded. No network access happens here.

Environment convention (``<PREFIX>`` is the exchange id upper-cased, for
example ``OKX`` or ``BINANCEUSDM``):

==================  ===========================
CCXT field          Environment variable
==================  ===========================
apiKey              ``<PREFIX>_API_KEY``
secret              ``<PREFIX>_API_SECRET``
password            ``<PREFIX>_API_PASSPHRASE``
uid                 ``<PREFIX>_UID``
accountId           ``<PREFIX>_ACCOUNT_ID``
login               ``<PREFIX>_LOGIN``
twofa               ``<PREFIX>_TWOFA``
privateKey          ``<PREFIX>_PRIVATE_KEY``
walletAddress       ``<PREFIX>_WALLET_ADDRESS``
token               ``<PREFIX>_TOKEN``
==================  ===========================

``<PREFIX>_SANDBOX=true`` selects the exchange's testnet when CCXT publishes
one, and ``<PREFIX>_MARGIN=true`` enables margin mode in the broker.
"""

from __future__ import annotations

import json
import os
from typing import Any, Mapping, Optional

# CCXT credential attribute -> environment suffix. Order is the display order.
CREDENTIAL_ENV_SUFFIXES: dict[str, str] = {
    "apiKey": "API_KEY",
    "secret": "API_SECRET",
    "password": "API_PASSPHRASE",
    "uid": "UID",
    "accountId": "ACCOUNT_ID",
    "login": "LOGIN",
    "twofa": "TWOFA",
    "privateKey": "PRIVATE_KEY",
    "walletAddress": "WALLET_ADDRESS",
    "token": "TOKEN",
}

CREDENTIAL_LABELS: dict[str, str] = {
    "apiKey": "API Key",
    "secret": "API Secret",
    "password": "API Passphrase",
    "uid": "User ID",
    "accountId": "Account ID",
    "login": "Login",
    "twofa": "Two-Factor Secret",
    "privateKey": "Private Key",
    "walletAddress": "Wallet Address",
    "token": "Token",
}

# Fields that are not secret material and may be shown back to the user.
NON_SECRET_FIELDS = {"apiKey", "uid", "accountId", "login", "walletAddress"}

# LumiBot ships native (non-CCXT) brokers under these names. A CCXT exchange
# with the same id is reachable only through ``ccxt:<id>`` so the native broker
# keeps its name.
NATIVE_BROKER_NAMES = {
    "alpaca",
    "tradier",
    "schwab",
    "tradovate",
    "bitunix",
    "polymarket",
    "polymarket_clob",
    "projectx",
    "ib",
    "interactivebrokers",
    "ibrest",
    "interactivebrokersrest",
}

# Historical aliases that predate the generic convention.
LEGACY_ENV_ALIASES: dict[str, dict[str, tuple[str, ...]]] = {
    "coinbase": {
        "apiKey": ("COINBASE_API_KEY_NAME",),
        "secret": ("COINBASE_PRIVATE_KEY",),
    },
}


def _ccxt():
    import ccxt  # imported lazily so importing LumiBot stays fast

    return ccxt


def env_prefix(exchange_id: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in exchange_id).upper()


def credential_env_key(exchange_id: str, field: str) -> str:
    return f"{env_prefix(exchange_id)}_{CREDENTIAL_ENV_SUFFIXES[field]}"


def resolve_ccxt_exchange_id(name: Optional[str]) -> Optional[str]:
    """Return the CCXT exchange id for ``name`` (case-insensitive), else None.

    ``ccxt:<id>`` and ``ccxt_<id>`` are accepted so an exchange whose id
    collides with a native LumiBot broker (for example ``alpaca``) stays
    reachable.
    """
    if not name:
        return None
    token = str(name).strip().lower()
    for prefix in ("ccxt:", "ccxt_", "ccxt-"):
        if token.startswith(prefix):
            token = token[len(prefix):]
            break
    try:
        exchanges = _ccxt().exchanges
    except Exception:
        return None
    return token if token in exchanges else None


def is_generic_ccxt_broker_name(name: Optional[str]) -> bool:
    """True when ``name`` should route to the generic CCXT path."""
    if not name:
        return False
    token = str(name).strip().lower()
    if token in NATIVE_BROKER_NAMES:
        return False
    return resolve_ccxt_exchange_id(token) is not None


def _exchange_instance(exchange_id: str):
    return getattr(_ccxt(), exchange_id)()


def required_credential_fields(exchange_id: str) -> list[str]:
    required = _exchange_instance(exchange_id).requiredCredentials or {}
    return [field for field in CREDENTIAL_ENV_SUFFIXES if required.get(field)]


def _env_value(environ: Mapping[str, str], keys: tuple[str, ...]) -> Optional[str]:
    for key in keys:
        value = environ.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _truthy(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "y", "on")


def build_ccxt_config_from_env(exchange_id: str, environ: Optional[Mapping[str, str]] = None) -> dict[str, Any]:
    """Build a ``Ccxt``/``CcxtData`` config for any exchange from environment variables.

    Every recognised credential present in the environment is passed through,
    not only the required ones, because some exchanges accept optional fields.
    """
    environ = os.environ if environ is None else environ
    prefix = env_prefix(exchange_id)
    aliases = LEGACY_ENV_ALIASES.get(exchange_id, {})
    config: dict[str, Any] = {"exchange_id": exchange_id}
    for field, suffix in CREDENTIAL_ENV_SUFFIXES.items():
        value = _env_value(environ, (f"{prefix}_{suffix}", *aliases.get(field, ())))
        if value is not None:
            config[field] = value
    config["sandbox"] = _truthy(environ.get(f"{prefix}_SANDBOX"))
    config["margin"] = _truthy(environ.get(f"{prefix}_MARGIN"))
    return config


def missing_required_credentials(exchange_id: str, config: Mapping[str, Any]) -> list[str]:
    return [field for field in required_credential_fields(exchange_id) if not config.get(field)]


def validate_ccxt_config(exchange_id: str, config: Mapping[str, Any]) -> None:
    """Raise a clear error naming the environment variables an exchange still needs."""
    missing = missing_required_credentials(exchange_id, config)
    if missing:
        keys = ", ".join(credential_env_key(exchange_id, field) for field in missing)
        raise ValueError(
            f"CCXT exchange '{exchange_id}' needs these credentials to trade: {keys}. "
            "Set them as environment variables (or secrets) and restart."
        )
    if config.get("sandbox") and not exchange_capabilities(exchange_id)["sandbox"]:
        raise ValueError(
            f"CCXT exchange '{exchange_id}' has no testnet/sandbox in CCXT. "
            f"Unset {env_prefix(exchange_id)}_SANDBOX to use the live exchange."
        )


def _has(value: Any) -> bool:
    return bool(value) and value is not None


def exchange_capabilities(exchange_id: str) -> dict[str, Any]:
    exchange = _exchange_instance(exchange_id)
    has = exchange.has or {}
    urls = exchange.urls or {}
    return {
        "spot": _has(has.get("spot")),
        "swap": _has(has.get("swap")),
        "future": _has(has.get("future")),
        "option": _has(has.get("option")),
        "createOrder": _has(has.get("createOrder")),
        "cancelOrder": _has(has.get("cancelOrder")),
        "fetchBalance": _has(has.get("fetchBalance")),
        "fetchOHLCV": _has(has.get("fetchOHLCV")),
        "fetchTicker": _has(has.get("fetchTicker")),
        "fetchPositions": _has(has.get("fetchPositions")),
        "watchOrders": _has(has.get("watchOrders")),
        "sandbox": bool(urls.get("test")),
        "dex": bool(getattr(exchange, "dex", False) or has.get("dex")),
    }


def support_level(capabilities: Mapping[str, Any]) -> str:
    """Plain summary: 'trading' (orders + history), 'trading_no_history', or 'data_only'."""
    if capabilities["createOrder"] and capabilities["fetchBalance"]:
        return "trading" if capabilities["fetchOHLCV"] else "trading_no_history"
    return "data_only"


def exchange_entry(exchange_id: str) -> dict[str, Any]:
    exchange = _exchange_instance(exchange_id)
    required = set(required_credential_fields(exchange_id))
    capabilities = exchange_capabilities(exchange_id)
    return {
        "id": exchange_id,
        "name": str(exchange.name or exchange_id),
        "envPrefix": env_prefix(exchange_id),
        "credentialFields": [
            {
                "ccxtField": field,
                "key": credential_env_key(exchange_id, field),
                "label": CREDENTIAL_LABELS[field],
                "secret": field not in NON_SECRET_FIELDS,
                "multiline": field == "privateKey",
            }
            for field in CREDENTIAL_ENV_SUFFIXES
            if field in required
        ],
        "sandboxKey": f"{env_prefix(exchange_id)}_SANDBOX",
        "capabilities": capabilities,
        "supportLevel": support_level(capabilities),
        "nativeNameCollision": exchange_id in NATIVE_BROKER_NAMES,
        "docsUrl": _docs_url(exchange),
    }


def _docs_url(exchange) -> Optional[str]:
    doc = (exchange.urls or {}).get("doc")
    if isinstance(doc, (list, tuple)):
        doc = doc[0] if doc else None
    return doc if isinstance(doc, str) else None


def build_catalog() -> dict[str, Any]:
    ccxt = _ccxt()
    return {
        "source": "ccxt",
        "ccxtVersion": ccxt.__version__,
        "exchanges": [exchange_entry(exchange_id) for exchange_id in ccxt.exchanges],
    }


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Print the CCXT exchange catalog as JSON.")
    parser.add_argument("--output", help="Write to this file instead of stdout.")
    args = parser.parse_args(argv)
    text = json.dumps(build_catalog(), indent=2, sort_keys=False) + "\n"
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(text)
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
