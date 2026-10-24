"""Wallet safety: DEX exchanges can trade with a trading-only key instead of the
main wallet key wherever the exchange (and CCXT) supports it."""

import ccxt
import pytest

from lumibot.tools.ccxt_exchange_catalog import (
    AGENT_KEY_CREDENTIALS,
    build_ccxt_config_from_env,
    exchange_entry,
    missing_required_credentials,
    validate_ccxt_config,
)


def _client(exchange_id, config):
    config = dict(config)
    config.pop("exchange_id")
    config.pop("sandbox", None)
    config.pop("margin", None)
    return getattr(ccxt, exchange_id)(config)


def test_paradex_trades_with_a_subkey_and_no_main_wallet_key():
    env = {"PARADEX_WALLET_ADDRESS": "0xabc", "PARADEX_L2_PRIVATE_KEY": "0x123",
           "PARADEX_L2_ADDRESS": "0xdef"}
    config = build_ccxt_config_from_env("paradex", env)
    assert "privateKey" not in config
    assert missing_required_credentials("paradex", config) == []
    validate_ccxt_config("paradex", config)
    # CCXT uses the cached L2 account and never derives it from the main Ethereum key.
    assert _client("paradex", config).retrieve_account() == {"privateKey": "0x123", "address": "0xdef"}


def test_lighter_trades_with_an_existing_api_key_not_the_l1_key():
    env = {"LIGHTER_API_KEY_PRIVATE_KEY": "secret-api-key", "LIGHTER_API_KEY_INDEX": "3",
           "LIGHTER_ACCOUNT_INDEX": "42", "LIGHTER_LIBRARY_PATH": "/opt/lighter/signer.so"}
    config = build_ccxt_config_from_env("lighter", env)
    assert "privateKey" not in config
    assert missing_required_credentials("lighter", config) == []
    client = _client("lighter", config)
    assert client.options["accountIndex"] == 42 and client.options["apiKeyIndex"] == 3
    assert client.get_lighter_private_key("42", "3") == "secret-api-key"
    assert client.options["libraryPath"] == "/opt/lighter/signer.so"


def test_partial_agent_key_still_reports_what_is_missing():
    config = build_ccxt_config_from_env("lighter", {"LIGHTER_API_KEY_PRIVATE_KEY": "x"})
    missing = missing_required_credentials("lighter", config)
    assert "privateKey" in missing
    with pytest.raises(ValueError, match="LIGHTER_API_KEY_INDEX"):
        validate_ccxt_config("lighter", config)


def test_dydx_uses_chain_mnemonic_not_ethereum_key():
    config = build_ccxt_config_from_env("dydx", {"DYDX_MNEMONIC": "word " * 24,
                                                 "DYDX_WALLET_ADDRESS": "dydx1abc"})
    client = _client("dydx", config)
    assert client.options["mnemonic"].startswith("word")
    assert "privateKey" not in config


def test_main_key_path_unchanged():
    config = build_ccxt_config_from_env("paradex", {"PARADEX_WALLET_ADDRESS": "0x1", "PARADEX_PRIVATE_KEY": "0x2"})
    assert config["privateKey"] == "0x2" and "options" not in config
    assert missing_required_credentials("paradex", config) == []


def test_catalog_lists_agent_key_fields_as_secret():
    for exchange_id in AGENT_KEY_CREDENTIALS:
        fields = exchange_entry(exchange_id)["agentKeyFields"]
        assert fields
        assert all(f["key"].startswith(exchange_id.upper() + "_") for f in fields)
    secrets = {f["key"]: f["secret"] for f in exchange_entry("lighter")["agentKeyFields"]}
    assert secrets["LIGHTER_API_KEY_PRIVATE_KEY"] is True and secrets["LIGHTER_ACCOUNT_INDEX"] is False
