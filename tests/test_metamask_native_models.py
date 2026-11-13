"""Canonical native-token identity and owner registry validation."""

import pytest

from lumibot.tools.metamask.models import Binding, TokenAsset
from lumibot.tools.metamask.provider import MetaMaskError
from tests.test_metamask_native_journal import binding


@pytest.mark.parametrize(
    "chain,coin",
    [
        (1, 60),
        (10, 60),
        (137, 966),
        (56, 714),
        (43114, 9005),
        (97, 1),
        (43113, 1),
        (80002, 1),
        (421614, 1),
        (84532, 1),
        (11155111, 1),
        (11155420, 1),
    ],
)
def test_native_price_identity_uses_chain_coin_type(chain, coin):
    asset = TokenAsset.parse({"symbol": "GAS", "chainId": chain, "address": "native", "decimals": 18})
    assert asset.caip == f"eip155:{chain}/slip44:{coin}"


def test_unknown_native_chain_never_inherits_eth_identity():
    asset = TokenAsset.parse({"symbol": "GAS", "chainId": 999999, "address": "native", "decimals": 18})
    with pytest.raises(MetaMaskError, match="NATIVE_ASSET_UNSUPPORTED"):
        _ = asset.caip


@pytest.mark.parametrize("alias", [{}, {"symbol": "ALIAS"}, {"decimals": 8}])
def test_duplicate_contract_registry_cannot_double_account_value(alias):
    token = {"symbol": "TOKEN", "chainId": 421614, "address": "0x" + "2" * 40, "decimals": 6}
    with pytest.raises(MetaMaskError, match="duplicate chain and contract"):
        Binding.parse({**binding(), "assets": [token, {**token, **alias}]})


def test_registry_requires_approved_chains_and_keeps_distinct_chain_assets():
    token = {"symbol": "TOKEN", "chainId": 421614, "address": "0x" + "2" * 40, "decimals": 6}
    other = {**token, "chainId": 1}
    with pytest.raises(MetaMaskError, match="unapproved chain"):
        Binding.parse({**binding(), "assets": [other]})
    result = Binding.parse({**binding(), "chains": [421614, 1], "assets": [token, other]})
    assert len(result.assets) == 2 and result.assets[0].identity != result.assets[1].identity
