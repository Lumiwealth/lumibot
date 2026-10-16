"""Empty near-the-money chains must never repeat the same request recursively."""
from types import SimpleNamespace

import pytest

from lumibot.data_sources.schwab_data import SchwabData
from lumibot.entities import Asset


def data_source(responses):
    calls = []
    def get_option_chain(**kwargs):
        calls.append(kwargs.copy())
        # Abort the pre-fix recursion at the third call, without waiting for
        # Python's recursion limit. The assertion below still exposes it.
        if len(calls) > len(responses):
            raise AssertionError("unbounded repeated option-chain request")
        payload = responses[len(calls) - 1]
        return SimpleNamespace(status_code=200, json=lambda: payload)
    data = SchwabData(auto_create_client=False)
    data.client = SimpleNamespace(
        get_option_chain=get_option_chain,
        Options=SimpleNamespace(
            StrikeRange=SimpleNamespace(NTM="NTM", ALL="ALL"),
            ContractType=SimpleNamespace(ALL="ALL"),
            Strategy=SimpleNamespace(SINGLE="SINGLE"),
        ),
    )
    return data, calls


CHAIN = {"callExpDateMap": {"2026-10-16:1": {"20.0": [{}]}}}


def test_empty_initial_chain_retries_all_strikes_once():
    data, calls = data_source([{}, CHAIN])
    chain = data.get_chains(Asset("ABC"), strike_count=17)
    assert [call["strike_range"] for call in calls] == ["NTM", "ALL"]
    assert chain["Chains"]["CALL"]["2026-10-16"] == [20]
    assert all(call["strike_count"] == 17 for call in calls)


def test_two_empty_responses_stop_instead_of_restarting_the_original_range():
    data, calls = data_source([{}, {}])
    data.get_chains(Asset("ABC"))
    assert [call["strike_range"] for call in calls] == ["NTM", "ALL"]


def test_successful_initial_chain_does_not_repeat():
    data, calls = data_source([CHAIN])
    assert data.get_chains(Asset("ABC"))["Chains"]["CALL"]["2026-10-16"] == [20]
    assert len(calls) == 1


@pytest.mark.parametrize("payload", [{"callExpDateMap": {}, "putExpDateMap": {}}, {"callExpDateMap": {"2026-10-16:1": {}}}])
def test_empty_maps_are_not_mistaken_for_available_strikes(payload):
    data, calls = data_source([payload, CHAIN])
    assert data.get_chains(Asset("ABC"))["Chains"]["CALL"]["2026-10-16"] == [20]
    assert len(calls) == 2
