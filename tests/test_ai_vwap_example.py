from lumibot.example_strategies.ai_vwap import build_vwap_system_prompt


def test_vwap_entry_requires_reclaim_confirmation():
    prompt = build_vwap_system_prompt({})

    assert "require reclaim evidence" in prompt.lower()
    assert "do not skip a clear dip" not in prompt.lower()
