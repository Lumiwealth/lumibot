import lumibot.example_strategies.ai_vwap as ai_vwap
from lumibot.components.agents.skills import load_builtin_skills
from lumibot.example_strategies.ai_vwap import build_vwap_system_prompt


def test_vwap_entry_requires_reclaim_confirmation():
    prompt = build_vwap_system_prompt({})

    assert "require reclaim evidence" in prompt.lower()
    assert "do not skip a clear dip" not in prompt.lower()


def test_vwap_signal_window_is_the_evaluation_cadence_not_one_bar():
    """vwap-luna-v4 (2026-01-06): SPY closed 0.150% below running VWAP at 11:07
    and reclaimed it at 11:53. At the 12:00 evaluation every agent rejected the
    entry as 'six completed bars old, beyond the one-bar horizon', so a 30-minute
    cadence could only act on a reclaim in the final minute before a wake-up."""
    prompt = " ".join(build_vwap_system_prompt({"sleeptime": "30M"}).split())

    assert "since the previous evaluation (30M)" in prompt
    assert "still at or above VWAP" in prompt
    assert "hold_bars counts bars after entry" in prompt


def test_vwap_example_has_a_single_trading_prompt_owner():
    assert not hasattr(ai_vwap, "build_vwap_trading_prompt")


def test_stock_skill_scopes_intraday_signals_to_the_evaluation_cadence():
    stock_skill = next(skill for skill in load_builtin_skills() if skill.name == "stock-trading")
    intraday = " ".join(stock_skill.resources.references["intraday-setups.md"].split())

    assert "formed at any completed bar since the previous evaluation" in intraday
    assert "A holding period counts bars after entry" in intraday
