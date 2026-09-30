from pathlib import Path

from lumibot.components.agents.skills import load_builtin_skills

VWAP_EXAMPLE = Path(__file__).resolve().parents[1] / "lumibot" / "example_strategies" / "ai_vwap.py"


def test_vwap_signal_window_is_the_evaluation_cadence_not_one_bar():
    """vwap-luna-v4 (2026-01-06): SPY closed 0.150% below running VWAP at 11:07
    and reclaimed it at 11:53. At the 12:00 evaluation every agent rejected the
    entry as 'six completed bars old', so an hourly bot could only act on a
    reclaim in the final minute before it woke up."""
    source = " ".join(VWAP_EXAMPLE.read_text().replace('"', " ").split())

    assert "since the last hourly check" in source
    assert "closed back above it" in source


def test_stock_skill_scopes_intraday_signals_to_the_evaluation_cadence():
    stock_skill = next(skill for skill in load_builtin_skills() if skill.name == "stock-trading")
    intraday = " ".join(stock_skill.resources.references["intraday-setups.md"].split())

    assert "formed at any completed bar since the previous evaluation" in intraday
    assert "A holding period counts bars after entry" in intraday
