from lumibot.components.agents.context_budget import budget_text_by_tokens, estimate_token_count


def test_estimate_token_count_uses_positive_budget():
    assert estimate_token_count("hello world") > 0


def test_budget_text_by_tokens_keeps_small_text():
    result = budget_text_by_tokens("short handoff", max_tokens=1000, label="handoff")

    assert result.text == "short handoff"
    assert result.was_truncated is False
    assert result.estimated_tokens <= 1000


def test_budget_text_by_tokens_middle_truncates_large_text():
    text = "alpha " * 20000

    result = budget_text_by_tokens(text, max_tokens=1000, label="evidence_pack")

    assert result.was_truncated is True
    assert result.original_estimated_tokens > result.estimated_tokens
    assert result.estimated_tokens <= 1100
    assert "Token-budget safety truncation" in result.text
    assert "evidence_pack" in result.text
