"""Helpers for keeping agent handoffs inside a token budget."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


_FALLBACK_CHARS_PER_TOKEN = 4


@dataclass(frozen=True)
class TokenBudgetedText:
    text: str
    estimated_tokens: int
    was_truncated: bool
    original_estimated_tokens: int


def estimate_token_count(value: Any, *, model: str | None = None) -> int:
    """Estimate token count for prompt budgeting.

    Uses LiteLLM/tiktoken when available, then falls back to the common rough
    English heuristic of four characters per token.
    """

    text = "" if value is None else str(value)
    if not text:
        return 0
    try:
        import litellm

        token_count = litellm.token_counter(model=model or "gpt-4o-mini", text=text)
        if token_count is not None:
            return max(int(token_count), 0)
    except Exception:
        pass
    return max((len(text) + _FALLBACK_CHARS_PER_TOKEN - 1) // _FALLBACK_CHARS_PER_TOKEN, 1)


def budget_text_by_tokens(
    value: Any,
    *,
    max_tokens: int,
    label: str = "agent handoff",
    model: str | None = None,
) -> TokenBudgetedText:
    """Return text no larger than ``max_tokens`` using middle truncation.

    The caller should still prompt the model to be concise. This helper is a
    last-resort safety rail for provider context windows.
    """

    text = "" if value is None else str(value)
    max_tokens = max(int(max_tokens), 256)
    original_tokens = estimate_token_count(text, model=model)
    if original_tokens <= max_tokens:
        return TokenBudgetedText(
            text=text,
            estimated_tokens=original_tokens,
            was_truncated=False,
            original_estimated_tokens=original_tokens,
        )

    notice = (
        f"\n\n[Token-budget safety truncation: {label} was about {original_tokens} tokens; "
        f"retained the beginning and end within a {max_tokens} token budget.]\n\n"
    )
    notice_tokens = estimate_token_count(notice, model=model)
    remaining_tokens = max(max_tokens - notice_tokens, 256)
    head_tokens = max(int(remaining_tokens * 0.65), 128)
    tail_tokens = max(remaining_tokens - head_tokens, 128)

    try:
        import tiktoken

        encoding = tiktoken.encoding_for_model(model or "gpt-4o-mini")
        encoded = encoding.encode(text)
        truncated = encoding.decode(encoded[:head_tokens]) + notice + encoding.decode(encoded[-tail_tokens:])
    except Exception:
        chars_per_token = max(len(text) // max(original_tokens, 1), 1)
        head_chars = head_tokens * chars_per_token
        tail_chars = tail_tokens * chars_per_token
        truncated = text[:head_chars] + notice + text[-tail_chars:]

    return TokenBudgetedText(
        text=truncated,
        estimated_tokens=estimate_token_count(truncated, model=model),
        was_truncated=True,
        original_estimated_tokens=original_tokens,
    )
