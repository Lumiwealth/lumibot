from lumibot.components.agents.runtime import _classify_agent_error


class _ProviderError(Exception):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        if status_code is not None:
            self.status_code = status_code


def test_xai_monthly_spending_limit_classifies_as_billing():
    exc = _ProviderError(
        '{"code":"Some resource has been exhausted","error":"Your team has either used all available credits or reached its monthly spending limit."}',
        status_code=429,
    )

    assert _classify_agent_error(exc) == "billing"


def test_plain_rate_limit_still_classifies_as_transient():
    exc = type("RateLimitError", (Exception,), {})("too many requests right now")

    assert _classify_agent_error(exc) == "transient"
