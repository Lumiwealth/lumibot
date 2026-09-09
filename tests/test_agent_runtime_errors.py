from lumibot.components.agents.runtime import _classify_agent_error


class _ProviderError(Exception):
    def __init__(self, message: str, status_code: int | None = None, code: str | None = None):
        super().__init__(message)
        if status_code is not None:
            self.status_code = status_code
        if code is not None:
            self.code = code


def test_xai_monthly_spending_limit_classifies_as_billing():
    exc = _ProviderError(
        '{"code":"Some resource has been exhausted","error":"Your team has either used all available credits or reached its monthly spending limit."}',
        status_code=429,
    )

    assert _classify_agent_error(exc) == "billing"


def test_plain_rate_limit_still_classifies_as_transient():
    exc = type("RateLimitError", (Exception,), {})("too many requests right now")

    assert _classify_agent_error(exc) == "transient"


def test_managed_provider_hard_quota_classifies_as_billing():
    exc = _ProviderError(
        "Managed AI provider quota is exhausted.",
        status_code=503,
        code="provider_quota_exhausted",
    )

    assert _classify_agent_error(exc) == "billing"


def test_managed_provider_schema_rejection_classifies_as_config():
    exc = _ProviderError(
        "Managed AI provider rejected the function schema.",
        status_code=502,
        code="protocol_integrity_error",
    )

    assert _classify_agent_error(exc) == "config"
