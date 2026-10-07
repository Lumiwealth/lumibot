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


# Customer investigation (2026-10-06): OpenAI 429 rate-limit traces appeared in a
# customer's runs. In a backtest the runtime gave up after two quick tries (and trading
# agents after one), so the bar was silently skipped. A rate limit must wait
# for the provider's Retry-After and retry a bounded number of times, unless
# the failed attempt already touched the broker.
import pytest

from lumibot.components.agents.runtime import GoogleADKRuntime, RuntimeRequest
from lumibot.components.agents.schemas import AgentRunResult, BoundTool


class _Headers(dict):
    def get(self, key, default=None):  # case-insensitive like httpx
        for k, v in self.items():
            if k.lower() == str(key).lower():
                return v
        return default


class _Response:
    def __init__(self, headers):
        self.headers = _Headers(headers)


class RateLimitError(Exception):
    def __init__(self, message="Rate limit reached for gpt-4o. Please try again in 7s.", retry_after="7"):
        super().__init__(message)
        self.status_code = 429
        self.response = _Response({"Retry-After": retry_after} if retry_after is not None else {})


def _rate_limit_request(*, trading: bool) -> RuntimeRequest:
    tools = []
    if trading:
        tools.append(BoundTool(name="orders_submit_order", description="submit", function=lambda **_: {"ok": True}))
    return RuntimeRequest(
        agent_name="analyst",
        model="openai/gpt-4o",
        system_prompt="Analyze",
        task_prompt="Analyze INTC",
        context=None,
        runtime_context={"mode": "backtesting"},
        memory_state=None,
        memory_notes=[],
        bound_tools=tools,
    )


@pytest.mark.parametrize("trading", [False, True])
def test_backtest_rate_limit_waits_for_retry_after_and_retries(monkeypatch, trading):
    monkeypatch.delenv("LUMIBOT_AGENT_MAX_RUN_ATTEMPTS", raising=False)
    runtime = GoogleADKRuntime()
    request = _rate_limit_request(trading=trading)
    attempts = {"count": 0}

    async def flaky(req):
        attempts["count"] += 1
        if attempts["count"] <= 3:
            raise RateLimitError()
        return AgentRunResult(summary="PASS", model=req.model, events=[])

    sleeps = []
    monkeypatch.setattr(runtime, "_run_async", flaky)
    monkeypatch.setattr("time.sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setenv("LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS", "0")

    result = runtime.run(request)

    assert result.summary == "PASS"
    assert attempts["count"] == 4
    assert sleeps and all(delay >= 7 for delay in sleeps)


def test_rate_limit_is_not_retried_after_the_attempt_touched_the_broker(monkeypatch):
    monkeypatch.delenv("LUMIBOT_AGENT_MAX_RUN_ATTEMPTS", raising=False)
    runtime = GoogleADKRuntime()
    request = _rate_limit_request(trading=True)
    attempts = {"count": 0}

    async def submits_then_rate_limited(req):
        attempts["count"] += 1
        # The runtime records broker side effects of the current attempt here.
        req._attempt_tool_calls.append({"tool_name": "orders_submit_order", "ok": True})
        raise RateLimitError()

    monkeypatch.setattr(runtime, "_run_async", submits_then_rate_limited)
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    monkeypatch.setenv("LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS", "0")

    with pytest.raises(RateLimitError):
        runtime.run(request)
    assert attempts["count"] == 1


def test_retry_after_is_read_from_headers_and_provider_messages():
    from lumibot.components.agents.runtime import _retry_after_seconds

    assert _retry_after_seconds(RateLimitError(retry_after="12")) == 12
    assert _retry_after_seconds(RateLimitError("Please try again in 1.5s.", retry_after=None)) == 1.5
    assert _retry_after_seconds(RateLimitError("Please try again in 450ms.", retry_after=None)) == 0.45
    assert _retry_after_seconds(RateLimitError("RESOURCE_EXHAUSTED. Please retry in 34.2s", retry_after=None)) == 34.2
    assert _retry_after_seconds(RuntimeError("boom")) is None


def test_rate_limit_is_not_retried_after_a_multileg_order_was_submitted(monkeypatch):
    monkeypatch.delenv("LUMIBOT_AGENT_MAX_RUN_ATTEMPTS", raising=False)
    runtime = GoogleADKRuntime()
    request = _rate_limit_request(trading=True)
    attempts = {"count": 0}

    async def multileg_then_rate_limited(req):
        attempts["count"] += 1
        req._attempt_tool_calls.append({"tool_name": "orders_submit_multileg", "ok": True})
        raise RateLimitError()

    monkeypatch.setattr(runtime, "_run_async", multileg_then_rate_limited)
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    monkeypatch.setenv("LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS", "0")

    with pytest.raises(RateLimitError):
        runtime.run(request)
    assert attempts["count"] == 1


def test_multileg_tool_disables_whole_run_retries_like_other_order_tools(monkeypatch):
    monkeypatch.delenv("LUMIBOT_AGENT_MAX_RUN_ATTEMPTS", raising=False)
    request = _rate_limit_request(trading=False)
    request.bound_tools.append(
        BoundTool(name="orders_submit_multileg", description="multileg", function=lambda **_: {"ok": True})
    )
    request.runtime_context = {"mode": "live"}

    assert GoogleADKRuntime._max_attempts_for_request(request) == 1


def test_retry_after_http_date_is_honored():
    from datetime import datetime, timedelta, timezone
    from email.utils import format_datetime

    from lumibot.components.agents.runtime import _retry_after_seconds

    future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True)
    past = format_datetime(datetime.now(timezone.utc) - timedelta(seconds=30), usegmt=True)

    assert 25 <= _retry_after_seconds(RateLimitError(retry_after=future)) <= 31
    assert _retry_after_seconds(RateLimitError(retry_after=past)) == 0


def test_managed_gateway_rate_limit_counts_as_a_rate_limit():
    # BotSpot's managed AI gateway retries the provider's 429 itself (up to 3 times,
    # honoring Retry-After) and then answers 503 with code provider_rate_limited.
    # LumiBot must treat that as the provider's rate limit, not a generic error.
    from lumibot.components.agents.managed_gateway import ManagedAiGatewayError
    from lumibot.components.agents.runtime import _is_rate_limit_error

    exc = ManagedAiGatewayError(
        "Managed AI provider rejected the request.", status_code=503, code="provider_rate_limited"
    )
    assert _classify_agent_error(exc) == "transient"
    assert _is_rate_limit_error(exc)
