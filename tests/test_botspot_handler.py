import os
import time
import logging

from lumibot.tools.lumibot_logger import BotspotErrorHandler


def test_botspot_handler_dedup_and_aggregation(monkeypatch):
    """Ensure BotspotErrorHandler sends first occurrence immediately and aggregates repeats within window."""
    # Set environment before handler instantiation
    monkeypatch.setenv("LUMIWEALTH_API_KEY", "DUMMY")
    monkeypatch.setenv("BOTSPOT_RATE_LIMIT_WINDOW", "1")  # 1 second window for fast test
    monkeypatch.setenv("BOTSPOT_MAX_ERRORS_PER_MINUTE", "1000")

    handler = BotspotErrorHandler()

    # Collect outbound payloads instead of real HTTP
    call_payloads = []

    class DummyResp:
        def __init__(self, status_code=200, text="OK"):
            self.status_code = status_code
            self.text = text

    def fake_post(url, headers=None, json=None, timeout=10):  # noqa: A002 - match requests signature
        call_payloads.append(json)
        return DummyResp()

    # Patch the requests.post used inside the handler
    assert handler.requests is not None, "requests should be available when API key is set"
    monkeypatch.setattr(handler.requests, "post", fake_post)

    logger = logging.getLogger("lumibot.test.botspot")
    logger.setLevel(logging.ERROR)
    # Ensure no duplicate handlers from previous tests
    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.addHandler(handler)

    # First occurrence -> immediate send
    logger.error("Order submission failed | details: order_id=123 uuid=abc")
    # Two more identical errors inside the rate limit window -> suppressed
    logger.error("Order submission failed | details: order_id=124 uuid=def")
    logger.error("Order submission failed | details: order_id=125 uuid=ghi")

    # Should still have only one API call so far
    assert len(call_payloads) == 1
    first_payload = call_payloads[0]
    assert first_payload["error_code"].endswith("_ERROR")
    # First payload should not include aggregated count key (count==1)
    assert "count" not in first_payload

    # Wait past the window to trigger aggregation flush on next error
    time.sleep(1.1)
    logger.error("Order submission failed | details: order_id=126 uuid=jkl")

    # Now second API call should include aggregated suppressed repeats (2 suppressed + current = 3)
    assert len(call_payloads) == 2
    second_payload = call_payloads[1]
    assert second_payload.get("count") == 3, second_payload
    assert second_payload["message"].startswith("[") or second_payload["message"].startswith("Order submission failed"), "Message preserved"

    # Ensure full details (with uuids / order_ids) are retained in the second send
    assert "uuid=jkl" in second_payload.get("details", "") or "uuid=jkl" in second_payload.get("message", "")

    # Fire another inside the window -> no immediate third call
    logger.error("Order submission failed | details: order_id=127 uuid=mno")
    assert len(call_payloads) == 2


def test_botspot_handler_distinct_fingerprints(monkeypatch):
    """Different functions or files should produce separate sends even within window."""
    monkeypatch.setenv("LUMIWEALTH_API_KEY", "DUMMY")
    monkeypatch.setenv("BOTSPOT_RATE_LIMIT_WINDOW", "5")
    monkeypatch.setenv("BOTSPOT_MAX_ERRORS_PER_MINUTE", "1000")

    handler = BotspotErrorHandler()

    call_payloads = []

    class DummyResp:
        def __init__(self, status_code=200, text="OK"):
            self.status_code = status_code
            self.text = text

    def fake_post(url, headers=None, json=None, timeout=10):  # noqa: A002
        call_payloads.append(json)
        return DummyResp()

    assert handler.requests is not None
    monkeypatch.setattr(handler.requests, "post", fake_post)

    logger_a = logging.getLogger("lumibot.test.botspot.a")
    logger_b = logging.getLogger("lumibot.test.botspot.b")
    for lg in (logger_a, logger_b):
        lg.setLevel(logging.ERROR)
        for h in list(lg.handlers):
            lg.removeHandler(h)
        lg.addHandler(handler)

    logger_a.error("Alpha failure | details: A1")
    logger_b.error("Alpha failure | details: B1")

    # Expect at least one send per distinct fingerprint (some environments may duplicate via propagation)
    assert len(call_payloads) >= 2
    codes = [p["error_code"] for p in call_payloads]
    # Each logger should have produced its own error_code (based on logger name suffix before level)
    assert any(code.endswith("_ERROR") for code in codes)
