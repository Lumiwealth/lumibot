"""Unit tests for lumibot.entities.chains helpers."""
from datetime import date, datetime

import pytest

from lumibot.entities import chains as chains_module
from lumibot.entities.chains import OptionsDataFormatError, _normalise_expiry


def test_normalise_expiry_accepts_the_supported_shapes():
    assert _normalise_expiry("2026-08-21") == date(2026, 8, 21)
    assert _normalise_expiry("20260821") == date(2026, 8, 21)
    assert _normalise_expiry(" 2026-08-21 ") == date(2026, 8, 21)
    assert _normalise_expiry(date(2026, 8, 21)) == date(2026, 8, 21)
    assert _normalise_expiry(datetime(2026, 8, 21, 16, 0)) == date(2026, 8, 21)


def test_normalise_expiry_still_rejects_bad_text_every_time():
    for _ in range(2):
        with pytest.raises(OptionsDataFormatError):
            _normalise_expiry("2026-13-45")
        with pytest.raises(OptionsDataFormatError):
            _normalise_expiry("")
        with pytest.raises(OptionsDataFormatError):
            _normalise_expiry("next friday")


def test_normalise_expiry_parses_each_distinct_text_once(monkeypatch):
    """Option helpers normalise every expiry key of a chain on every call.

    A minute-level chain scan re-parsed the same few dozen strings about 150,000 times in one
    simulated week (yappi, 2026-11-10). The parse is a pure function of the text, so it is
    remembered.
    """
    calls = []
    real_datetime = chains_module.datetime

    class CountingDatetime(real_datetime):
        @classmethod
        def strptime(cls, text, fmt):
            calls.append(text)
            return real_datetime.strptime(text, fmt)

    monkeypatch.setattr(chains_module, "datetime", CountingDatetime)
    text = "2031-03-21"  # not used by any other test, so the first call is a real parse
    for _ in range(50):
        assert _normalise_expiry(text) == date(2031, 3, 21)
    assert calls == ["20310321"]
