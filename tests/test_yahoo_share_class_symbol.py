"""Yahoo lists share classes with a dash ("ABC-B"); a dot ("ABC.B", how IBKR-routed backtests and
most index lists write them) finds nothing, so every daily corporate-action lookup failed and was
retried each simulated day (Nov 17 2026, two S&P 500 members). Symbols are synthetic."""
import pandas as pd
import pytest

from lumibot.tools.yahoo_helper import YahooHelper


@pytest.mark.parametrize(
    "written, asked",
    [("ABC.B", "ABC-B"), ("abc.a", "ABC-A"), ("ABC", "ABC"), ("ABC-B", "ABC-B"), ("^GSPC", "^GSPC"), ("XYZ-USD", "XYZ-USD")],
)
def test_share_class_with_a_dot_is_asked_with_a_dash(monkeypatch, written, asked):
    seen = []

    def fake_fetch(symbol, interval="1d", caching=True, last_needed_datetime=None):
        seen.append(symbol)
        return pd.DataFrame()

    monkeypatch.setattr(YahooHelper, "fetch_symbol_data", staticmethod(fake_fetch))
    monkeypatch.setattr(YahooHelper, "format_df", staticmethod(lambda df, auto_adjust: df))
    YahooHelper.get_symbol_data(written)
    assert seen == [asked]
