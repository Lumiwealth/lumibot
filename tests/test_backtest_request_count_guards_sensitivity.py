"""The request-count guards must fail when the behavior they guard comes back (test the tests)."""
import pytest

from tests import test_backtest_request_count_guards as guards


def test_pick_from_backtest_start_fails_the_guard(monkeypatch, tmp_path):
    from lumibot.backtesting.alpaca_backtesting import AlpacaBacktesting

    original = AlpacaBacktesting.prefetch_option_bars

    def no_until(self, assets, quote=None, timestep=None, until=None):
        return original(self, assets, quote, timestep, until=None)  # whole window, from the backtest start

    monkeypatch.setattr(AlpacaBacktesting, "prefetch_option_bars", no_until)
    with pytest.raises(AssertionError):
        guards.test_month_of_option_picks_fetches_chains_once_a_day_and_reads_only_the_pick_day(monkeypatch, tmp_path)


def test_refetch_every_call_fails_the_ibkr_guard(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    # Regression: one-hour pages instead of 1000-point pages (16 times the requests).
    monkeypatch.setattr(ibkr_helper, "_max_period_for_bar", lambda bar: "60min")
    with pytest.raises(AssertionError):
        guards.test_ibkr_stock_minute_backtest_reads_each_session_once_and_nothing_when_warm(monkeypatch, tmp_path)
