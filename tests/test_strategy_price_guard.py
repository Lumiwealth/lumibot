import math
from lumibot.components.options_helper import OptionsHelper, OptionMarketEvaluation


class _GuardedStrategy:
    def __init__(self):
        self.logged = []
        self.options_helper = OptionsHelper(self)

    def log_message(self, message, color="white"):
        self.logged.append((color, message))

    def get_cash(self):
        return 10_000.0

    def size_position(self, evaluation: OptionMarketEvaluation):
        if evaluation.spread_too_wide or not self.options_helper.has_actionable_price(evaluation):
            self.log_message(
                f"Skipping trade due to invalid quotes (flags={evaluation.data_quality_flags}).",
                color="yellow",
            )
            return 0

        buy_price = float(evaluation.buy_price)
        budget = self.get_cash() * 0.1
        return math.floor(budget / (buy_price * 100.0))


def test_strategy_guard_skips_non_finite_prices():
    strategy = _GuardedStrategy()
    evaluation = OptionMarketEvaluation(
        bid=None,
        ask=None,
        last_price=None,
        spread_pct=None,
        has_bid_ask=False,
        spread_too_wide=False,
        missing_bid_ask=True,
        missing_last_price=True,
        buy_price=float("nan"),
        sell_price=None,
        used_last_price_fallback=False,
        max_spread_pct=None,
        data_quality_flags=["buy_price_non_finite"],
    )

    contracts = strategy.size_position(evaluation)

    assert contracts == 0
    assert any("invalid quotes" in msg for _, msg in strategy.logged)
