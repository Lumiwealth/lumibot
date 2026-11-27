"""Discord account summary values contract positions with the asset multiplier."""

from datetime import date
from types import SimpleNamespace

from lumibot.entities import Asset
from lumibot.entities.position import Position
from lumibot.strategies._strategy import _Strategy


class _SummaryStrategy:
    is_backtesting = False
    hide_positions = False
    discord_account_summary_footer = None
    _name = "summary-test"

    def __init__(self, prices):
        self._prices = prices
        self._positions = []
        self._quote_asset = Asset("USD", asset_type=Asset.AssetType.FOREX)
        self.logger = SimpleNamespace(info=lambda *a, **k: None)
        self.sent = []

    def get_positions(self):
        return self._positions

    def get_last_price(self, asset, **kwargs):
        return self._prices[asset]

    def send_discord_message(self, message, image=None, **kwargs):
        self.sent.append(message)


def _positions_lines(message):
    lines = message.split("\n")
    start = lines.index("**Positions:**") + 1
    result = []
    for line in lines[start:]:
        if not line.strip():
            break
        result.append(line)
    return result


def _render(assets_prices, portfolio_value=1_000_000.0):
    """Render the summary for ``(asset, last_price[, quantity[, fill_price]])`` entries.

    The fill price defaults to half the last price so the rendered values must come
    from the current quote rather than the acquisition cost.
    """
    entries = []
    for entry in assets_prices:
        asset, last_price, *rest = entry
        quantity = rest[0] if len(rest) > 0 else 1
        fill_price = rest[1] if len(rest) > 1 else last_price / 2
        entries.append((asset, last_price, quantity, fill_price))
    fake = _SummaryStrategy({asset: last for asset, last, _, _ in entries})
    fake._positions = [
        Position(fake, asset, qty, avg_fill_price=fill) for asset, _, qty, fill in entries
    ]
    _Strategy.send_result_text_to_discord(fake, "returns_text", portfolio_value=portfolio_value, cash=0.0)
    assert len(fake.sent) == 1
    return _positions_lines(fake.sent[0])


def test_future_position_uses_multiplier():
    es = Asset("ES", asset_type=Asset.AssetType.FUTURE, multiplier=50, expiration=date(2026, 12, 18))
    lines = _render([(es, 6000.0)])
    assert lines == [f"1.00 {es} ($300,000 or 30%)"]


def test_future_position_value_scales_with_quantity():
    es = Asset("ES", asset_type=Asset.AssetType.FUTURE, multiplier=50, expiration=date(2026, 12, 18))
    lines = _render([(es, 6000.0, 2)], portfolio_value=100_000.0)
    assert lines == [f"2.00 {es} ($600,000 or 600%)"]


def test_continuous_future_position_uses_multiplier():
    mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE, multiplier=5)
    lines = _render([(mes, 6000.0)])
    assert lines == [f"1.00 {mes} ($30,000 or 3%)"]


def test_adjusted_option_uses_its_multiplier_not_100():
    adj = Asset(
        "AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 12, 18),
        strike=200.0,
        right="call",
        multiplier=10,
    )
    # Bought at $5, now quoted at $6: the summary shows current value.
    lines = _render([(adj, 6.0, 1, 5.0)], portfolio_value=600.0)
    assert lines == [f"1.00 {adj} ($60 or 10%)"]


def test_standard_option_and_stock_values_unchanged():
    opt = Asset(
        "SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 12, 18),
        strike=500.0,
        right="put",
    )
    # Explicit multiplier metadata on a stock must not scale its value.
    stock = Asset("MSFT", asset_type=Asset.AssetType.STOCK, multiplier=7)
    lines = _render([(opt, 5.0), (stock, 400.0)], portfolio_value=1000.0)
    assert lines == [
        f"1.00 {opt} ($500 or 50%)",
        f"1.00 {stock} ($400 or 40%)",
    ]


def test_summary_from_issue_report_orders_by_corrected_value():
    es = Asset("ES", asset_type=Asset.AssetType.FUTURE, multiplier=50, expiration=date(2026, 12, 18))
    adj = Asset(
        "AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 12, 18),
        strike=200.0,
        right="call",
        multiplier=10,
    )
    lines = _render([(adj, 6.0, 1, 5.0), (es, 6000.0)], portfolio_value=100_000.0)
    assert lines == [
        f"1.00 {es} ($300,000 or 300%)",
        f"1.00 {adj} ($60 or 0%)",
    ]
