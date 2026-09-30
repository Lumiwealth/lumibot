from decimal import Decimal
from unittest.mock import MagicMock

from lumibot.entities import TradingFee, TradingSlippage


class TestTradingFee:
    def test_init(self):
        fee = TradingFee(flat_fee=5.2)
        assert fee.flat_fee == Decimal("5.2")

    def test_per_contract_fee_init(self):
        fee = TradingFee(per_contract_fee=0.65)
        assert fee.per_contract_fee == Decimal("0.65")
        assert fee.flat_fee == Decimal("0")
        assert fee.percent_fee == Decimal("0")

    def test_per_contract_fee_default_zero(self):
        fee = TradingFee(flat_fee=1.0)
        assert fee.per_contract_fee == Decimal("0")

    def test_per_contract_fee_with_flat_fee(self):
        fee = TradingFee(flat_fee=5.0, per_contract_fee=0.65)
        assert fee.flat_fee == Decimal("5.0")
        assert fee.per_contract_fee == Decimal("0.65")

    def test_slippage_init(self):
        slippage = TradingSlippage(amount=0.15)
        assert slippage.amount == 0.15


class TestPerContractFeeCalculation:
    """Test that per_contract_fee is correctly multiplied by order quantity in calculate_trade_cost."""

    def _make_order(self, side="sell_to_open", order_type="market", quantity=40):
        order = MagicMock()
        order.side = side
        order.order_type = MagicMock()
        order.order_type.value = order_type
        order.quantity = quantity
        return order

    def _make_strategy(self, buy_fees=None, sell_fees=None):
        strategy = MagicMock()
        strategy.buy_trading_fees = buy_fees or []
        strategy.sell_trading_fees = sell_fees or []
        return strategy

    def test_per_contract_fee_multiplied_by_quantity(self):
        """$0.65/contract on a 40-contract order should cost $26.00."""
        from lumibot.backtesting.backtesting_broker import BacktestingBroker

        broker = BacktestingBroker.__new__(BacktestingBroker)
        fee = TradingFee(per_contract_fee=0.65)
        order = self._make_order(side="sell_to_open", order_type="market", quantity=40)
        strategy = self._make_strategy(sell_fees=[fee])

        cost = broker.calculate_trade_cost(order, strategy, price=1.50)
        assert cost == Decimal("26.00")

    def test_per_contract_fee_with_flat_fee(self):
        """Both flat_fee and per_contract_fee should apply."""
        from lumibot.backtesting.backtesting_broker import BacktestingBroker

        broker = BacktestingBroker.__new__(BacktestingBroker)
        fee = TradingFee(flat_fee=5.0, per_contract_fee=0.65)
        order = self._make_order(side="buy_to_open", order_type="market", quantity=10)
        strategy = self._make_strategy(buy_fees=[fee])

        cost = broker.calculate_trade_cost(order, strategy, price=2.00)
        # flat_fee=5.0 + per_contract=10*0.65=6.50 + percent=0 = 11.50
        assert cost == Decimal("11.50")

    def test_per_contract_fee_on_limit_order(self):
        """Per-contract fee should work with limit/smart_limit orders too."""
        from lumibot.backtesting.backtesting_broker import BacktestingBroker

        broker = BacktestingBroker.__new__(BacktestingBroker)
        fee = TradingFee(per_contract_fee=0.65)
        order = self._make_order(side="sell_to_open", order_type="smart_limit", quantity=20)
        strategy = self._make_strategy(sell_fees=[fee])

        cost = broker.calculate_trade_cost(order, strategy, price=3.00)
        assert cost == Decimal("13.00")  # 20 * 0.65

    def test_old_flat_fee_behavior_unchanged(self):
        """Existing flat_fee behavior should NOT change (still per-order, not per-contract)."""
        from lumibot.backtesting.backtesting_broker import BacktestingBroker

        broker = BacktestingBroker.__new__(BacktestingBroker)
        fee = TradingFee(flat_fee=0.65)
        order = self._make_order(side="sell_to_open", order_type="market", quantity=40)
        strategy = self._make_strategy(sell_fees=[fee])

        cost = broker.calculate_trade_cost(order, strategy, price=1.50)
        # flat_fee is per-order, so $0.65 regardless of quantity
        assert cost == Decimal("0.65")

    def test_every_option_side_pays_its_fee(self):
        """Closing a short option (buy_to_close) is a buy and pays the buy fee.

        Found 2026-09-29 on the AI Iron Condor: the two short legs of every
        closing package were charged nothing, so option backtests looked cheaper
        than live trading.
        """
        from lumibot.backtesting.backtesting_broker import BacktestingBroker

        broker = BacktestingBroker.__new__(BacktestingBroker)
        fee = TradingFee(per_contract_fee=0.65)
        strategy = self._make_strategy(buy_fees=[fee], sell_fees=[fee])
        for side in ("buy_to_open", "buy_to_close", "sell_to_open", "sell_to_close"):
            order = self._make_order(side=side, order_type="market", quantity=10)
            assert broker.calculate_trade_cost(order, strategy, price=1.0) == Decimal("6.50"), side
