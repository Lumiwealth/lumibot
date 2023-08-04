from lumibot.entities import TradingFee


class TestTradingFee:
    def test_init(self):
        fee = TradingFee(flat_fee=5.2)
        assert fee.flat_fee == 5.2
