import pandas as pd

from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.entities import Asset


def test_get_bars_preserves_crypto_tuple_quote(monkeypatch):
    captured = {}

    class FakeCryptoBarsRequest:
        def __init__(self, **kwargs):
            captured["symbols"] = kwargs["symbol_or_symbols"]

    class FakeClient:
        def get_crypto_bars(self, _params):
            return type("BarsResponse", (), {"df": pd.DataFrame()})()

    class FakeTimeFrame:
        Minute = "1Min"
        Hour = "1Hour"
        Day = "1Day"

        def __init__(self, amount, unit):
            self.amount = amount
            self.unit = unit

    class FakeTimeFrameUnit:
        Minute = "Minute"
        Hour = "Hour"
        Day = "Day"

    class FakeAdjustment:
        ALL = "all"
        RAW = "raw"

    def fake_get_alpaca_attr(name):
        if name == "CryptoBarsRequest":
            return FakeCryptoBarsRequest
        if name == "TimeFrame":
            return FakeTimeFrame
        if name == "TimeFrameUnit":
            return FakeTimeFrameUnit
        if name == "Adjustment":
            return FakeAdjustment
        raise AssertionError(name)

    monkeypatch.setattr("lumibot.data_sources.alpaca_data._get_alpaca_attr", fake_get_alpaca_attr)

    data_source = AlpacaData({"API_KEY": "key", "API_SECRET": "secret"})
    data_source._get_crypto_client = lambda: FakeClient()

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    data_source.get_bars([(base, quote)], length=1, timestep="minute")

    assert captured["symbols"] == ["BTC/USDT"]
