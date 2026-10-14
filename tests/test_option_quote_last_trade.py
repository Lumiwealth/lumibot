from datetime import datetime, timezone
from lumibot.entities import Asset, Quote


def test_quote_keeps_last_trade_distinct_from_midpoint_and_source_time():
    quote = Quote(asset=Asset("SPY",asset_type="stock"),price=4.2,bid=4,ask=4.4,last_price=4.1,trade_time="2026-10-13T19:58:00Z")
    assert quote.mid_price == 4.2
    assert quote.last_price == 4.1
    assert quote.trade_time == datetime(2026,10,13,19,58,tzinfo=timezone.utc)


def test_quote_does_not_infer_last_trade_from_a_midpoint():
    quote = Quote(asset=Asset("SPY",asset_type="stock"),price=4.2,bid=4,ask=4.4)
    assert quote.last_price is None
    assert quote.trade_time is None


from types import SimpleNamespace
from datetime import date
import pandas as pd
from lumibot.data_sources import TradierData, SchwabData


def test_tradier_uses_latest_side_time_and_keeps_actual_last_trade():
    source = TradierData.__new__(TradierData)
    source.tradier = SimpleNamespace(market=SimpleNamespace(get_quotes=lambda symbols: pd.DataFrame([
        {"bid": 2.0, "ask": 2.4, "last": 2.1, "bid_date": 1791918000000,
         "ask_date": 1791918060000, "trade_date": 1791917940000}
    ])))
    quote = source.get_quote(Asset("NVDA", asset_type="option", expiration=date(2026,11,20),right="PUT",strike=220))
    assert quote.last_price == 2.1
    assert quote.timestamp == datetime.fromtimestamp(1791918060, tz=timezone.utc)
    assert quote.trade_time == datetime.fromtimestamp(1791917940, tz=timezone.utc)


def test_schwab_unknown_quote_time_is_not_retrieval_time():
    source = SchwabData.__new__(SchwabData)
    source.client = SimpleNamespace(get_quotes=lambda symbols: SimpleNamespace(status_code=200, json=lambda: {
        symbols[0]: {"quote": {"bidPrice": 2.0, "askPrice": 2.4, "lastPrice": 2.1, "tradeTime": 1791917940000}}
    }))
    quote = source.get_quote(Asset("NVDA"))
    assert quote.last_price == 2.1
    assert quote.timestamp is None
    assert quote.trade_time == datetime.fromtimestamp(1791917940, tz=timezone.utc)
