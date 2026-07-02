from datetime import datetime
from decimal import Decimal

import pandas as pd

from lumibot.backtesting import BacktestingBroker, PolymarketBacktesting
from lumibot.data_sources.polymarket_data import PolymarketData
from lumibot.entities import Asset
from lumibot.entities.order import Order
from lumibot.example_strategies.polymarket_prediction_contract import PolymarketPredictionContractStrategy
from lumibot.strategies.strategy import Strategy


class FakePolymarketHistoryClient:
    def get(self, url, params=None):
        if url.endswith("/prices-history"):
            return {
                "history": [
                    {"t": 1_735_732_800, "p": "0.40"},
                    {"t": 1_735_732_860, "p": "0.42"},
                ]
            }
        if url.endswith("/markets/slug/test-market"):
            return {
                "id": "m1",
                "slug": "test-market",
                "conditionId": "0x" + "1" * 64,
                "outcomes": '["Yes","No"]',
                "clobTokenIds": '["111","222"]',
                "orderPriceMinTickSize": "0.01",
                "orderMinSize": "5",
            }
        if url.endswith("/markets/token/111"):
            return {
                "id": "m1",
                "slug": "test-market",
                "conditionId": "0x" + "1" * 64,
                "outcomes": '["Yes","No"]',
                "clobTokenIds": '["111","222"]',
                "orderPriceMinTickSize": "0.01",
                "orderMinSize": "5",
            }
        raise AssertionError(f"Unexpected URL: {url}")


class FakeResolvedPolymarketClient(FakePolymarketHistoryClient):
    def get(self, url, params=None):
        if url.endswith("/prices-history"):
            return {
                "history": [
                    {"t": 1_735_732_800, "p": "0.40"},
                    {"t": 1_735_732_860, "p": "0.42"},
                    {"t": 1_735_733_040, "p": "0.43"},
                ]
            }
        if url.endswith("/markets/slug/resolved-market") or url.endswith("/markets/token/111"):
            return {
                "id": "m2",
                "slug": "resolved-market",
                "conditionId": "0x" + "2" * 64,
                "outcomes": '["Yes","No"]',
                "clobTokenIds": '["111","222"]',
                "closed": True,
                "winner": "Yes",
                "endDate": "2025-01-01T12:02:00Z",
                "orderPriceMinTickSize": "0.01",
                "orderMinSize": "5",
            }
        return super().get(url, params=params)


class PolymarketBacktestStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"

    def on_trading_iteration(self):
        return


def _build_prediction_backtest():
    start = pd.Timestamp("2025-01-01T12:00:00Z").to_pydatetime()
    end = pd.Timestamp("2025-01-01T12:03:00Z").to_pydatetime()
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    polymarket_data = PolymarketData(client=FakePolymarketHistoryClient())
    data_source = PolymarketBacktesting(
        start,
        end,
        assets=[asset],
        polymarket_data=polymarket_data,
        show_progress_bar=False,
        market="24/7",
    )
    data_source.load_data()
    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False
    strategy = PolymarketBacktestStrategy(
        broker=broker,
        data_source=data_source,
        budget=100.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False
    return strategy, broker, asset, quote, data_source


def test_polymarket_backtesting_loads_real_history_as_close_price_bars():
    _, _, asset, _, data_source = _build_prediction_backtest()

    stored_data = next(iter(data_source._data_store.values()))

    assert list(stored_data.df["close"]) == [0.40, 0.42]
    assert stored_data.asset.asset_type == Asset.AssetType.PREDICTION_CONTRACT


def test_polymarket_backtesting_quote_and_last_price_use_loaded_bars_without_rounding():
    strategy, _, asset, _, _ = _build_prediction_backtest()

    quote = strategy.get_quote(asset)
    last_price = strategy.get_last_price(asset)

    assert quote.price == 0.40
    assert quote.bid == 0.40
    assert quote.ask == 0.40
    assert quote.raw_data["source"] == "polymarket_backtesting"
    assert last_price == 0.40


def test_polymarket_backtesting_prediction_contract_cash_accounting():
    strategy, broker, asset, quote, _ = _build_prediction_backtest()
    buy = strategy.create_order(
        asset,
        Decimal("10"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )
    sell = strategy.create_order(
        asset,
        Decimal("5"),
        Order.OrderSide.SELL,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )

    broker._execute_filled_order(buy, price=0.40, filled_quantity=Decimal("10"), strategy=strategy)
    strategy._executor.process_queue()
    broker._execute_filled_order(sell, price=0.40, filled_quantity=Decimal("5"), strategy=strategy)
    strategy._executor.process_queue()

    assert strategy.cash == 98.0


def test_polymarket_backtesting_market_and_limit_orders_fill_from_polymarket_bars():
    strategy, broker, asset, quote, _ = _build_prediction_backtest()

    market_buy = strategy.create_order(
        asset,
        Decimal("10"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )
    strategy.submit_order(market_buy)
    broker.process_pending_orders(strategy=strategy)
    strategy._executor.process_queue()

    assert market_buy.is_filled()
    assert float(market_buy.avg_fill_price) == 0.40
    assert strategy.cash == 96.0

    limit_sell = strategy.create_order(
        asset,
        Decimal("5"),
        Order.OrderSide.SELL,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.40,
        quote=quote,
    )
    strategy.submit_order(limit_sell)
    broker.process_pending_orders(strategy=strategy)
    strategy._executor.process_queue()

    assert limit_sell.is_filled()
    assert strategy.cash == 98.0


def test_polymarket_backtesting_validates_tick_size_and_min_order_size():
    strategy, broker, asset, quote, _ = _build_prediction_backtest()
    asset.polymarket_market = {
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["111", "222"],
        "orderPriceMinTickSize": "0.01",
        "orderMinSize": "5",
    }

    too_small = strategy.create_order(
        asset,
        Decimal("1"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.40,
        quote=quote,
    )
    try:
        broker.submit_order(too_small)
    except ValueError as exc:
        assert "minimum order size" in str(exc)
    else:
        raise AssertionError("Expected minimum order size validation to fail")

    bad_tick = strategy.create_order(
        asset,
        Decimal("5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.405,
        quote=quote,
    )
    try:
        broker.submit_order(bad_tick)
    except ValueError as exc:
        assert "tick size" in str(exc)
    else:
        raise AssertionError("Expected tick-size validation to fail")


def test_polymarket_backtesting_applies_market_close_and_resolution_settlement():
    start = pd.Timestamp("2025-01-01T12:00:00Z").to_pydatetime()
    end = pd.Timestamp("2025-01-01T12:05:00Z").to_pydatetime()
    polymarket_data = PolymarketData(client=FakeResolvedPolymarketClient())
    data_source = PolymarketBacktesting(
        start,
        end,
        markets=[{"slug": "resolved-market", "outcome": "Yes"}],
        polymarket_data=polymarket_data,
        show_progress_bar=False,
        market="24/7",
    )
    data_source.load_data()

    stored_data = next(iter(data_source._data_store.values()))
    assert stored_data.df.index.max() == pd.Timestamp("2025-01-01T12:02:00Z")
    assert stored_data.df["close"].iloc[-1] == 1.0


def test_polymarket_example_strategy_backtest_runs_with_mocked_history(monkeypatch):
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "none")
    start = pd.Timestamp("2025-01-01T12:00:00Z").to_pydatetime()
    end = pd.Timestamp("2025-01-01T12:03:00Z").to_pydatetime()
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)

    results = PolymarketPredictionContractStrategy.backtest(
        PolymarketBacktesting,
        start,
        end,
        assets=[asset],
        polymarket_data=PolymarketData(client=FakePolymarketHistoryClient()),
        market="24/7",
        timestep="minute",
        parameters={"token_id": "111", "backtest_trade": True},
        benchmark_asset=None,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        save_stats_file=False,
        show_progress_bar=False,
    )

    assert results is not None


def test_polymarket_backtesting_rejects_out_of_range_prediction_prices():
    start = datetime(2025, 1, 1)
    end = datetime(2025, 1, 2)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    bad_df = pd.DataFrame(
        {
            "open": [1.2],
            "high": [1.2],
            "low": [1.2],
            "close": [1.2],
            "volume": [0],
        },
        index=pd.date_range("2025-01-01", periods=1, freq="1min"),
    )

    try:
        PolymarketBacktesting._validate_price_frame(bad_df, asset)
    except ValueError as exc:
        assert "between 0 and 1" in str(exc)
    else:
        raise AssertionError("Expected out-of-range prices to fail")
