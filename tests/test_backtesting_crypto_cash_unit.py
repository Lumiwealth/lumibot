import pandas as pd
import pytest
from datetime import timedelta, date
from decimal import Decimal

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, TradingFee
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy


DEFAULT_START = "2025-01-13 00:00"
DEFAULT_FREQ = "1min"


class _DummyStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"
        self.include_cash_positions = True

    def on_trading_iteration(self):
        return


def _make_ohlcv(
    bars,
    start: str = DEFAULT_START,
    freq: str = DEFAULT_FREQ,
    tz: str = "America/New_York",
    volume: int = 1_000,
):
    index = pd.date_range(start, periods=len(bars), freq=freq, tz=tz)
    opens, highs, lows, closes, volumes = [], [], [], [], []

    for bar in bars:
        if isinstance(bar, dict):
            open_ = bar["open"]
            high = bar.get("high", open_)
            low = bar.get("low", open_)
            close = bar.get("close", open_)
            vol = bar.get("volume", volume)
        elif isinstance(bar, (tuple, list)) and len(bar) == 4:
            open_, high, low, close = bar
            vol = volume
        else:
            open_ = high = low = close = bar
            vol = volume

        opens.append(open_)
        highs.append(high)
        lows.append(low)
        closes.append(close)
        volumes.append(vol)

    return pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
        },
        index=index,
    )


def _build_data_source(asset: Asset, quote: Asset | None, df: pd.DataFrame) -> PandasDataBacktesting:
    dt_start = df.index[0]
    dt_end = df.index[-1] + pd.Timedelta(minutes=1)

    df_local = df.copy()
    if df_local.index.tz is not None:
        df_local = df_local.tz_convert("America/New_York").tz_localize(None)

    data = Data(
        asset=asset,
        df=df_local,
        quote=quote,
        timestep="minute",
        timezone="America/New_York",
    )

    if quote is None:
        pandas_data_key = asset
    else:
        pandas_data_key = (asset, quote)

    pandas_data = {pandas_data_key: data}
    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=dt_start,
        datetime_end=dt_end,
        show_progress_bar=False,
        market="24/7",
        auto_adjust=True,
    )
    data_source.load_data()
    return data_source


def _build_strategy(
    broker,
    *,
    budget: float = 100_000.0,
    buy_fee: TradingFee | None = None,
    sell_fee: TradingFee | None = None,
):
    buy_fees = [buy_fee] if buy_fee else []
    sell_fees = [sell_fee] if sell_fee else []
    strategy = _DummyStrategy(
        broker=broker,
        budget=budget,
        buy_trading_fees=buy_fees,
        sell_trading_fees=sell_fees,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False
    return strategy


def _setup_strategy(
    *,
    asset: Asset,
    quote: Asset | None,
    bars,
    budget: float = 100_000.0,
    buy_fee: TradingFee | None = None,
    sell_fee: TradingFee | None = None,
):
    df = _make_ohlcv(bars)
    data_source = _build_data_source(asset, quote, df)
    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _build_strategy(
        broker,
        budget=budget,
        buy_fee=buy_fee,
        sell_fee=sell_fee,
    )
    return strategy, broker


def _submit_and_fill(strategy, broker, order):
    strategy.submit_order(order)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()


def test_crypto_forex_buy_updates_cash_once():
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    strategy, broker = _setup_strategy(
        asset=base,
        quote=quote,
        bars=[(20_000.0, 20_050.0, 19_900.0, 20_010.0)],
    )

    order = strategy.create_order(
        base,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )

    _submit_and_fill(strategy, broker, order)

    expected_cash = 100_000.0 - (0.5 * 20_000.0)
    assert strategy.cash == pytest.approx(expected_cash, rel=1e-9)
    quote_position = broker.get_tracked_position(strategy.name, quote)
    assert quote_position is not None
    assert float(quote_position.quantity) == pytest.approx(strategy.cash, rel=1e-9)
    base_position = broker.get_tracked_position(strategy.name, base)
    assert base_position is not None and base_position.quantity == pytest.approx(Decimal("0.5"))


def test_crypto_forex_round_trip_restores_cash_on_sell():
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    strategy, broker = _setup_strategy(
        asset=base,
        quote=quote,
        bars=[
            (20_000.0, 20_050.0, 19_900.0, 20_010.0),
            (20_100.0, 20_200.0, 20_000.0, 20_150.0),
        ],
    )

    buy = strategy.create_order(
        base,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )
    _submit_and_fill(strategy, broker, buy)

    broker._update_datetime(broker.datetime + timedelta(minutes=1))

    sell = strategy.create_order(
        base,
        Decimal("0.5"),
        Order.OrderSide.SELL,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )
    _submit_and_fill(strategy, broker, sell)

    expected_cash = 100_000.0 - (0.5 * 20_000.0) + (0.5 * 20_100.0)
    assert strategy.cash == pytest.approx(expected_cash, rel=1e-9)
    base_position = broker.get_tracked_position(strategy.name, base)
    assert base_position is None


def test_crypto_crypto_pair_uses_quote_position_not_cash():
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)
    strategy, broker = _setup_strategy(
        asset=base,
        quote=quote,
        bars=[(20_000.0, 20_050.0, 19_900.0, 20_010.0)],
    )

    order = strategy.create_order(
        base,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )
    _submit_and_fill(strategy, broker, order)

    # Cash remains at the initial budget, while the quote asset captures the debit.
    assert strategy.cash == pytest.approx(100_000.0, rel=1e-9)
    quote_position = broker.get_tracked_position(strategy.name, quote)
    assert quote_position is not None
    assert float(quote_position.quantity) == pytest.approx(-10_000.0)


def test_filled_order_payload_contains_quantity_and_trade_cost():
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    strategy, broker = _setup_strategy(
        asset=base,
        quote=quote,
        bars=[(20_000.0, 20_050.0, 19_900.0, 20_010.0)],
    )

    captured = {}

    original_handler = strategy._executor._on_filled_order

    def _capture_filled(**payload):
        captured["payload"] = payload
        return original_handler(**payload)

    strategy._executor._on_filled_order = _capture_filled

    order = strategy.create_order(
        base,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )
    _submit_and_fill(strategy, broker, order)

    payload = captured["payload"]
    assert payload["order"] is order
    assert payload["quantity"] == Decimal("0.5")


def test_percent_fee_applied_once_to_cash_and_order():
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    fee = TradingFee(percent_fee=Decimal("0.0025"), taker=True)
    strategy, broker = _setup_strategy(
        asset=base,
        quote=quote,
        bars=[(20_000.0, 20_050.0, 19_900.0, 20_010.0)],
        buy_fee=fee,
    )

    order = strategy.create_order(
        base,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )
    _submit_and_fill(strategy, broker, order)

    trade_amount = 0.5 * 20_000.0
    expected_fee = trade_amount * 0.0025
    expected_cash = 100_000.0 - trade_amount - expected_fee

    assert strategy.cash == pytest.approx(expected_cash, rel=1e-9)
    assert order.trade_cost == pytest.approx(expected_fee, rel=1e-9)


def test_multiple_orders_same_cycle_keep_cash_consistent():
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    strategy, broker = _setup_strategy(
        asset=base,
        quote=quote,
        bars=[(20_000.0, 20_050.0, 19_900.0, 20_010.0)],
    )

    orders = [
        strategy.create_order(
            base,
            Decimal("0.3"),
            Order.OrderSide.BUY,
            order_type=Order.OrderType.MARKET,
            quote=quote,
        ),
        strategy.create_order(
            base,
            Decimal("0.2"),
            Order.OrderSide.BUY,
            order_type=Order.OrderType.MARKET,
            quote=quote,
        ),
    ]

    for order in orders:
        _submit_and_fill(strategy, broker, order)

    expected_cash = 100_000.0 - (0.5 * 20_000.0)
    assert strategy.cash == pytest.approx(expected_cash, rel=1e-9)


def test_option_round_trip_applies_multiplier():
    option_asset = Asset(
        "SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2025, 4, 17),
        strike=568.0,
        right=Asset.OptionRight.CALL,
    )

    bars = [
        (9.75, 9.90, 9.50, 9.80),
        (2.09, 2.20, 2.00, 2.10),
    ]

    strategy, broker = _setup_strategy(
        asset=option_asset,
        quote=None,
        bars=bars,
    )

    quantity = Decimal("6")

    buy_order = strategy.create_order(
        option_asset,
        quantity,
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
    )
    _submit_and_fill(strategy, broker, buy_order)

    buy_price = bars[0][0]
    expected_cash_after_buy = 100_000.0 - (float(quantity) * buy_price * option_asset.multiplier)
    assert strategy.cash == pytest.approx(expected_cash_after_buy, rel=1e-9)

    option_position = broker.get_tracked_position(strategy.name, option_asset)
    assert option_position is not None
    assert option_position.quantity == pytest.approx(quantity)

    broker._update_datetime(broker.datetime + timedelta(minutes=1))

    sell_order = strategy.create_order(
        option_asset,
        quantity,
        Order.OrderSide.SELL,
        order_type=Order.OrderType.MARKET,
    )
    _submit_and_fill(strategy, broker, sell_order)

    sell_price = bars[1][0]
    expected_cash_final = 100_000.0 - (float(quantity) * buy_price * option_asset.multiplier)
    expected_cash_final += float(quantity) * sell_price * option_asset.multiplier
    assert strategy.cash == pytest.approx(expected_cash_final, rel=1e-9)

    final_position = broker.get_tracked_position(strategy.name, option_asset)
    assert final_position is None or final_position.quantity == 0
