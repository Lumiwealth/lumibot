from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, TradingFee
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class _CryptoRebalanceStrategy(Strategy):
    def initialize(self, parameters=None):
        params = parameters or getattr(self, "parameters", {}) or {}
        self.assets = params["assets"]
        self.quote = params["quote"]
        self.traded = False
        self.include_cash_positions = True
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        if not self.traded:
            for asset in self.assets:
                order = self.create_order(
                    asset,
                    Decimal("0.5"),
                    Order.OrderSide.BUY,
                    order_type=Order.OrderType.MARKET,
                    quote=self.quote,
                )
                self.submit_order(order)
            self.traded = True
        else:
            return


def _build_crypto_backtester(price_map, timestep="day"):
    assets = [Asset(symbol, asset_type=Asset.AssetType.CRYPTO) for symbol in price_map]
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    pandas_data = {}
    for asset in assets:
        prices = price_map[asset.symbol]
        pandas_freq = "D" if timestep == "day" else timestep
        index = pd.date_range("2025-01-13", periods=len(prices), freq=pandas_freq, tz="America/New_York")
        df = pd.DataFrame(
            {
                "open": prices,
                "high": [p * 1.01 for p in prices],
                "low": [p * 0.99 for p in prices],
                "close": prices,
                "volume": [1_000] * len(prices),
            },
            index=index,
        )
        local_df = df.tz_convert("America/New_York").tz_localize(None)
        pandas_data[(asset, quote)] = Data(
            asset=asset,
            df=local_df,
            quote=quote,
            timestep=timestep,
            timezone="America/New_York",
        )

    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=list(pandas_data.values())[0].datetime_start,
        datetime_end=list(pandas_data.values())[0].datetime_end,
        show_progress_bar=False,
        market="24/7",
        auto_adjust=True,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())

    return assets, quote, broker


@pytest.mark.parametrize(
    "price_map",
    [
        {"BTC": [20_000, 20_500, 21_000], "ETH": [3_000, 3_150, 3_250]},
        {"BTC": [30_000, 29_800, 30_400], "ETH": [4_000, 4_050, 4_100]},
    ],
)
def test_crypto_cash_regression_no_fees(price_map):
    assets, quote, broker = _build_crypto_backtester(price_map)
    strategy = _CryptoRebalanceStrategy(
        broker=broker,
        budget=100_000.0,
        analyze_backtest=False,
        parameters={"assets": assets, "quote": quote},
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)

    stats = strategy.stats
    assert not stats.empty

    # Cash should never spike above the starting balance when booking fills.
    assert stats["cash"].max() == pytest.approx(100_000.0, rel=1e-9, abs=1e-6)

    # Portfolio value should equal cash + market value of open positions at the end.
    final_row = stats.iloc[-1]
    manual_value = float(strategy.cash)
    for position in strategy.positions:
        # Skip USD positions as they represent cash and are already included in strategy.cash
        if position.asset.symbol == 'USD':
            continue
        final_price = price_map[position.asset.symbol][-1]
        manual_value += float(position.quantity) * final_price
    assert final_row["portfolio_value"] == pytest.approx(manual_value, rel=1e-6)

    # Only two fills should have occurred (one per asset).
    fills = strategy.broker._trade_event_log_df
    # Filter for actual fills (not new orders)
    actual_fills = fills[fills['status'] == 'fill']
    assert len(actual_fills) == 2


def test_crypto_cash_regression_with_fees():
    price_map = {"BTC": [20_000, 20_300, 20_800], "ETH": [3_000, 3_200, 3_350]}
    assets, quote, broker = _build_crypto_backtester(price_map)
    fee = TradingFee(percent_fee=Decimal("0.0025"), taker=True)
    strategy = _CryptoRebalanceStrategy(
        broker=broker,
        budget=100_000.0,
        analyze_backtest=False,
        parameters={"assets": assets, "quote": quote},
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)

    fills = strategy.broker._trade_event_log_df
    # Filter for actual fills (not new orders)
    actual_fills = fills[fills['status'] == 'fill']
    assert len(actual_fills) == 2

    total_notional = sum(float(row["filled_quantity"]) * float(row["price"]) for _, row in actual_fills.iterrows())
    fee_total = total_notional * 0.0025

    expected_cash = 100_000.0 - total_notional - fee_total
    assert strategy.cash == pytest.approx(expected_cash, rel=1e-6)

    # Ensure the final portfolio value equals manual valuation using recorded fills.
    manual_value = float(strategy.cash)
    for position in strategy.positions:
        # Skip USD positions as they represent cash and are already included in strategy.cash
        if position.asset.symbol == 'USD':
            continue
        final_price = price_map[position.asset.symbol][-1]
        manual_value += float(position.quantity) * final_price

    final_value = strategy.stats["portfolio_value"].iloc[-1]
    assert final_value == pytest.approx(manual_value, rel=1e-6)

