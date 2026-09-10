"""Read Kalshi cash, portfolio value, positions and orders without submitting."""

from lumibot.strategies import Strategy


class KalshiAccountCheck(Strategy):
    def on_trading_iteration(self):
        pass


if __name__ == "__main__":
    from lumibot.credentials import BROKER

    if BROKER is None or BROKER.name != "Kalshi":
        raise SystemExit("Set TRADING_BROKER=KALSHI and configure Kalshi credentials")
    strategy = KalshiAccountCheck(broker=BROKER, analyze_backtest=False, synchronize_broker_on_start=False)
    try:
        if not strategy.update_broker_balances():
            raise SystemExit("Kalshi balance check failed; check credentials and environment")
        print("Cash:", strategy.get_cash())
        print("Total portfolio value:", strategy.get_portfolio_value())
        print("Positions:", strategy.get_positions())
        print("Orders:", strategy.get_orders())
    finally:
        BROKER.cleanup_streams()
