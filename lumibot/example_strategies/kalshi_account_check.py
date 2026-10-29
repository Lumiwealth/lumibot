"""Read Kalshi account values using the existing Strategy methods; no orders."""

from lumibot.strategies import Strategy


class KalshiAccountCheck(Strategy):
    def on_trading_iteration(self):
        pass


def main():
    from lumibot.credentials import BROKER

    if BROKER is None or BROKER.name != "Kalshi":
        raise SystemExit("Set TRADING_BROKER=kalshi and configure Kalshi credentials")
    try:
        strategy = KalshiAccountCheck(broker=BROKER, analyze_backtest=False, synchronize_broker_on_start=False)
        if not strategy.update_broker_balances():
            raise SystemExit("Kalshi account read failed; verify credentials and selected environment")
        print("Cash:", strategy.get_cash())
        print("Holdings value:", strategy.get_portfolio_value() - strategy.get_cash())
        print("Total portfolio value:", strategy.get_portfolio_value())
        print("Balance updated timestamp:", BROKER.balance_updated_ts)
    finally:
        BROKER.close()


if __name__ == "__main__":
    main()
