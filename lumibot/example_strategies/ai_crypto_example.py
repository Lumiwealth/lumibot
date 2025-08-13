from datetime import datetime

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy


class AICryptoAgent(Strategy):
    parameters = {
        "ai_allow_trading": True,  # enable trading by default for this example
        "base_symbol": "BTC",
        "quote_symbol": "USDT",
        "ai_log_enabled": True,
    }

    def initialize(self):
        # 24/7 market for crypto
        self.set_market("24/7")
        # Run main loop every minute
        self.sleeptime = "1M"
        # Enable AI logs by default for visibility
        self.ai_log_enabled = self.parameters.get("ai_log_enabled", True)

        # Create an AI agent that can trade crypto pairs the strategy manages
        self.crypto_agent = self.agents.create(
            name="crypto_agent",
            prompt=(
                "You are a conservative crypto trading agent. You only trade widely-listed pairs. "
                "Prefer small sizes and avoid overtrading. If uncertain, return no actions."
            ),
            cadence="1m",  # once per minute
            allow_trading=self.parameters["ai_allow_trading"],
        )

    # Optional observability hooks
    def on_ai_decision(self, agent_name, decision):
        # Example: ensure actions specify crypto base/quote if missing
        actions = decision.get("actions", [])
        for a in actions:
            if a.get("type") == "trade":
                a.setdefault("asset_type", Asset.AssetType.CRYPTO)
                # default quote to USDT if not provided
                a.setdefault("quote", {"symbol": self.parameters.get("quote_symbol", "USDT"), "asset_type": Asset.AssetType.CRYPTO})
        return {"actions": actions} if actions else None

    def on_ai_executed(self, agent_name, decision, execution_result):
        self.log_message(f"[AI:{agent_name}] Executed {execution_result}", color="green")

    def on_ai_error(self, agent_name, error, context):
        self.log_message(f"[AI:{agent_name}] ERROR: {error}", color="red")

    def on_trading_iteration(self):
        pv = self.get_portfolio_value()
        cash = self.get_cash()
        self.log_message(f"PV=${pv:,.2f}  Cash=${cash:,.2f}", color="blue")

    # Per-tick AI lifecycle hook for visibility into state/decisions
    def on_ai_tick(self, agent_name: str, context: dict):
        phase = context.get("phase")
        if phase == "before":
            snap = context.get("snapshot", {})
            self.log_message(f"[AI:{agent_name}] before: cash={snap.get('cash')} positions={len(snap.get('positions', []))} open_orders={len(snap.get('open_orders', []))}")
        elif phase == "after":
            decision = context.get("decision", {})
            actions = decision.get("actions", []) if isinstance(decision, dict) else []
            self.log_message(f"[AI:{agent_name}] after: actions={len(actions)} notes={decision.get('notes', '')}")


if __name__ == "__main__":
    # Live by default; let credentials/env pick the broker and data source
    # Example env keys for Alpaca Crypto (paper):
    #   ALPACA_TEST_API_KEY=...
    #   ALPACA_TEST_API_SECRET=...
    #   ALPACA_TEST_IS_PAPER=True
    # Or set TRADING_BROKER=alpaca and DATA_SOURCE=alpaca accordingly.
    strategy = AICryptoAgent(broker=None)
    strategy.run_live()
