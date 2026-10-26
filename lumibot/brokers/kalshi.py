"""Kalshi account connection through the existing LumiBot Broker interface."""

from decimal import Decimal
from threading import RLock
from time import monotonic

from lumibot.brokers.broker import Broker
from lumibot.data_sources.kalshi_data import KalshiData
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient, decimal_value


def _parse_balances(row):
    """Parse cash, holdings, total; portfolio_value already includes cash.

    Kalshi's October 7, 2025 changelog defines total value explicitly. Cash can
    have sub-cent precision while the legacy total field has cent precision.
    The derived holdings value therefore inherits the total's precision limit.
    """
    if not isinstance(row, dict):
        raise KalshiAPIError("Kalshi returned an invalid balance response")
    try:
        cash = (
            decimal_value(row["balance_dollars"], "balance")
            if row.get("balance_dollars") is not None
            else decimal_value(row["balance"], "balance") / Decimal(100)
        )
        total = decimal_value(row["portfolio_value"], "portfolio_value") / Decimal(100)
    except (KeyError, TypeError, ValueError):
        raise KalshiAPIError("Kalshi balance response is missing valid cash or portfolio value") from None
    return cash, total - cash, total


class Kalshi(Broker):
    """Account-read foundation. Trading and position normalization are unfinished.

    Constructing the adapter does not verify authentication or start workers.
    The normal balance hook performs the signed account read. A caller-owned
    compatible client may own authentication instead of direct API credentials.
    """

    NAME = "Kalshi"

    def __init__(self, config=None, data_source=None, *, client=None, connect_stream=False, max_workers=4):
        config = config or {}
        configured = config.get("CLIENT", config.get("KALSHI_CLIENT"))
        self._client = client if client is not None else configured
        self._owns_client = self._client is None
        if self._owns_client:
            self._client = KalshiClient(config, require_auth=True)
        # Omission preserves a restricted key's own subaccount binding. An
        # explicitly selected zero remains zero; do not substitute a default.
        self._account_params = {}
        for key, field in (("SUBACCOUNT", "subaccount"), ("EXCHANGE_INDEX", "exchange_index")):
            value = config.get(key, config.get("KALSHI_" + key))
            if value is not None:
                self._account_params[field] = value
        self._balance_lock = RLock()
        self._balance_snapshot = None
        self._balance_read_at = None
        self.balance_updated_ts = None
        self._closed = False
        self._owns_data_source = data_source is None
        source = data_source if data_source is not None else KalshiData(client=self._client)
        super().__init__(
            name=self.NAME,
            data_source=source,
            config={"MARKET": "24/7"},
            connect_stream=connect_stream,
            start_orders_thread=False,
            max_workers=max_workers,
        )

    def _get_balances_at_broker(self, quote_asset, strategy):
        if quote_asset is not None and (quote_asset.symbol != "USD" or quote_asset.asset_type != "forex"):
            raise ValueError("Kalshi account balances are denominated in USD")
        # The balance endpoint has a separate 1/s Basic/Advanced bucket. Share
        # consecutive cash/equity reads; failures never populate this cache.
        with self._balance_lock:
            if self._balance_read_at is None or monotonic() - self._balance_read_at >= 1:
                row = self._client.request("GET", "/portfolio/balance", params=dict(self._account_params))
                parsed = _parse_balances(row)
                self.balance_updated_ts = row.get("updated_ts")
                self._balance_snapshot = tuple(float(value) for value in parsed)
                self._balance_read_at = monotonic()
            return self._balance_snapshot

    def close(self):
        if self._closed:
            return
        self.cleanup_streams()
        if self._owns_data_source:
            self.data_source.shutdown()
        if self._owns_client:
            self._client.close()
        self._closed = True

    def _get_stream_object(self):
        return None

    def _register_stream_events(self):
        raise NotImplementedError("Kalshi order streaming is not implemented yet")

    def _run_stream(self):
        raise NotImplementedError("Kalshi order streaming is not implemented yet")

    def _submit_order(self, order):
        raise NotImplementedError("Kalshi order execution is not implemented yet")

    def cancel_order(self, order):
        raise NotImplementedError("Kalshi order execution is not implemented yet")

    def _modify_order(self, order, limit_price=None, stop_price=None):
        raise NotImplementedError("Kalshi order execution is not implemented yet")

    def _pull_positions(self, strategy):
        raise NotImplementedError("Kalshi outcome-aware positions are not implemented yet")

    def _pull_position(self, strategy, asset):
        raise NotImplementedError("Kalshi outcome-aware positions are not implemented yet")

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        raise NotImplementedError("Kalshi order normalization is not implemented yet")

    def _pull_broker_order(self, identifier):
        raise NotImplementedError("Kalshi order reads are not implemented yet")

    def _pull_broker_all_orders(self):
        raise NotImplementedError("Kalshi order reads are not implemented yet")

    def get_historical_account_value(self):
        raise NotImplementedError("Kalshi historical account values are not implemented yet")
