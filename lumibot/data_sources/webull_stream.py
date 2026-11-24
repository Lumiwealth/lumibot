"""Webull market trade subscriptions and bounded realtime bars."""
import threading
from uuid import uuid4

import pandas as pd

from lumibot.tools.webull_helpers import LIVE_HOST, PAPER_HOST, WebullAPIError


class WebullMarketStream:
    def __init__(self, config, *, paper):
        self._config = config
        self._paper = paper
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._subscriptions = {}
        self._client = None
        self._thread = None
        self.state = "idle"

    def start(self, symbol, category, keep_bars):
        if keep_bars < 1:
            raise ValueError("keep_bars must be positive")
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError("Webull market stream is closed")
            key = (category, symbol)
            entry = self._subscriptions.get(key)
            if entry is not None:
                entry["consumers"] += 1
                entry["keep"] = max(keep_bars, entry["keep"])
                return
            self._subscriptions[key] = {"consumers": 1, "keep": keep_bars, "bars": {}}
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, daemon=True, name="webull-market-data")
                self._thread.start()
            elif self.state in {"connected", "subscribed", "receiving"}:
                try:
                    self._subscribe(self._client, category, [symbol])
                except Exception:
                    del self._subscriptions[key]
                    raise RuntimeError("Webull rejected the market trade subscription") from None

    def bars(self, symbol, category):
        with self._lock:
            entry = self._subscriptions.get((category, symbol))
            if entry is not None and entry.get("denied"):
                raise WebullAPIError("market trade subscription", code="MARKET_DATA_NOT_SUBSCRIBED")
            if entry is None or not entry["bars"]:
                return None
            return [{**bar, "vwap": bar["vwap"] / bar["volume"]} for bar in entry["bars"].values()]

    def cancel(self, symbol, category):
        with self._lock:
            key = (category, symbol)
            entry = self._subscriptions.get(key)
            if entry is None:
                return not self._subscriptions
            if entry["consumers"] > 1:
                entry["consumers"] -= 1
                return False
            connected = self._client is not None and self.state in {"connected", "subscribed", "receiving"}
            if connected and not entry.get("denied"):
                try:
                    self._client.unsubscribe([symbol], category, ["TICK"])
                except Exception:
                    # The last consumer closes the entire connection, even if
                    # the transport was already lost during unsubscribe.
                    if len(self._subscriptions) > 1:
                        raise RuntimeError("Webull trade unsubscribe failed") from None
            del self._subscriptions[key]
            return not self._subscriptions

    def _subscribe(self, client, category, symbols):
        try:
            client.subscribe(symbols, category, ["TICK"])
        except Exception as exc:
            if getattr(exc, "get_error_code", lambda: None)() != "MARKET_DATA_NOT_SUBSCRIBED":
                raise
            if len(symbols) == 1:
                self._subscriptions[(category, symbols[0])]["denied"] = True
            else:
                # Contracts in the same category can require different exchange feeds.
                for symbol in symbols:
                    self._subscribe(client, category, [symbol])

    def _on_connect(self, client, api, session_id):
        with self._lock:
            if self._stop.is_set():
                client.disconnect()
                return
            self.state = "connected"
            grouped = {}
            for (category, symbol), entry in self._subscriptions.items():
                # An interrupted candle is incomplete.
                entry["bars"].clear()
                if not entry.get("denied"):
                    grouped.setdefault(category, []).append(symbol)
            for category, symbols in grouped.items():
                for offset in range(0, len(symbols), 20):
                    self._subscribe(client, category, symbols[offset:offset + 20])

    def _on_subscribe(self, client, api, session_id):
        with self._lock:
            if self.state == "connected":
                self.state = "subscribed"

    def _on_message(self, client, topic, tick):
        if not callable(getattr(tick, "get_volume", None)):
            return
        basic = tick.get_basic()
        price, volume = tick.get_price(), tick.get_volume()
        if price is None or volume is None or float(price) <= 0 or float(volume) <= 0:
            return
        # TickResult.get_time() contains only the display clock. The SDK's
        # common message header carries the full exchange timestamp in UTC.
        stamp = pd.to_datetime(basic.get_timestamp_as_utc(), utc=True).floor("5s")
        self._record_tick(basic.get_symbol(), stamp, float(price), float(volume))

    def _record_tick(self, symbol, stamp, price, size):
        with self._lock:
            for (_, subscribed), entry in self._subscriptions.items():
                if subscribed != symbol or entry.get("denied"):
                    continue
                bars = entry["bars"]
                if bars and stamp < next(reversed(bars)):
                    continue
                if stamp not in bars:
                    bars[stamp] = dict(datetime=stamp, open=price, high=price, low=price, close=price,
                                       volume=0, vwap=0, count=0)
                current = bars[stamp]
                current["high"] = max(current["high"], price)
                current["low"] = min(current["low"], price)
                current["close"] = price
                current["volume"] += size
                current["vwap"] += price * size
                current["count"] += 1
                if len(bars) > entry["keep"]:
                    del bars[next(iter(bars))]
                self.state = "receiving"

    def _run(self):
        from webull.core.retry.retry_policy import NO_RETRY_POLICY
        from webull.data.data_streaming_client import DataStreamingClient

        delay = 1
        while not self._stop.is_set():
            with self._lock:
                if self._stop.is_set():
                    break
                self.state = "connecting"
                client = DataStreamingClient(
                    self._config["APP_KEY"], self._config["APP_SECRET"], "us", uuid4().hex,
                    http_host=PAPER_HOST if self._paper else LIVE_HOST,
                    mqtt_host="data-api.sandbox.webull.com" if self._paper else "data-api.webull.com",
                    tls_enable=True, retry_policy=NO_RETRY_POLICY,
                )
                client.set_token_dir(self._config.get("TOKEN_DIR"))
                client.on_connect_success = self._on_connect
                client.on_subscribe_success = self._on_subscribe
                client.on_quotes_message = self._on_message
                self._client = client
            try:
                client.connect_and_loop_forever(logger_enable=False)
            except Exception:
                # The SDK exception can contain a signed request.
                pass
            finally:
                with self._lock:
                    self._client = None
                    self.state = "recovering" if not self._stop.is_set() else "stopped"
            self._stop.wait(delay)
            delay = min(delay * 2, 30)

    def close(self):
        self._stop.set()
        with self._lock:
            if self._client is not None:
                self._client.disconnect()
        if self._thread is not None:
            self._thread.join(timeout=3)
        self.state = "stopped"
