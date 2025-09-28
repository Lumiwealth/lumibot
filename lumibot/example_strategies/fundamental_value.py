"""Fundamental value investing example strategy.

This strategy demonstrates how to combine external fundamental data with
Lumibot's trading lifecycle. Tickers are defined in a YAML configuration file
and refreshed automatically when the file changes. Fundamental metrics are
retrieved from Yahoo Finance via the ``yfinance`` package, an intrinsic value is
calculated using the Graham number, and simple buy/sell signals are generated
by comparing intrinsic value to the latest market price.

Usage:
    >>> from lumibot.example_strategies.fundamental_value import FundamentalValueStrategy

The default configuration file lives alongside this module under
``config/fundamental_value.yaml`` but a custom path can be supplied via the
``config_path`` parameter.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import yfinance as yf
import yaml

from lumibot.strategies.strategy import Strategy


@dataclass
class FundamentalSnapshot:
    """Container for the metrics we collect for each symbol."""

    symbol: str
    market_price: Optional[float]
    intrinsic_value: Optional[float]
    eps: Optional[float]
    pe_ratio: Optional[float]
    book_value: Optional[float]
    dividend_yield: Optional[float]
    timestamp: datetime


class FundamentalValueStrategy(Strategy):
    """Simple value strategy driven by EPS/PE and Graham intrinsic value."""

    parameters = {
        "config_path": None,  # Optional override for the YAML configuration file
        "fundamental_refresh_hours": 24,
        # ``target_allocation`` is interpreted as the desired fraction of the
        # portfolio to hold per symbol when a buy signal is active. When left to
        # ``None`` it falls back to ``1 / number_of_tickers`` dynamically.
        "target_allocation": None,
        # Optional margin of safety applied to the computed intrinsic value.
        "margin_of_safety": 0.0,
    }

    def initialize(self, config_path: Optional[str] = None) -> None:
        self.sleeptime = "1D"

        configured_path = config_path or self.parameters.get("config_path")
        if configured_path:
            self.config_path = Path(configured_path).expanduser().resolve()
        else:
            self.config_path = (
                Path(__file__).resolve().parent / "config" / "fundamental_value.yaml"
            )

        refresh_hours = float(self.parameters.get("fundamental_refresh_hours", 24))
        self._fundamental_cache_ttl = timedelta(hours=max(refresh_hours, 1))
        self._fundamental_cache: Dict[str, FundamentalSnapshot] = {}
        self._config_mtime: Optional[float] = None
        self._tracked_tickers: List[str] = []

        self.margin_of_safety = float(self.parameters.get("margin_of_safety", 0.0))

        # Load tickers immediately so that runtime errors surface early.
        self._reload_config(force=True)

    # ===== Lifecycle =====

    def on_trading_iteration(self) -> None:
        # Refresh configuration if the YAML file has changed.
        self._reload_config()
        if not self._tracked_tickers:
            self.log_message("No tickers configured; skipping iteration.")
            self.await_market_to_close()
            return

        per_symbol_allocation = self._compute_target_allocation()
        portfolio_value = self.get_portfolio_value()
        cash_available = self.get_cash()
        fundamentals_for_iteration: Dict[str, FundamentalSnapshot] = {}

        buy_signals: List[str] = []
        sell_signals: List[str] = []

        for symbol in self._tracked_tickers:
            snapshot = self._get_fundamental_snapshot(symbol)
            fundamentals_for_iteration[symbol] = snapshot

            if snapshot.market_price is None or snapshot.intrinsic_value is None:
                self.log_message(
                    f"Skipping {symbol}: missing fundamental data (price={snapshot.market_price}, intrinsic={snapshot.intrinsic_value})."
                )
                continue

            self.log_message(
                " | ".join(
                    [
                        f"{symbol}",
                        f"price={snapshot.market_price:.2f}",
                        f"intrinsic={snapshot.intrinsic_value:.2f}",
                        f"eps={snapshot.eps if snapshot.eps is not None else 'n/a'}",
                        f"pe={snapshot.pe_ratio if snapshot.pe_ratio is not None else 'n/a'}",
                        f"book={snapshot.book_value if snapshot.book_value is not None else 'n/a'}",
                    ]
                )
            )

            if snapshot.intrinsic_value < snapshot.market_price:
                buy_signals.append(symbol)
            else:
                sell_signals.append(symbol)

        # Execute buy orders first so that cash accounting is straightforward.
        for symbol in buy_signals:
            snapshot = fundamentals_for_iteration.get(symbol)
            if not snapshot or snapshot.market_price is None:
                continue

            target_value = portfolio_value * per_symbol_allocation
            current_position = self.get_position(symbol)
            current_value = 0.0
            if current_position and current_position.quantity:
                current_value = current_position.quantity * snapshot.market_price

            difference = target_value - current_value
            if difference <= 0:
                continue

            max_affordable = int(cash_available // snapshot.market_price)
            if max_affordable <= 0:
                continue

            requested_qty = max(int(difference // snapshot.market_price), 1)
            quantity = min(requested_qty, max_affordable)
            if quantity <= 0:
                continue

            buy_order = self.create_order(symbol, quantity, "buy")
            self.submit_order(buy_order)
            cash_available -= quantity * snapshot.market_price

        # Sell signals off-load entire positions to simplify state.
        for symbol in sell_signals:
            position = self.get_position(symbol)
            if position and position.quantity:
                sell_order = self.create_order(symbol, position.quantity, "sell")
                self.submit_order(sell_order)

        self.await_market_to_close()

    # ===== Helpers =====

    def _compute_target_allocation(self) -> float:
        configured = self.parameters.get("target_allocation")
        if configured is not None:
            try:
                configured_value = float(configured)
                return max(min(configured_value, 1.0), 0.0)
            except (TypeError, ValueError):
                self.log_message(
                    f"Invalid target_allocation '{configured}' supplied; defaulting to equal weighting."
                )

        if not self._tracked_tickers:
            return 0.0
        return 1.0 / len(self._tracked_tickers)

    def _reload_config(self, force: bool = False) -> None:
        try:
            mtime = self.config_path.stat().st_mtime
        except FileNotFoundError:
            message = f"Ticker configuration file not found at {self.config_path}."
            if force:
                raise FileNotFoundError(message) from None
            self.log_message(message)
            self._tracked_tickers = []
            return

        if not force and self._config_mtime is not None and mtime <= self._config_mtime:
            return

        with self.config_path.open("r", encoding="utf-8") as stream:
            try:
                config = yaml.safe_load(stream) or {}
            except yaml.YAMLError as exc:
                if force:
                    raise ValueError(f"Failed to parse YAML config at {self.config_path}: {exc}") from exc
                self.log_message(f"Failed to parse YAML config: {exc}")
                return

        tickers = self._normalize_ticker_list(config.get("tickers", []))
        if not tickers:
            message = f"No tickers found in configuration {self.config_path}."
            if force:
                raise ValueError(message)
            self.log_message(message)
            self._tracked_tickers = []
            return

        self._tracked_tickers = tickers
        self._config_mtime = mtime
        self.log_message(
            f"Tracking {len(self._tracked_tickers)} ticker(s) from {self.config_path.name}: {', '.join(self._tracked_tickers)}"
        )

    @staticmethod
    def _normalize_ticker_list(raw: Iterable) -> List[str]:
        tickers: List[str] = []
        for item in raw or []:
            symbol: Optional[str]
            if isinstance(item, dict):
                symbol = item.get("symbol") or item.get("ticker")
            else:
                symbol = str(item)

            if not symbol:
                continue
            cleaned = symbol.strip().upper()
            if cleaned:
                tickers.append(cleaned)

        return tickers

    def _get_fundamental_snapshot(self, symbol: str) -> FundamentalSnapshot:
        now = self.get_datetime()
        if now is None:
            now = datetime.now(timezone.utc)
        elif now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        cached = self._fundamental_cache.get(symbol)
        if cached and now - cached.timestamp < self._fundamental_cache_ttl:
            return cached

        ticker = yf.Ticker(symbol)
        try:
            info = ticker.get_info()
        except Exception as exc:  # pragma: no cover - defensive logging path
            self.log_message(f"Failed to download fundamentals for {symbol}: {exc}")
            info = {}

        eps = self._to_float(info.get("trailingEps")) or self._to_float(info.get("forwardEps"))
        pe_ratio = self._to_float(info.get("trailingPE")) or self._to_float(info.get("forwardPE"))
        book_value = self._to_float(info.get("bookValue"))
        dividend_yield = self._to_float(info.get("dividendYield"))

        margin_multiplier = 1.0 - max(min(self.margin_of_safety, 0.99), 0.0)
        intrinsic_value = self._calculate_intrinsic_value(eps, book_value)
        if intrinsic_value is not None:
            intrinsic_value *= margin_multiplier

        market_price = self._latest_price(symbol, info)

        snapshot = FundamentalSnapshot(
            symbol=symbol,
            market_price=market_price,
            intrinsic_value=intrinsic_value,
            eps=eps,
            pe_ratio=pe_ratio,
            book_value=book_value,
            dividend_yield=dividend_yield,
            timestamp=now,
        )

        self._fundamental_cache[symbol] = snapshot
        return snapshot

    def _latest_price(self, symbol: str, info: Dict) -> Optional[float]:
        # Prefer Lumibot's data source for pricing to maintain consistency with
        # the backtesting/live environment.
        try:
            price = self.get_last_price(symbol)
            if price is not None:
                return float(price)
        except Exception:
            pass

        for key in ("currentPrice", "regularMarketPrice", "previousClose"):
            value = self._to_float(info.get(key))
            if value is not None:
                return value

        return None

    @staticmethod
    def _calculate_intrinsic_value(eps: Optional[float], book_value: Optional[float]) -> Optional[float]:
        if eps is None or book_value is None:
            return None
        if eps <= 0 or book_value <= 0:
            return None
        try:
            return math.sqrt(22.5 * eps * book_value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_float(value: Optional[float]) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
