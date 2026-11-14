"""The "synthetic" route for option data in routed backtests.

Options routed to ``synthetic`` get chains, bars, bid/ask and greeks computed from the
underlying's own bars (which come from that underlying's normal route) by
:mod:`lumibot.tools.synthetic_options`. Nothing here is market data: every row is labeled
synthetic, and a notice is logged once per backtest.

When the routing also names a real option route under ``option_real``, bars for which that
route has a real quote or trade override the synthetic ones (each bar keeps its own label).
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from lumibot.constants import LUMIBOT_CACHE_FOLDER
from lumibot.entities import Asset
from lumibot.tools import synthetic_options as model

logger = logging.getLogger(__name__)

PROVIDER = "synthetic"
REAL_ROUTE_KEY = "option_real"
DAILY_HISTORY_BARS = 300


def option_underlying(asset: Asset) -> Asset:
    underlying = getattr(asset, "underlying_asset", None)
    if underlying is not None:
        return underlying
    symbol = model.underlying_symbol(asset.symbol)
    kind = Asset.AssetType.INDEX if symbol in model.INDEX_SYMBOLS else Asset.AssetType.STOCK
    return Asset(symbol=symbol, asset_type=kind)


class SyntheticOptionEngine:
    """Builds and caches the model inputs of each underlying, and prices contracts from them."""

    def __init__(self, router):
        self._router = router
        self._surfaces: dict = {}
        self._chains: dict = {}
        self._notice_logged = False
        self.volatility_sources: set = set()
        self.parameter_versions: set = set()
        self.notices: list = []

    # -- underlying bars -------------------------------------------------------------
    def _frame(self, asset: Asset, quote_asset, ts_unit: str, length: int, end_dt) -> Optional[pd.DataFrame]:
        try:
            self._router._update_pandas_data(asset, quote_asset, max(2, int(length)), ts_unit, start_dt=end_dt)
        except Exception:
            logger.debug("Synthetic options: no %s bars for %s", ts_unit, getattr(asset, "symbol", asset), exc_info=True)
        key, legacy = self._router._build_dataset_keys(asset, quote_asset, ts_unit)
        data = self._router._data_store.get(key)
        if data is None:
            data = self._router._data_store.get(legacy)
            if data is not None and getattr(data, "timestep", ts_unit) != ts_unit:
                data = None
        frame = getattr(data, "df", None)
        return frame if isinstance(frame, pd.DataFrame) and not frame.empty else None

    def _note_once(self):
        if self._notice_logged:
            return
        self._notice_logged = True
        logger.warning(model.NOTICE)
        self.notices.append({"code": "synthetic_option_data", "message": model.NOTICE, "vendor": PROVIDER})

    # -- surface ---------------------------------------------------------------------
    def surface(self, option: Asset, quote_asset, ts_unit: str, length: int, end_dt) -> Optional[model.SurfaceDay]:
        self._note_once()
        underlying = option_underlying(option)
        bars = self._frame(underlying, quote_asset, ts_unit, length, end_dt)
        if bars is None:
            return None
        cache_key = (underlying, ts_unit)
        stamp = (len(bars), bars.index[0], bars.index[-1])
        cached = self._surfaces.get(cache_key)
        if cached is not None and cached[0] == stamp:
            return cached[1]
        daily = bars if ts_unit == "day" else self._frame(underlying, quote_asset, "day", DAILY_HISTORY_BARS, end_dt)
        if daily is None:
            return None
        vol_index = None
        vol_symbol = model.VOLATILITY_INDEX.get(str(option.symbol).upper()) or model.VOLATILITY_INDEX.get(underlying.symbol)
        if vol_symbol and (model.os.environ.get(model.VOL_INPUT_ENV) or "auto").strip().lower() != "realized":
            # optional input: any failure falls back to realized volatility
            frame = self._frame(Asset(vol_symbol, asset_type=Asset.AssetType.INDEX), quote_asset, "day", DAILY_HISTORY_BARS, end_dt)
            vol_index = self._daily_close(frame) if frame is not None else None
        surface = self._cached_surface(
            bars, self._daily_close(daily), symbol=str(option.symbol), underlying=underlying,
            vol_index=vol_index, ts_unit=ts_unit,
        )
        if surface is not None:
            self.volatility_sources.update(surface.volatility_source.split("+"))
            self.parameter_versions.add(surface.kind.version)
            self._surfaces[cache_key] = (stamp, surface)
        return surface

    @staticmethod
    def _daily_close(frame: pd.DataFrame) -> pd.Series:
        index = pd.DatetimeIndex(frame.index)
        index = index.tz_convert("America/New_York") if index.tz is not None else index
        return pd.Series(pd.to_numeric(frame["close"], errors="coerce").to_numpy(dtype=float), index=index.date)

    def _cached_surface(self, bars, daily_close, *, symbol, underlying, vol_index, ts_unit):
        """Compute the surface, reusing per-day cache files (local, mirrored to the remote cache)."""
        minutes = {"minute": 1.0, "hour": 60.0, "day": 390.0}.get(ts_unit, 1.0)
        fresh = model.build_surface(
            bars, daily_close, symbol=symbol, asset_type=str(getattr(underlying, "asset_type", "")),
            volatility_index_close=vol_index, bar_minutes=minutes,
        )
        if fresh is None or ts_unit == "day":
            return fresh
        frame = fresh.to_frame()
        frame["volatility_source"] = fresh.volatility_source
        days = np.array(frame.index.date)
        cache = None
        try:
            from lumibot.tools.backtest_cache import get_backtest_cache

            cache = get_backtest_cache()
        except Exception:
            cache = None
        pieces = []
        for day in sorted(set(days)):
            part = frame[days == day]
            path = Path(LUMIBOT_CACHE_FOLDER) / model.cache_relative_path(
                underlying.symbol, ts_unit, day, fresh.kind.identity
            )
            stored = None
            try:
                if not path.exists() and cache is not None:
                    cache.ensure_local_file(path)
                if path.exists():
                    stored = pd.read_parquet(path)
                    stored.index = pd.DatetimeIndex(stored.index).tz_convert(part.index.tz)
                    same = len(stored) >= len(part) and part.index.isin(stored.index).all()
                    same = same and np.allclose(stored.loc[part.index, "open"], part["open"], rtol=1e-9, atol=1e-9)
                    stored = stored.loc[part.index] if same else None
            except Exception:
                stored = None
            if stored is None:
                # Only a completed session is written: a partial day would be rewritten all day.
                if part.index[-1].time() >= datetime(2000, 1, 1, 15, 55).time():
                    try:
                        path.parent.mkdir(parents=True, exist_ok=True)
                        part.to_parquet(path)
                        if cache is not None:
                            cache.on_local_update(path)
                    except Exception:
                        logger.debug("Synthetic options: cache write skipped for %s", path, exc_info=True)
                stored = part
            pieces.append(stored)
        merged = pd.concat(pieces).sort_index()
        return model.SurfaceDay(
            index=pd.DatetimeIndex(merged.index),
            **{name: merged[name].to_numpy(dtype=float) for name in model.SurfaceDay.COLUMNS},
            bar_years=fresh.bar_years, kind=fresh.kind,
            volatility_source="+".join(sorted(set(merged["volatility_source"]))),
            rate=fresh.rate, dividend_yield=fresh.dividend_yield,
        )

    # -- contracts and chains --------------------------------------------------------
    def contract_frame(self, option: Asset, quote_asset, ts_unit: str, length: int, end_dt, real=None):
        surface = self.surface(option, quote_asset, ts_unit, length, end_dt)
        if surface is None or option.strike is None or option.expiration is None:
            return None
        is_call = str(option.right).lower().startswith("c")
        frame = model.contract_frame(surface, float(option.strike), is_call, option.expiration)
        if real is not None and not real.empty:
            frame = model.anchor_to_real_trades(
                surface, frame, float(option.strike), is_call, option.expiration, real,
                spread_on_real_trades=model.env_flag(model.SPREAD_ON_REAL_TRADES_ENV),
            )
        # bars after the expiration day do not exist
        frame = frame[np.array(frame.index.date) <= option.expiration]
        return frame if not frame.empty else None

    def chains(self, asset: Asset, quote_asset, now, max_date=None) -> Optional[dict]:
        self._note_once()
        underlying = Asset(model.underlying_symbol(asset.symbol), asset_type=asset.asset_type)
        key = (asset.symbol, now.date(), max_date)
        if key in self._chains:
            return self._chains[key]
        daily = self._frame(underlying, quote_asset, "day", 10, now)
        if daily is None:
            return None
        close = self._daily_close(daily)
        before = close[[d < now.date() for d in close.index]].dropna()
        if before.empty:
            return None
        chain = model.build_chain(asset.symbol, now.date(), float(before.iloc[-1]), max_date)
        chain["UnderlyingSymbol"] = asset.symbol
        self._chains[key] = chain
        return chain


def make_adapter(router, registry, base_class):
    """Create the routing adapter (built here to avoid a circular import with the router module)."""

    class _SyntheticOptionRoutingAdapter(base_class):
        provider_key = PROVIDER
        uses_spot_proxy = False

        def __init__(self, router):
            super().__init__(router)
            self.engine = SyntheticOptionEngine(router)
            self._registry = registry
            self._stamps: dict = {}

        @property
        def notices(self):
            return self.engine.notices

        def route_details(self, asset, quote_asset=None) -> dict:
            return {
                "priceSource": model.PRICE_SOURCE_SYNTHETIC,
                "modelVersion": model.MODEL_VERSION,
                "volatilityInput": "+".join(sorted(self.engine.volatility_sources)) or "pending",
            }

        def _real_frame(self, **kwargs) -> Optional[pd.DataFrame]:
            raw = (getattr(self._router, "_routing", {}) or {}).get(REAL_ROUTE_KEY)
            if not raw:
                return None
            try:
                spec = self._registry.resolve_provider_spec(raw)
                adapter = self._registry.adapter_for_spec(spec)
                if spec.provider == PROVIDER or not hasattr(adapter, "_fetch_df"):
                    return None
                kwargs["provider_spec"] = spec
                frame = adapter._fetch_df(**kwargs)
                return adapter._normalize_fetched_index(frame) if frame is not None and not frame.empty else None
            except Exception:
                logger.debug("Synthetic options: real option route returned nothing", exc_info=True)
                return None

        def update_pandas_data(self, *, asset, quote_asset, length, timestep, start_dt, **kwargs):
            if str(getattr(asset, "asset_type", "")).lower().split(".")[-1] != "option":
                raise ValueError("The synthetic route provides option data only; route other asset types elsewhere.")
            # A contract whose frame was built from the same underlying bars is not rebuilt.
            ts = timestep or self._router.get_timestep()
            _start, ts_unit = self._router.get_start_datetime_and_ts_unit(length, ts, start_dt=start_dt or self._router.get_datetime())
            key, _legacy = self._router._build_dataset_keys(asset, quote_asset, ts_unit)
            underlying_key, _ = self._router._build_dataset_keys(option_underlying(asset), quote_asset, ts_unit)
            held = self._router._data_store.get(underlying_key)
            stamp = None if held is None else (len(held.df), held.df.index[-1])
            end = start_dt if isinstance(start_dt, datetime) else self._router.get_datetime()
            covered = held is not None and pd.Timestamp(end) <= held.df.index[-1] + pd.Timedelta(days=1)
            if stamp is not None and covered and self._stamps.get(key) == stamp and key in self._router._data_store:
                return None
            result = super().update_pandas_data(
                asset=asset, quote_asset=quote_asset, length=length, timestep=timestep, start_dt=start_dt, **kwargs
            )
            held = self._router._data_store.get(underlying_key)
            if held is not None:
                self._stamps[key] = (len(held.df), held.df.index[-1])
            return result

        def _fetch_df(self, *, asset, quote_asset, ts_unit, start_datetime, end_dt, length, canonical_key,
                      provider_spec, require_quote_data, require_ohlc_data):
            real = self._real_frame(
                asset=asset, quote_asset=quote_asset, ts_unit=ts_unit, start_datetime=start_datetime, end_dt=end_dt,
                length=length, canonical_key=canonical_key, require_quote_data=require_quote_data,
                require_ohlc_data=require_ohlc_data,
            )
            return self.engine.contract_frame(asset, quote_asset, ts_unit, length, end_dt, real=real)

    return _SyntheticOptionRoutingAdapter(router)


def row_label(data: Any, when) -> Optional[str]:
    """Label of the bar of ``data`` that is current at ``when`` ("synthetic", "trade", ...), or None."""
    frame = getattr(data, "df", None)
    if frame is None or "price_source_code" not in getattr(frame, "columns", ()):
        return None
    index = frame.index
    stamp = pd.Timestamp(when)
    if index.tz is not None:
        stamp = stamp.tz_localize(index.tz) if stamp.tz is None else stamp.tz_convert(index.tz)
    position = int(index.searchsorted(stamp, side="right")) - 1
    if position < 0:
        return None
    code = frame["price_source_code"].iloc[position]
    return model.CODE_LABELS.get(float(code)) if pd.notna(code) else None
