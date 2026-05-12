from __future__ import annotations

import datetime
import traceback
from dataclasses import dataclass
from datetime import timedelta
from importlib import import_module
from types import ModuleType
from typing import Any, Protocol, TypeAlias, cast

"""
    Description
    -----------

    This is a general component for working with the VIX. It can be used to check the
    VIX, VIX 1D, VIX RSI, VIX percentile values and more.
"""

DateInput: TypeAlias = datetime.datetime | datetime.date  # noqa: UP040


class _StrategyLike(Protocol):
    def log_message(
        self,
        message: str,
        *,
        color: str | None = None,
        broadcast: bool = False,
        **kwargs: Any,
    ) -> Any: ...

    def add_marker(self, name: str, **kwargs: Any) -> Any: ...

    def add_line(self, name: str, value: Any) -> Any: ...


class _LazyModule:
    __slots__ = ("_module_name", "_module")

    _module_name: str
    _module: ModuleType | None

    def __init__(self, module_name: str) -> None:
        object.__setattr__(self, "_module_name", module_name)
        object.__setattr__(self, "_module", None)

    def _load(self) -> ModuleType:
        module = object.__getattribute__(self, "_module")
        if module is None:
            module = import_module(object.__getattribute__(self, "_module_name"))
            object.__setattr__(self, "_module", module)
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self._load(), name, value)

    def __delattr__(self, name: str) -> None:
        if name in {"_module_name", "_module"}:
            object.__delattr__(self, name)
        else:
            delattr(self._load(), name)


pd = cast(Any, _LazyModule("pandas"))
yf = cast(Any, _LazyModule("yfinance"))
stats = cast(Any, _LazyModule("scipy.stats"))
_ta_module_cache: ModuleType | None = None


def _get_ta_module() -> Any:
    global _ta_module_cache
    if _ta_module_cache is None:
        np_module = cast(Any, import_module("numpy"))
        if not hasattr(np_module, "NaN"):
            np_module.NaN = np_module.nan
        _ta_module_cache = import_module("pandas_ta_classic")
    return cast(Any, _ta_module_cache)


@dataclass(frozen=True, slots=True)
class _VolatilityIndex:
    key: str
    symbol: str
    history_attr: str
    updated_attr: str
    display: str
    value_marker: str
    value_error_fallback: float | None
    print_traceback_on_error: bool = False


_VIX = _VolatilityIndex(
    key="vix",
    symbol="^VIX",
    history_attr="historical_vix",
    updated_attr="last_historical_vix_update",
    display="VIX",
    value_marker="vix_value",
    value_error_fallback=None,
)
_VIX_1D = _VolatilityIndex(
    key="vix_1d",
    symbol="^VIX1D",
    history_attr="historical_vix_1d",
    updated_attr="last_historical_vix_1d_update",
    display="VIX 1D",
    value_marker="vix_1d_value",
    value_error_fallback=1000,
)
_GVZ = _VolatilityIndex(
    key="gvz",
    symbol="^GVZ",
    history_attr="historical_gvz",
    updated_attr="last_historical_gvz_update",
    display="GVZ",
    value_marker="gvz_value",
    value_error_fallback=1000,
    print_traceback_on_error=True,
)


class VixHelper:
    def __init__(self, strategy: _StrategyLike) -> None:
        """
        Initialize the VIX helper with the given strategy.

        Parameters
        ----------
        strategy : Strategy
            The strategy to use for the VIX helper.

        Returns
        -------
        None
        """
        self.strategy = strategy

        self.historical_vix: Any | None = None
        self.historical_vix_1d: Any | None = None
        self.historical_gvz: Any | None = None

        self.last_historical_vix_update: datetime.datetime | None = None
        self.last_historical_vix_1d_update: datetime.datetime | None = None
        self.last_historical_gvz_update: datetime.datetime | None = None

    def _log_error(self, message: str) -> None:
        self.strategy.log_message(message, color="red", broadcast=True)

    def _get_cached_history(self, spec: _VolatilityIndex) -> Any | None:
        return cast(Any | None, getattr(self, spec.history_attr))

    def _set_cached_history(self, spec: _VolatilityIndex, history: Any) -> None:
        setattr(self, spec.history_attr, history)

    def _get_last_update(self, spec: _VolatilityIndex) -> datetime.datetime | None:
        return cast(datetime.datetime | None, getattr(self, spec.updated_attr))

    def _set_last_update(self, spec: _VolatilityIndex, dt: datetime.datetime) -> None:
        setattr(self, spec.updated_attr, dt)

    def _history_for(self, spec: _VolatilityIndex, actual_dt: datetime.datetime) -> tuple[Any, bool]:
        previous_update = self._get_last_update(spec)
        cached_history = self._get_cached_history(spec)
        if (
            cached_history is not None
            and previous_update is not None
            and previous_update.date() == actual_dt.date()
        ):
            return cached_history, False

        ticker = yf.Ticker(spec.symbol)
        history = ticker.history(period="max")
        history.index = pd.to_datetime(history.index)
        self._set_cached_history(spec, history)
        self._set_last_update(spec, actual_dt)
        return history, True

    def _normalize_lookup_dates(self, history: Any, current_dt: DateInput) -> tuple[Any, Any]:
        previous_dt = current_dt - timedelta(days=1)
        today_date = pd.Timestamp(current_dt)
        previous_date = pd.Timestamp(previous_dt)

        idx = history.index
        idx_tz = getattr(idx, "tz", None)
        today_tz = getattr(today_date, "tzinfo", None)

        if idx_tz is not None and today_tz is None:
            return today_date.tz_localize(idx_tz), previous_date.tz_localize(idx_tz)
        if idx_tz is None and today_tz is not None:
            return today_date.tz_localize(None), previous_date.tz_localize(None)
        return today_date, previous_date

    def _nearest_price(self, history: Any, current_dt: DateInput, *, use_open: bool) -> float | None:
        today_date, previous_date = self._normalize_lookup_dates(history, current_dt)
        lookup_date = today_date if use_open else previous_date
        column = "Open" if use_open else "Close"
        nearest_date = history.index.asof(lookup_date)
        value = history.loc[nearest_date][column]
        return self._to_float(value)

    def _window_values(
        self,
        history: Any,
        current_dt: DateInput,
        window: int,
        *,
        use_open: bool,
    ) -> list[float]:
        today_date, previous_date = self._normalize_lookup_dates(history, current_dt)
        lookup_date = today_date if use_open else previous_date
        column = "Open" if use_open else "Close"
        nearest_date = history.index.asof(lookup_date)
        raw_values = history.loc[nearest_date - timedelta(days=window) : nearest_date][column].tolist()
        return [float(value) for value in raw_values]

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _values(
        self,
        spec: _VolatilityIndex,
        dt: DateInput | None,
        window: int | None,
        *,
        use_open: bool = False,
    ) -> list[float] | None:
        if dt is None or window is None:
            return None

        try:
            actual_dt = datetime.datetime.now()
            history, _ = self._history_for(spec, actual_dt)
            return self._window_values(history, dt, int(window), use_open=use_open)
        except Exception as e:
            self._log_error(f"ERROR: Failed to fetch live {spec.display} values: {e}")
            return None

    def _value(
        self,
        spec: _VolatilityIndex,
        current_dt: DateInput | None,
        *,
        use_open: bool = False,
    ) -> float | None:
        if current_dt is None:
            return spec.value_error_fallback

        try:
            actual_dt = datetime.datetime.now()
            history, refreshed = self._history_for(spec, actual_dt)
            value = self._nearest_price(history, current_dt, use_open=use_open)

            if refreshed:
                self.strategy.add_marker(spec.value_marker, value=value, symbol="square", color="blue")

            return value
        except Exception as e:
            self._log_error(f"ERROR: Failed to fetch live {spec.display} value: {e}")
            if spec.print_traceback_on_error:
                traceback.print_exc()
            return spec.value_error_fallback

    def _percentile(
        self,
        spec: _VolatilityIndex,
        dt: DateInput | None,
        window: int | None,
        *,
        use_open: bool = False,
    ) -> float | None:
        if dt is None or window is None:
            return None

        values = self._values(spec, dt, window, use_open=use_open)
        if not values:
            return None

        current_value = self._value(spec, dt, use_open=use_open)
        if current_value is None:
            return None

        percentile = float(stats.percentileofscore(values, current_value))
        self.strategy.add_marker(
            f"{spec.key}_percentile",
            symbol="square",
            color="blue",
            value=percentile,
            detail_text=f"{spec.display} percentile: {percentile}",
        )
        return percentile

    def _rsi_value(
        self,
        spec: _VolatilityIndex,
        dt: DateInput | None,
        window: int,
        *,
        use_open: bool = False,
    ) -> float | None:
        if dt is None:
            return None

        download_window = int(window * 1.6)
        values = self._values(spec, dt, download_window, use_open=use_open)
        if not values:
            return None

        df = pd.DataFrame(values, columns=[spec.display])
        df["RSI"] = _get_ta_module().rsi(df[spec.display], length=window)
        return self._to_float(df["RSI"].iloc[-1])

    def _check_max_threshold(
        self,
        *,
        label: str,
        value: float | None,
        threshold: float | None,
        marker_name: str,
        line_name: str | None = None,
        log_within_limits: bool = False,
    ) -> bool:
        if value is None or threshold is None:
            return False

        if line_name is not None:
            self.strategy.add_line(line_name, value)

        if value > threshold:
            self.strategy.log_message(
                f"{label} is too high: {value} which is greater than the max of {threshold}",
                color="yellow",
                broadcast=True,
            )
            self.strategy.add_marker(
                marker_name,
                symbol="circle",
                color="blue",
                detail_text=f"{label} too high: {value}",
            )
            return True

        if log_within_limits:
            self.strategy.log_message(
                f"{label} is within the limits: {value} which is less than the max of {threshold}",
                color="green",
                broadcast=True,
            )
        return False

    def _check_min_threshold(
        self,
        *,
        label: str,
        value: float | None,
        threshold: float | None,
        marker_name: str,
    ) -> bool:
        if value is None or threshold is None:
            return False

        if value < threshold:
            self.strategy.log_message(
                f"{label} is too low: {value} which is less than the min of {threshold}",
                color="yellow",
                broadcast=True,
            )
            self.strategy.add_marker(
                marker_name,
                symbol="circle",
                color="blue",
                detail_text=f"{label} too low: {value}",
            )
            return True

        return False

    def _check_max_percentile(
        self,
        *,
        spec: _VolatilityIndex,
        dt: DateInput | None,
        threshold: float | None,
        window: int | None,
        marker_name: str,
        use_open: bool,
        log_within_limits: bool = False,
    ) -> bool:
        if threshold is None or window is None:
            return False

        percentile = self._percentile(spec, dt, window, use_open=use_open)
        if percentile is None:
            return False

        if percentile > threshold:
            self.strategy.log_message(
                f"{spec.display} is too high based on the percentile: "
                f"{percentile} which is greater than the max of {threshold}",
                color="yellow",
                broadcast=True,
            )
            self.strategy.add_marker(
                marker_name,
                symbol="circle",
                color="blue",
                detail_text=f"{spec.display} too high based on the percentile: {percentile}",
            )
            return True

        if log_within_limits:
            self.strategy.log_message(
                f"{spec.display} is within the limits based on the percentile: "
                f"{percentile} which is less than the max of {threshold}",
                color="green",
                broadcast=True,
            )
        return False

    def get_vix_values(
        self,
        dt: DateInput | None,
        window: int | None,
        use_open: bool = False,
    ) -> list[float] | None:
        return self._values(_VIX, dt, window, use_open=use_open)

    def get_vix_value(self, current_dt: DateInput | None = None, use_open: bool = False) -> float | None:
        return self._value(_VIX, current_dt, use_open=use_open)

    def get_vix_1d_value(self, current_dt: DateInput | None = None, use_open: bool = False) -> float | None:
        return self._value(_VIX_1D, current_dt, use_open=use_open)

    def check_max_gvz_percentile(
        self,
        dt: DateInput | None,
        max_gvz_percentile: float | None,
        gvz_percentile_window: int | None,
        use_open: bool = False,
    ) -> bool:
        return self._check_max_percentile(
            spec=_GVZ,
            dt=dt,
            threshold=max_gvz_percentile,
            window=gvz_percentile_window,
            marker_name="gvz_percentile_too_high",
            use_open=use_open,
        )

    def get_gvz_values(
        self,
        dt: DateInput | None,
        window: int | None,
        use_open: bool = False,
    ) -> list[float] | None:
        return self._values(_GVZ, dt, window, use_open=use_open)

    def get_gvz_rsi_value(
        self,
        dt: DateInput | None,
        window: int = 14,
        use_open: bool = False,
    ) -> float | None:
        return self._rsi_value(_GVZ, dt, window, use_open=use_open)

    def check_max_vix_1d(
        self,
        dt: DateInput | None,
        max_vix_1d: float | None,
        use_open: bool = False,
    ) -> bool:
        value = self.get_vix_1d_value(dt, use_open=use_open)
        if value is not None:
            self.strategy.add_line("vix_1d", value)
        return self._check_max_threshold(
            label="VIX 1D",
            value=value,
            threshold=max_vix_1d,
            marker_name="vix_1d_too_high",
            log_within_limits=True,
        )

    def check_min_vix_1d(
        self,
        dt: DateInput | None,
        min_vix_1d: float | None,
        use_open: bool = False,
    ) -> bool:
        return self._check_min_threshold(
            label="VIX 1D",
            value=self.get_vix_1d_value(dt, use_open=use_open),
            threshold=min_vix_1d,
            marker_name="vix_1d_too_low",
        )

    def check_max_vix_percentile(
        self,
        dt: DateInput | None,
        max_vix_percentile: float | None,
        vix_percentile_window: int | None,
        use_open: bool = False,
    ) -> bool:
        return self._check_max_percentile(
            spec=_VIX,
            dt=dt,
            threshold=max_vix_percentile,
            window=vix_percentile_window,
            marker_name="vix_percentile_too_high",
            use_open=use_open,
            log_within_limits=True,
        )

    def get_vix_percentile(
        self,
        dt: DateInput | None,
        window: int | None,
        use_open: bool = False,
    ) -> float | None:
        return self._percentile(_VIX, dt, window, use_open=use_open)

    def check_max_vix(
        self,
        dt: DateInput | None,
        max_vix: float | None,
        use_open: bool = False,
    ) -> bool:
        value = self.get_vix_value(dt, use_open=use_open)
        if value is not None:
            self.strategy.add_line("vix", value)
            self.strategy.add_marker("vix", symbol="square", color="blue", detail_text=f"VIX: {value}", value=value)
        return self._check_max_threshold(
            label="VIX",
            value=value,
            threshold=max_vix,
            marker_name="vix_too_high",
            log_within_limits=True,
        )

    def check_min_vix(
        self,
        dt: DateInput | None,
        min_vix: float | None,
        use_open: bool = False,
    ) -> bool:
        return self._check_min_threshold(
            label="VIX",
            value=self.get_vix_value(dt, use_open=use_open),
            threshold=min_vix,
            marker_name="vix_too_low",
        )

    def check_max_vix_rsi(
        self,
        dt: DateInput | None,
        max_vix_rsi: float | None,
        rsi_window: int = 14,
        use_open: bool = False,
    ) -> bool:
        if max_vix_rsi is None:
            return False

        value = self.get_vix_rsi_value(dt, rsi_window, use_open=use_open)
        if value is not None:
            self.strategy.add_line("vix_rsi", value)
        return self._check_max_threshold(
            label="VIX RSI",
            value=value,
            threshold=max_vix_rsi,
            marker_name="vix_rsi_too_high",
            log_within_limits=True,
        )

    def get_vix_rsi_value(
        self,
        dt: DateInput | None,
        window: int = 14,
        use_open: bool = False,
    ) -> float | None:
        return self._rsi_value(_VIX, dt, window, use_open=use_open)

    def get_gvz_value(self, current_dt: DateInput | None = None, use_open: bool = False) -> float | None:
        return self._value(_GVZ, current_dt, use_open=use_open)

    def get_gvz_percentile(
        self,
        dt: DateInput | None,
        window: int | None,
        use_open: bool = False,
    ) -> float | None:
        return self._percentile(_GVZ, dt, window, use_open=use_open)

    def check_max_gvz(
        self,
        dt: DateInput | None,
        max_gvz: float | None,
        use_open: bool = False,
    ) -> bool:
        value = self.get_gvz_value(dt, use_open=use_open)
        if value is not None:
            self.strategy.add_line("gvz", value)
            self.strategy.add_marker("gvz", symbol="square", color="blue", detail_text=f"GVZ: {value}", value=value)
        return self._check_max_threshold(
            label="GVZ",
            value=value,
            threshold=max_gvz,
            marker_name="gvz_too_high",
            log_within_limits=True,
        )

    def check_min_gvz(
        self,
        dt: DateInput | None,
        min_gvz: float | None,
        use_open: bool = False,
    ) -> bool:
        return self._check_min_threshold(
            label="GVZ",
            value=self.get_gvz_value(dt, use_open=use_open),
            threshold=min_gvz,
            marker_name="gvz_too_low",
        )
