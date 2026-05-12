from __future__ import annotations

from collections.abc import MutableMapping
from datetime import datetime
from typing import Any, ClassVar

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.tools.helpers import ComparaisonMixin


class Bar(ComparaisonMixin):
    """
    The Bar class represents a single bar (OHLC) of data.

    Attributes
    ----------
    timestamp : int
        The Unix timestamp of the bar.
    open : float
        The opening price of the bar.
    high : float
        The high price of the bar.
    low : float
        The low price of the bar.
    close : float
        The closing price of the bar.
    volume : float
        The volume of the bar.
    dividend : float
        The dividend amount of the bar.
    stock_splits : float
        The stock splits amount of the bar.
    """

    COMPARAISON_PROP: ClassVar[str] = "timestamp"
    DEFAULT_TIMEZONE: ClassVar[Any] = LUMIBOT_DEFAULT_TIMEZONE
    DEFAULT_PYTZ: ClassVar[Any] = LUMIBOT_DEFAULT_PYTZ

    _raw: MutableMapping[str, Any]
    _timestamp: int
    _open: float
    _high: float
    _low: float
    _close: float
    _volume: float
    _dividend: float
    _stock_splits: float

    def __init__(self, raw: MutableMapping[str, Any]) -> None:
        self._raw = raw
        self.update(raw)

    @classmethod
    def get_empty_bar(cls) -> Bar:
        """Return an empty bar object."""
        item: dict[str, int] = {
            "timestamp": 0,
            "open": 0,
            "high": 0,
            "low": 0,
            "close": 0,
            "volume": 0,
            "dividend": 0,
            "stock_splits": 0,
        }
        return cls(item)

    @property
    def raw(self) -> MutableMapping[str, Any]:
        return self._raw

    @property
    def timestamp(self) -> int:
        """Return the Unix timestamp of the bar."""
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value: Any) -> None:
        timestamp = self._coerce_int(value, "Timestamp property must be convertible to integer")
        self._raw["timestamp"] = timestamp
        self._timestamp = timestamp

    @property
    def datetime(self) -> datetime:
        result = datetime.fromtimestamp(self._timestamp)
        return self.DEFAULT_PYTZ.localize(result, is_dst=None)

    @datetime.setter
    def datetime(self, value: Any) -> None:
        if not isinstance(value, datetime):
            raise ValueError("Datetime property must be a datetime object.")

        if self.datetime.tzinfo != value.tzinfo:
            raise ValueError(f"Datetime must be localized in {self.DEFAULT_TIMEZONE!r}")

        timestamp = int(value.timestamp())
        self._raw["timestamp"] = timestamp
        self._timestamp = timestamp

    @property
    def open(self) -> float:  # noqa: A003 - OHLC data exposes an `open` field.
        return self._open

    @open.setter  # noqa: A003 - OHLC data exposes an `open` field.
    def open(self, value: Any) -> None:  # noqa: A003 - OHLC data exposes an `open` field.
        self._open = self._coerce_raw_float("open", value, "Open property must be convertible to float")

    @property
    def high(self) -> float:
        return self._high

    @high.setter
    def high(self, value: Any) -> None:
        self._high = self._coerce_raw_float("high", value, "High property must be convertible to float")

    @property
    def low(self) -> float:
        return self._low

    @low.setter
    def low(self, value: Any) -> None:
        self._low = self._coerce_raw_float("low", value, "Low property must be convertible to float")

    @property
    def close(self) -> float:
        return self._close

    @close.setter
    def close(self, value: Any) -> None:
        self._close = self._coerce_raw_float("close", value, "Close property must be convertible to float")

    @property
    def volume(self) -> float:
        return self._volume

    @volume.setter
    def volume(self, value: Any) -> None:
        self._volume = self._coerce_raw_float("volume", value, "Volume property must be convertible to float")

    @property
    def dividend(self) -> float:
        return self._dividend

    @dividend.setter
    def dividend(self, value: Any) -> None:
        self._dividend = self._coerce_raw_float(
            "dividend",
            value,
            "Dividend property must be convertible to float",
        )

    @property
    def stock_splits(self) -> float:
        return self._stock_splits

    @stock_splits.setter
    def stock_splits(self, value: Any) -> None:
        self._stock_splits = self._coerce_raw_float(
            "stock_splits",
            value,
            "Stock_splits property must be convertible to float",
        )

    def update(self, data: MutableMapping[str, Any]) -> None:
        self._timestamp = self._parse_int(data, "timestamp", required=True)
        self._open = self._parse_float(data, "open", required=True)
        self._high = self._parse_float(data, "high", required=True)
        self._low = self._parse_float(data, "low", required=True)
        self._close = self._parse_float(data, "close", required=True)
        self._volume = self._parse_float(data, "volume", required=True)
        self._dividend = self._parse_float(data, "dividend", default=0.0)
        self._stock_splits = self._parse_float(data, "stock_splits", default=0.0)

    def _coerce_raw_float(self, key: str, value: Any, error_message: str) -> float:
        coerced = self._coerce_float(value, error_message)
        self._raw[key] = coerced
        return coerced

    @staticmethod
    def _coerce_float(value: Any, error_message: str) -> float:
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(error_message) from exc

    @staticmethod
    def _coerce_int(value: Any, error_message: str) -> int:
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(error_message) from exc

    def _parse_int(self, data: MutableMapping[str, Any], key: str, *, required: bool = False, default: Any = None) -> int:
        value = self._value_for_key(data, key, required=required, default=default)
        return self._coerce_int(value, f"{key} type does not fit to {int!r} type")

    def _parse_float(
        self,
        data: MutableMapping[str, Any],
        key: str,
        *,
        required: bool = False,
        default: Any = None,
    ) -> float:
        value = self._value_for_key(data, key, required=required, default=default)
        return self._coerce_float(value, f"{key} type does not fit to {float!r} type")

    @staticmethod
    def _value_for_key(
        data: MutableMapping[str, Any],
        key: str,
        *,
        required: bool,
        default: Any,
    ) -> Any:
        if key not in data:
            if required:
                raise ValueError(f"{key} key is a required field for Bar objects")
            return default
        return data.get(key)
