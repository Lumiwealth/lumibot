from datetime import datetime

from lumibot import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.tools import ComparaisonMixin


class Bar(ComparaisonMixin):
    COMPARAISON_PROP = "timestamp"
    DEFAULT_TIMEZONE = LUMIBOT_DEFAULT_TIMEZONE
    DEFAULT_PYTZ = LUMIBOT_DEFAULT_PYTZ

    def __init__(self, raw):
        self._raw = raw
        self.update(raw)

    @classmethod
    def get_empty_bar(cls):
        item = {
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
    def raw(self):
        return self._raw

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, input):
        try:
            value = int(input)
            self._raw["timestamp"] = value
            self._timestamp = value
        except:
            raise ValueError("Timestamp property must be convertible to integer")

    @property
    def datetime(self):
        result = datetime.fromtimestamp(self._timestamp)
        result = self.DEFAULT_PYTZ.localize(result, is_dst=None)
        return result

    @datetime.setter
    def datetime(self, input):
        if not isinstance(input, datetime):
            raise ValueError("Datetime property must be a datetime object.")

        if self.datetime.tzinfo != input.tzinfo:
            raise ValueError("Datetime must be localized in %r" % self.DEFAULT_TIMEZONE)

        value = int(input.timestamp())
        self._raw["timestamp"] = value
        self._timestamp = value

    @property
    def open(self):
        return self._open

    @open.setter
    def open(self, input):
        try:
            value = float(input)
            self._raw["open"] = value
            self._open = value
        except:
            raise ValueError("Open property must be convertible to float")

    @property
    def high(self):
        return self._high

    @high.setter
    def high(self, input):
        try:
            value = float(input)
            self._raw["high"] = value
            self._high = value
        except:
            raise ValueError("High property must be convertible to float")

    @property
    def low(self):
        return self._low

    @low.setter
    def low(self, input):
        try:
            value = float(input)
            self._raw["low"] = value
            self._low = value
        except:
            raise ValueError("Low property must be convertible to float")

    @property
    def close(self):
        return self._close

    @close.setter
    def close(self, input):
        try:
            value = float(input)
            self._raw["close"] = value
            self._close = value
        except:
            raise ValueError("Close property must be convertible to float")

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, input):
        try:
            value = float(input)
            self._raw["volume"] = value
            self._volume = value
        except:
            raise ValueError("Volume property must be convertible to float")

    @property
    def dividend(self):
        return self._dividend

    @dividend.setter
    def dividend(self, input):
        try:
            value = float(input)
            self._raw["dividend"] = value
            self._dividend = value
        except:
            raise ValueError("Dividend property must be convertible to float")

    @property
    def stock_splits(self):
        return self._stock_splits

    @stock_splits.setter
    def stock_splits(self, input):
        try:
            value = float(input)
            self._raw["stock_splits"] = value
            self._stock_splits = value
        except:
            raise ValueError("Stock_splits property must be convertible to float")

    def update(self, data):
        self._timestamp = self._parse_property(
            data, "timestamp", required=True, type=int
        )
        self._open = self._parse_property(data, "open", required=True, type=float)
        self._high = self._parse_property(data, "high", required=True, type=float)
        self._low = self._parse_property(data, "low", required=True, type=float)
        self._close = self._parse_property(data, "close", required=True, type=float)
        self._volume = self._parse_property(data, "volume", required=True, type=float)
        self._dividend = self._parse_property(
            data, "dividend", required=False, type=float
        )
        self._stock_splits = self._parse_property(
            data, "stock_splits", required=False, type=float
        )

    def _parse_property(self, data, key, required=False, type=None):
        if required:
            if key not in data:
                raise ValueError(f"{key} key is a required field for Bar objects")

        value = data.get(key)
        if type:
            try:
                value = type(value)
            except:
                raise ValueError("%s type does not fit to %r type" % (key, type))

        return value
