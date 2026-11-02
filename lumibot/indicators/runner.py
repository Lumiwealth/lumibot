"""Run Indicator v2 instances outside a strategy.

``IndicatorRunner`` loads history through the data source's normal
``get_historical_prices`` (so the data source's own caching applies), then
takes live bars one at a time through ``on_bar(bar, closed)``::

    from lumibot.indicators import IndicatorRunner

    runner = IndicatorRunner(data_source, asset, quote=quote, timestep="minute")
    rsi_id = runner.add("rsi", length=14)
    snapshot = runner.load_history(500)          # {"bars": [...], "indicators": {...}}
    ...
    values = runner.on_bar(bar, closed=False)    # forming bar: rollback-safe
    values = runner.on_bar(bar, closed=True)     # bar closed: committed

Any LumiBot data source works: backtesting sources answer as of their clock,
live sources answer as of now. Where a data source can stream bars, feed each
streamed bar to ``on_bar``; history is never recomputed.
"""

from __future__ import annotations

import itertools
from collections import deque

from lumibot.indicators.base import Bar, Indicator, iter_bars, to_bar
from lumibot.indicators.execution import prepare_indicator
from lumibot.indicators.indicator_registry import registry as default_registry


def _bar_dict(bar: Bar) -> dict:
    return {
        "t": bar.time.isoformat() if bar.time is not None else None,
        "o": bar.open,
        "h": bar.high,
        "l": bar.low,
        "c": bar.close,
        "v": bar.volume,
    }


def _replay(instance, bars) -> dict:
    """History for one instance as series. ``feed`` replays without calling
    ``infer``; an executor proxy without ``feed`` gets one closed update per bar."""
    feed = getattr(instance, "feed", None)
    if feed is not None:
        return feed(list(bars))
    out = {spec.id: [] for spec in instance.outputs}
    for bar in bars:
        for output_id, value in instance.update(bar, closed=True).items():
            out[output_id].append(value)
    return out


class IndicatorRunner:
    """One asset and timestep, any number of indicator instances.

    Parameters
    ----------
    data_source
        Any object with LumiBot's ``get_historical_prices(asset, length,
        timestep=..., quote=...)`` returning ``Bars`` (or something with a
        ``df`` OHLCV DataFrame).
    asset, quote, timestep
        Passed straight to ``get_historical_prices``.
    max_bars
        Closed bars kept so an indicator added later can be replayed without
        another history request. Defaults to the last ``load_history`` length.
    registry
        Where ``add(key, ...)`` looks indicators up (the shared one by default).
    executor
        How added indicators run (see :mod:`lumibot.indicators.execution`).
        ``None`` uses the executor registered when ``add`` is called, which
        is in-process unless a host changed it.
    """

    def __init__(
        self, data_source, asset, quote=None, timestep="minute", *, max_bars=None, registry=None, executor=None
    ):
        self.data_source = data_source
        self.asset = asset
        self.quote = quote
        self.timestep = timestep
        self.registry = registry or default_registry
        self.executor = executor
        self._max_bars = max_bars
        self._bars: deque = deque(maxlen=max_bars)
        self._instances: dict[str, Indicator] = {}
        self._series: dict[str, dict[str, deque]] = {}
        self._ids = itertools.count(1)
        # A closed bar some instances already committed while another raised.
        self._pending_bar = None
        self._pending: dict[str, dict] = {}

    # ---- indicators --------------------------------------------------------

    def add(self, indicator, version=None, *, instance_id=None, **inputs) -> str:
        """Add an indicator by registry key (or an ``Indicator`` instance).

        If history is already loaded, the kept bars are replayed into it so its
        outputs line up with the others. Returns the instance id."""
        instance = indicator if isinstance(indicator, Indicator) else self.registry.create(indicator, version, **inputs)
        instance = prepare_indicator(instance, self.executor)
        instance_id = instance_id or f"{instance.key}-{next(self._ids)}"
        if instance_id in self._instances:
            raise ValueError(f"indicator instance {instance_id!r} already exists")
        series = {spec.id: deque(maxlen=self._bars.maxlen) for spec in instance.outputs}
        for output_id, values in _replay(instance, self._bars).items():
            series[output_id].extend(values)
        self._instances[instance_id] = instance
        self._series[instance_id] = series
        return instance_id

    def remove(self, instance_id: str) -> None:
        del self._instances[instance_id]
        del self._series[instance_id]

    def instance(self, instance_id: str):
        return self._instances[instance_id]

    @property
    def instance_ids(self) -> list:
        return list(self._instances)

    def outputs(self, instance_id: str) -> dict:
        """Output series (aligned with the kept bars) for one instance."""
        return {k: list(v) for k, v in self._series[instance_id].items()}

    # ---- data --------------------------------------------------------------

    def load_history(self, length: int, *, last_bar_closed: bool = True, **history_kwargs) -> dict:
        """Fetch ``length`` bars with ``get_historical_prices`` and feed them.

        Every bar is treated as closed unless ``last_bar_closed=False``, in which
        case the newest bar is applied as a forming bar (some live sources
        return the still-building bar last). Indicators are reset first.
        Returns a snapshot: ``{"bars": [...], "indicators": {id: {...}}}``."""
        kwargs = {"timestep": self.timestep, **history_kwargs}
        if self.quote is not None:
            kwargs["quote"] = self.quote
        result = self.data_source.get_historical_prices(self.asset, length, **kwargs)
        frame = getattr(result, "df", result)
        bars = list(iter_bars(frame)) if frame is not None else []
        forming = None
        if bars and not last_bar_closed:
            forming = bars.pop()
        maxlen = self._max_bars or max(length, 1)
        self._bars = deque(bars, maxlen=maxlen)
        self._pending_bar, self._pending = None, {}
        for instance_id, instance in self._instances.items():
            instance.reset(instance.inputs)
            series = {spec.id: deque(maxlen=maxlen) for spec in instance.outputs}
            for output_id, values in _replay(instance, bars).items():
                series[output_id].extend(values)
            self._series[instance_id] = series
        snapshot = self.snapshot()
        if forming is not None:
            snapshot["forming"] = {"bar": _bar_dict(forming), "indicators": self.on_bar(forming, closed=False)}
        return snapshot

    def on_bar(self, bar, closed: bool) -> dict:
        """Apply one live bar to every instance. Returns ``{id: {output: value}}``.

        Forming bars (``closed=False``) never change state. A closed bar is
        committed for every instance or for none: values from instances that
        already committed it are held until all succeed, so if one instance
        raises, the kept bars and series do not move and retrying the same
        bar does not apply it twice to the instances that already took it."""
        bar = to_bar(bar)
        if not closed:
            return {iid: instance.update(bar, closed=False) for iid, instance in self._instances.items()}
        if self._pending_bar != bar:
            self._pending_bar, self._pending = bar, {}
        for instance_id, instance in self._instances.items():
            if instance_id not in self._pending:
                self._pending[instance_id] = instance.update(bar, closed=True)
        result = {iid: self._pending[iid] for iid in self._instances}
        for instance_id, values in result.items():
            for output_id, value in values.items():
                self._series[instance_id][output_id].append(value)
        self._bars.append(bar)
        self._pending_bar, self._pending = None, {}
        return result

    def snapshot(self) -> dict:
        """Kept bars plus every instance's aligned output series and schema."""
        return {
            "bars": [_bar_dict(bar) for bar in self._bars],
            "indicators": {
                instance_id: {
                    "key": instance.key,
                    "version": instance.version,
                    "meta": instance.describe(instance.inputs),
                    "outputs": self.outputs(instance_id),
                }
                for instance_id, instance in self._instances.items()
            },
        }


__all__ = ["IndicatorRunner"]
