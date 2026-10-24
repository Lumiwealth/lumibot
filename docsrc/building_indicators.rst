Building indicators
===================

.. meta::
   :description: Write a LumiBot indicator that updates one bar at a time, runs inside a strategy or on its own, and describes itself to chart UIs and AI agents.

An **Indicator v2** is a small Python class that processes **one bar at a
time**. The same class runs:

- inside a strategy with ``self.indicators.get(...)``,
- on its own with :py:class:`~lumibot.indicators.IndicatorRunner`, using any
  LumiBot data source (``get_historical_prices`` and its caching apply), and
- on a live chart, where a new bar costs one update instead of recomputing all
  history.

This page is written for people and for AI agents that generate indicators.
Follow the rules in `Rules for indicator authors`_ and your indicator gets
correct live behavior for free.

Use a built-in
--------------

Built-ins: ``sma``, ``ema``, ``rsi``, ``macd``, ``bbands``, ``atr``,
``stoch``, ``vwap`` and ``wavetrend``. They follow TradingView Pine Script v6
math: ``ema`` and ``rma`` are seeded with the simple average of the first
``length`` values, ``rsi`` and ``atr`` use Wilder smoothing (``rma``),
Bollinger Bands use the population standard deviation, ``stoch`` is not
clamped to 0..100, and ``vwap`` restarts at each UTC day.

Inside a strategy::

    def on_trading_iteration(self):
        rsi = self.indicators.get("rsi", self.asset, length=14)   # {"rsi": 61.2}
        macd = self.indicators.get("macd", self.asset, timestep="minute")
        if rsi and rsi["rsi"] is not None and rsi["rsi"] < 30:
            ...

``get`` returns a dict of output values for the current bar. A value is
``None`` while the indicator is still warming up. Only bars at or before
strategy time are used. Pass ``version=1`` to pin the math.

The existing ``self.indicators.sma(...)`` / ``custom(...)`` accessors keep
working exactly as before (see :doc:`indicators`).

On its own (scripts, notebooks, chart servers)::

    from lumibot.indicators import IndicatorRunner

    runner = IndicatorRunner(data_source, asset, quote=quote, timestep="minute")
    wt = runner.add("wavetrend", channel_length=10, average_length=21)
    snapshot = runner.load_history(500)
    # snapshot = {"bars": [...], "indicators": {wt: {"key", "version", "meta", "outputs"}}}

    # then, for every live bar from your stream:
    values = runner.on_bar(bar, closed=False)   # still forming: nothing is committed
    values = runner.on_bar(bar, closed=True)    # bar closed: state advances

On a list of bars or an OHLCV DataFrame::

    from lumibot.indicators import registry

    series = registry.create("bbands", length=20, mult=2.0).compute(df)
    series["upper"]   # list aligned with the bars, None during warmup

Bars can be dicts with ``t/o/h/l/c/v`` (or ``time/open/high/low/close/volume``)
keys, or a DataFrame whose index is the bar time. Numeric times are epoch
milliseconds; naive times are treated as UTC.

Write your own: WaveTrend example
---------------------------------

This is the built-in WaveTrend oscillator, written the way you would write any
indicator::

    from lumibot.indicators import Indicator, InputSpec, Level, OutputSpec, register
    from lumibot.indicators.primitives import RollingSMA, SeededEMA, safe_div

    @register
    class MyWaveTrend(Indicator):
        key = "my_wavetrend"
        version = 1                  # bump when you change the math
        title = "WaveTrend Oscillator"
        short_title = "WT"
        overlay = False              # draw in its own pane

        inputs = [
            InputSpec("channel_length", "int", 10, min=1, label="Channel length"),
            InputSpec("average_length", "int", 21, min=1, label="Average length"),
            InputSpec("source", "source", "ohlc4", label="Source"),
            InputSpec("ob1", "float", 60.0, label="Overbought"),
            InputSpec("os1", "float", -60.0, label="Oversold"),
        ]
        outputs = [
            OutputSpec("wave1", "Wave 1", "area", "#26A69A"),
            OutputSpec("wave2", "Wave 2", "area", "#2962FF"),
            OutputSpec("cross", "Cross", "markers", "#FFFFFF"),
        ]

        @classmethod
        def levels_for(cls, inputs):
            return [Level(0, "Zero", "dotted"), Level(inputs["ob1"], "OB"), Level(inputs["os1"], "OS")]

        def init_state(self):
            n1 = self.inputs["channel_length"]
            return {
                "esa": SeededEMA(n1),
                "d": SeededEMA(n1),
                "wave1": SeededEMA(self.inputs["average_length"]),
                "wave2": RollingSMA(3),
                "prev": None,
            }

        def step(self, state, bar):
            ap = bar.source(self.inputs["source"])           # ohlc4 by default
            esa = state["esa"].update(ap)
            d = state["d"].update(None if esa is None else abs(ap - esa))
            ci = None if esa is None or d is None else safe_div(ap - esa, 0.015 * d)
            wt1 = state["wave1"].update(ci)
            wt2 = state["wave2"].update(wt1)
            cross = None
            prev = state["prev"]
            if prev and None not in prev and wt1 is not None and wt2 is not None:
                if (wt1 > wt2) != (prev[0] > prev[1]):
                    cross = wt2
            state["prev"] = (wt1, wt2)
            return {"wave1": wt1, "wave2": wt2, "cross": cross}

Now ``self.indicators.get("my_wavetrend", asset)``,
``runner.add("my_wavetrend")`` and ``registry.list()`` all know about it.

What each piece does
--------------------

``key`` and ``version``
    The registry name and the version of the math. ``(key, version)`` is
    immutable: registering a different class under an existing pair raises an
    error. Change the formula, bump ``version``.

``inputs``
    A list of :py:class:`~lumibot.indicators.InputSpec`:
    ``InputSpec(name, type, default, min=, max=, step=, options=, label=)``.
    Types: ``int``, ``float``, ``bool``, ``color``, ``source``, ``string``,
    ``enum``. A ``source`` input picks ``close``, ``open``, ``high``, ``low``,
    ``hl2``, ``hlc3``, ``ohlc4`` or ``hlcc4``. Inputs are validated when the
    indicator is created: unknown names, wrong types and out-of-range values
    raise ``InvalidIndicatorInputError``. Inside the indicator,
    ``self.inputs`` is the dict of resolved values.

``outputs``
    A list of :py:class:`~lumibot.indicators.OutputSpec`:
    ``OutputSpec(id, label, kind, color)``. Kinds: ``line``, ``area``,
    ``histogram``, ``scatter``, ``markers``. ``step`` returns a value (or
    ``None``) for each output id.

``levels`` / ``levels_for(inputs)``
    Fixed guide lines (``Level(value, label, style)``). Use the ``levels``
    class attribute for constant lines, or override ``levels_for`` when they
    depend on inputs.

``overlay``
    ``True`` draws on the price chart, ``False`` in its own pane.

``init_state()``
    Returns the state for the current inputs. Put everything that changes
    from bar to bar here, nowhere else.

``step(state, bar)``
    Advances ``state`` by one bar and returns this bar's outputs. ``bar`` is a
    :py:class:`~lumibot.indicators.Bar` with ``time``, ``open``, ``high``,
    ``low``, ``close``, ``volume`` and ``hl2``/``hlc3``/``ohlc4``/``hlcc4``.

Rules for indicator authors
---------------------------

1. **Keep all changing data in the state returned by** ``init_state``. The
   base class copies that state to evaluate a forming bar and throws the copy
   away, so ``update(bar, closed=False)`` can run on every tick and the
   ``closed=True`` call commits exactly once. State stored on ``self``
   instead would leak forming ticks into history.
2. **Never look at future bars.** ``step`` only sees the current bar. The
   value at bar *k* must depend only on bars ``0..k``. The test suite checks
   this for every built-in by comparing a run on the first *k* bars with a
   full run.
3. **Do not recompute history.** Keep rolling state (running sums, the
   previous value, a fixed window). The primitives in
   ``lumibot.indicators.primitives`` (``RollingSMA``, ``SeededEMA``,
   ``SeededRMA``, ``RollingStdev``, ``RollingMax``, ``RollingMin``,
   ``Previous``, ``safe_div``) are O(1) per bar.
4. **Return** ``None`` **for "no value"**, never a made-up number. Primitives
   treat a ``None`` input like Pine's ``na``: the output is ``None`` and the
   state does not advance.
5. **Bump** ``version`` **whenever the math changes**, so saved charts and
   strategies that pinned the old version keep their numbers.

The streaming contract
----------------------

``update(bar, closed=True)``
    Commits one bar and returns ``{output_id: value}``. Closed bars must move
    forward in time.

``update(bar, closed=False)``
    Evaluates a still-forming bar without changing state. Call it as often as
    you like; the next ``closed=True`` call is unaffected.

``compute(bars)``
    Full series on a fresh instance. Always equal to looping ``update``.

``snapshot()`` / ``restore(snapshot)``
    Hand the committed state to another instance or process (snapshots
    pickle). ``restore`` refuses a snapshot from another key, version or
    inputs.

``describe()``
    A JSON-safe schema for chart UIs and AI agents: ``key``, ``version``,
    ``title``, ``short_title``, ``overlay``, ``inputs``, ``outputs``,
    ``levels``. ``registry.list()`` returns it for every registered
    indicator.

Registry
--------

::

    from lumibot.indicators import registry

    registry.list()                 # describe() of the latest version of each
    registry.get("rsi")             # latest class
    registry.get("rsi", 1)          # pinned version
    registry.create("rsi", length=7)
    registry.get("nope")            # raises UnknownIndicatorError

Use :py:class:`~lumibot.indicators.IndicatorRegistry` for a private registry
(for example to test indicators without touching the shared one).
