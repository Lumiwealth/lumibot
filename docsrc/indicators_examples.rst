Indicator examples
==================

.. meta::
   :description: Five complete custom LumiBot indicators you can copy: Donchian channel, an RSI-style up/down ratio, volume-weighted trend, an outside-data funding rate, and a numpy machine-learning classifier.

Each example is a real file in ``lumibot/example_indicators/``. LumiBot's test
suite (``tests/test_example_indicators.py``) runs every one: values against a
direct calculation, live updates against a full recompute, forming bars
against rollback, and a check that no value uses a future bar. They are
teaching examples, not trading advice.

Use one in a strategy::

    from lumibot.example_indicators import up_down_ratio   # registers "up_down_ratio"

    ratio = self.indicators.get("up_down_ratio", asset, length=14)

1. Donchian channel (on the price chart)
----------------------------------------

Highest high, lowest low and the midline of the last ``length`` bars.

.. literalinclude:: ../lumibot/example_indicators/donchian_midline.py
   :language: python

2. Up/down ratio (RSI-style, own pane)
--------------------------------------

The share of recent movement that went up, 0 to 100, with plain sums.

.. literalinclude:: ../lumibot/example_indicators/up_down_ratio.py
   :language: python

3. Volume-weighted trend
------------------------

How far the volume-weighted average sits above or below the plain average.

.. literalinclude:: ../lumibot/example_indicators/volume_weighted_trend.py
   :language: python

4. Funding rate from a public exchange API (outside data)
---------------------------------------------------------

Explained on :doc:`indicators_outside_data`.

.. literalinclude:: ../lumibot/example_indicators/funding_rate.py
   :language: python

5. Machine-learning direction classifier (numpy)
------------------------------------------------

A logistic regression trained on past returns only, retrained every few
bars. Put any model inside an indicator the same way: train or load it in
``state``, predict in ``step``.

.. literalinclude:: ../lumibot/example_indicators/ml_direction.py
   :language: python

To use a model you trained elsewhere (scikit-learn, LightGBM, ONNX), load it
once on the indicator object and call it in ``step`` with features from
closed bars only.
