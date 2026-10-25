"""Example indicators used by the LumiBot "Indicators" documentation.

Every file here runs and is executed by ``tests/test_example_indicators.py``.
Importing a module registers its indicator in the shared registry, so
``self.indicators.get("<key>", asset)`` and ``IndicatorRunner.add("<key>")``
find it. They are teaching examples, not trading advice.
"""
