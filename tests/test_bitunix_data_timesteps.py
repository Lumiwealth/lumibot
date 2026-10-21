"""Timestep resolution for the BitUnix data source.

Regression tests for the case where a canonical Lumibot timestep resolved to a
different interval than the one asked for. ``get_timestep_from_string`` matched
only each row's ``representations`` and fell back to ``"minute"``, so six of the
nine entries in ``TIMESTEP_MAPPING`` were unreachable by their own name and a
request for ``"4 hours"`` returned 1-minute candles with no error raised.
"""

import unittest

from lumibot.data_sources.bitunix_data import BitunixData


class TestBitunixTimesteps(unittest.TestCase):
    def setUp(self):
        # The timestep parser touches no client and needs no credentials.
        self.data = BitunixData.__new__(BitunixData)

    def test_canonical_timestep_names_resolve_to_their_own_interval(self):
        """Every ``timestep`` in TIMESTEP_MAPPING must reach its own row.

        Before the fix, "3 minutes", "5 minutes", "15 minutes", "30 minutes",
        "2 hours" and "4 hours" all resolved to "1m".
        """
        expected = {
            "minute": "1m",
            "3 minutes": "3m",
            "5 minutes": "5m",
            "15 minutes": "15m",
            "30 minutes": "30m",
            "hour": "1h",
            "2 hours": "2h",
            "4 hours": "4h",
            "day": "1d",
        }
        for mapping in BitunixData.TIMESTEP_MAPPING:
            with self.subTest(timestep=mapping["timestep"]):
                interval = self.data._parse_source_timestep(mapping["timestep"])
                self.assertEqual(interval, expected[mapping["timestep"]])

    def test_declared_representations_still_resolve(self):
        """The aliases that already worked must keep working."""
        for mapping in BitunixData.TIMESTEP_MAPPING:
            for representation in mapping["representations"]:
                with self.subTest(representation=representation):
                    interval = self.data._parse_source_timestep(representation)
                    self.assertIn(interval, mapping["representations"])

    def test_shared_broker_aliases_resolve(self):
        """The aliases canonicalize_timestep accepts for every other source."""
        for alias, expected in (
            ("4H", "4h"),
            ("240T", "4h"),
            ("1 hour", "1h"),
            ("60m", "1h"),
            ("15Min", "15m"),
            ("30T", "30m"),
            ("1Day", "1d"),
        ):
            with self.subTest(alias=alias):
                self.assertEqual(self.data._parse_source_timestep(alias), expected)

    def test_unsupported_interval_falls_back_rather_than_snapping(self):
        """An interval BitUnix does not offer must not become a nearby one.

        Resolving "45m" to "30m" or "10 minutes" to "5m" would be the same
        defect this change fixes, wearing a different number: a request served
        at an interval nobody asked for, with no error.
        """
        for timestep in ("45m", "10 minutes", "90m", "2 days", "week", "unknown"):
            with self.subTest(timestep=timestep):
                self.assertEqual(self.data._parse_source_timestep(timestep), "1m")

    def test_surrounding_whitespace_is_ignored(self):
        """`" 240 "` is `"240"`.

        The previous implementation stripped before comparing, so dropping the
        strip here would quietly send a padded timestep to the default.
        """
        for padded, expected in ((" 240 ", "4h"), ("  30 ", "30m"), (" 4h ", "4h"),
                                 ("\t1Day\n", "1d")):
            with self.subTest(timestep=padded):
                self.assertEqual(self.data._parse_source_timestep(padded), expected)

    def test_unhashable_input_falls_back_instead_of_raising(self):
        """`bytearray` must still fall through, not blow up.

        ``canonicalize_timestep`` is ``lru_cache``d, so handing it an unhashable
        value raises TypeError from inside the decorator. On dev a bytearray
        simply matched nothing and returned "1m"; that is preserved.
        """
        self.assertEqual(self.data._parse_source_timestep(bytearray(b"4h")), "1m")
        self.assertEqual(self.data._parse_source_timestep(b"4h"), "1m")

    def test_non_string_timestep_still_raises(self):
        """``None`` must not become a minute either.

        The previous implementation called ``timestep.lower()`` and raised
        AttributeError; turning that into a silent "1m" would be a regression
        of exactly the kind being fixed.
        """
        for timestep in (None, True, 1.0, 5):
            with self.subTest(timestep=timestep):
                with self.assertRaises(AttributeError):
                    self.data._parse_source_timestep(timestep)


if __name__ == "__main__":
    unittest.main()
