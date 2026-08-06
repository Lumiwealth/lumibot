"""Regression coverage for forgiving timestep aliases across brokers.

Why: BotSpot analysis and agents pass many spellings (5Min, 5T, 1Day). The
public Lumibot interface must canonicalize those before broker adapters map to
provider wire formats. Fake data is forbidden; these tests only cover parsing.
"""

from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.data_sources.data_source import DataSource
from lumibot.tools.helpers import canonicalize_timestep, parse_canonical_timestep


ALPACA_UNIT_CONFIG = {
    "API_KEY": "test_api_key",
    "API_SECRET": "test_api_secret",
    "PAPER": True,
}


class TestCanonicalTimestepAliases:
    def test_five_minute_permutations(self):
        aliases = [
            "5Min",
            "5min",
            "5MIN",
            "5m",
            "5M",
            "5 min",
            "5 minutes",
            "5minute",
            "5minutes",
            "5Mins",
            "5T",
            "5t",
            "5 T",
        ]
        for alias in aliases:
            assert parse_canonical_timestep(alias) == (5, "minute")
            assert canonicalize_timestep(alias) == "5 minutes"

    def test_second_hour_day_permutations(self):
        assert canonicalize_timestep("30S") == "30 seconds"
        assert canonicalize_timestep("30sec") == "30 seconds"
        assert canonicalize_timestep("30 seconds") == "30 seconds"
        assert canonicalize_timestep("2H") == "2 hours"
        assert canonicalize_timestep("2hr") == "2 hours"
        assert canonicalize_timestep("2 hours") == "2 hours"
        assert canonicalize_timestep("1Day") == "day"
        assert canonicalize_timestep("1D") == "day"
        assert canonicalize_timestep("1 day") == "day"
        assert canonicalize_timestep("day") == "day"
        assert canonicalize_timestep("minute") == "minute"
        assert canonicalize_timestep("1Min") == "minute"

    def test_week_and_month(self):
        assert parse_canonical_timestep("1w") == (1, "week")
        assert parse_canonical_timestep("2 weeks") == (2, "week")
        assert parse_canonical_timestep("1mo") == (1, "month")
        assert parse_canonical_timestep("2 months") == (2, "month")


class TestAlpacaReverseAliasLookup:
    def test_reverse_accepts_five_min_aliases(self):
        data_source = AlpacaData(ALPACA_UNIT_CONFIG)
        for alias in ("5Min", "5min", "5T", "5 minutes", "5m"):
            parsed = data_source._parse_source_timestep(alias, reverse=True)
            assert parsed.amount == 5
            assert parsed.unit == TimeFrameUnit.Minute

    def test_forward_still_normalizes(self):
        data_source = AlpacaData(ALPACA_UNIT_CONFIG)
        assert data_source._parse_source_timestep("15Min") == "15 minutes"
        assert data_source._parse_source_timestep("1Day") == "day"
        parsed = data_source._parse_source_timestep("5 minutes", reverse=True)
        # TimeFrame equality is identity-based in some alpaca-py versions.
        assert isinstance(parsed, TimeFrame)
        assert parsed.amount == 5
        assert parsed.unit == TimeFrameUnit.Minute


class TestDataSourceMappingUsesCanonicalAliases:
    def test_base_mapping_matches_canonical_forms(self):
        class SampleSource(DataSource):
            SOURCE = "SAMPLE"
            TIMESTEP_MAPPING = [
                {"timestep": "5 minutes", "representations": ["5 minutes", "5min"]},
                {"timestep": "day", "reps": ["day", "1d"]},
            ]

            def __init__(self):
                pass

            def get_chains(self, *args, **kwargs):
                return None

            def get_historical_prices(self, *args, **kwargs):
                return None

            def get_last_price(self, *args, **kwargs):
                return None

        source = SampleSource()
        assert source._parse_source_timestep("5Min") == "5 minutes"
        assert source._parse_source_timestep("5T") == "5 minutes"
        assert source._parse_source_timestep("1Day") == "day"
        assert source._parse_source_timestep("5Min", reverse=True) == "5 minutes"


class TestStrategyTimestepAliasParity:
    def test_strategy_accepts_pandas_t_and_seconds(self):
        from lumibot.strategies.strategy import Strategy

        class TestStrategy(Strategy):
            def __init__(self):
                pass

        strategy = TestStrategy()
        assert strategy._parse_timestep("5T") == (5, "minute")
        assert strategy._parse_timestep("5Min") == (5, "minute")
        assert strategy._parse_timestep("30S") == (30, "second")
        assert strategy._parse_timestep("1Day") == (1, "day")
