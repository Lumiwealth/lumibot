from datetime import datetime
from zoneinfo import ZoneInfo

from lumibot.example_strategies.ai_investment_committee import AIInvestmentCommitteeStrategy


class _CommitteeControlHarness:
    def __init__(self, current_dt, start_on_or_after):
        self.parameters = {"committee_start_on_or_after": start_on_or_after}
        self.current_dt = current_dt
        self.messages = []

    def get_datetime(self):
        return self.current_dt

    def log_message(self, message, **kwargs):
        self.messages.append(message)


def test_ai_committee_start_gate_waits_until_configured_date():
    before = _CommitteeControlHarness(
        datetime(2024, 1, 7, 9, 30, tzinfo=ZoneInfo("America/New_York")),
        "2024-01-08",
    )
    on_date = _CommitteeControlHarness(
        datetime(2024, 1, 8, 9, 30, tzinfo=ZoneInfo("America/New_York")),
        "2024-01-08",
    )

    assert AIInvestmentCommitteeStrategy._before_committee_start(before) is True
    assert AIInvestmentCommitteeStrategy._before_committee_start(on_date) is False


def test_ai_committee_start_gate_supports_timestamp_with_timezone():
    before = _CommitteeControlHarness(
        datetime(2024, 1, 8, 9, 29, tzinfo=ZoneInfo("America/New_York")),
        "2024-01-08T09:30:00-05:00",
    )
    at_time = _CommitteeControlHarness(
        datetime(2024, 1, 8, 9, 30, tzinfo=ZoneInfo("America/New_York")),
        "2024-01-08T09:30:00-05:00",
    )

    assert AIInvestmentCommitteeStrategy._before_committee_start(before) is True
    assert AIInvestmentCommitteeStrategy._before_committee_start(at_time) is False
