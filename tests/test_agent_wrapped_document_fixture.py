"""The agent receives native text whose wrapped bounds remain retrievable."""

from lumibot.components.agents import BuiltinTools
from scripts.agent_eval_production_fixture import ProductionFixture, _WRAPPED_RANGES, _wrapped_ranges_text
from scripts.run_agent_evals import build_fixture, load_cases


def test_unfiltered_pagination_retains_bounds_omitted_by_filtered_hits():
    fixture = ProductionFixture(build_fixture("web_documents_wrapped_ranges"))
    try:
        read = BuiltinTools.web.read_document().binder(fixture.strategy, fixture.manager).function
        url = "https://reports.example.net/reports/portfolio.txt"
        filtered = read(url=url, find="[EQ]", max_chars=2500)
        assert "QZX01" in filtered["text"]
        assert "$250,001 -" in filtered["text"]
        assert "$500,000" not in filtered["text"]
        text, start = "", 0
        while True:
            page = read(url=url, start=start, max_chars=2500)
            text += page["text"]
            if page["next_start"] is None:
                break
            assert page["next_start"] > start
            start = page["next_start"]
        assert start > 0  # The source requires more than one native text window.
        assert text == _wrapped_ranges_text()
        for symbol, lower, upper in _WRAPPED_RANGES:
            assert f"({symbol}) [EQ] Owner A ${lower:,} -\n${upper:,}" in text
        assert "QZXZERO" in text and "[CASH]" in text
        case = load_cases({"web_documents_wrapped_table_ranges"})[0]
        assert case["requiredRepetitions"] == 5
        assert case["machineContract"]["forbidOrderTools"] is True
    finally:
        fixture.close()


def test_actual_polygon_sdk_is_in_the_qualified_dependency_tuple():
    from scripts import run_agent_evals as evals

    assert "massive" in evals.EVAL_SDK_PACKAGES
    assert evals.sdk_versions()["massive"] == "2.9.1"
