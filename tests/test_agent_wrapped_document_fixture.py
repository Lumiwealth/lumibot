"""The agent receives native text whose wrapped bounds remain retrievable."""

from lumibot.components.agents import BuiltinTools
from scripts.agent_eval_production_fixture import ProductionFixture, _WRAPPED_RANGES, _wrapped_ranges_text
from scripts.run_agent_evals import build_fixture, load_cases


def test_wrapped_instrument_marker_and_other_instrument_do_not_erase_stock_value():
    # Reproduce the observed multi-instrument layout, rather than assuming that
    # every equity marker, owner and value shares the ticker's line.
    text = _wrapped_ranges_text()
    assert "Call options (QZX13) [OP] Owner A None" in text
    assert "Ordinary common shares (QZX13)\n[EQ]\nOwner A $100,001 -\n$250,000\nNone" in text


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
            # Both observed layouts must retain the complete instrument row.
            inline = f"({symbol}) [EQ] Owner A ${lower:,} -\n${upper:,}"
            wrapped = f"({symbol})\n[EQ]\nOwner A ${lower:,} -\n${upper:,}"
            assert inline in text or wrapped in text
        assert "QZXZERO" in text and "[CASH]" in text
        case = load_cases({"web_documents_wrapped_table_ranges"})[0]
        assert case["requiredRepetitions"] == 5
        assert case["machineContract"]["forbidOrderTools"] is True
    finally:
        fixture.close()


def test_actual_provider_sdk_is_in_the_qualified_dependency_tuple():
    from scripts import run_agent_evals as evals

    assert "massive" in evals.EVAL_SDK_PACKAGES
    assert evals.sdk_versions()["massive"] == "2.9.1"
