from lumibot.components.agents.manager import _usage_breakdown
from lumibot.components.agents.runtime import (
    _aggregate_usage_metadata,
    _prune_large_context_strings,
    _prune_request_contents_for_context_window,
    _prune_tool_response_for_context_window,
)

from scripts.run_ai_committee_provider_benchmark import _estimate_cost as estimate_benchmark_cost
from scripts.summarize_ai_committee_provider_benchmarks import _estimate_cost as estimate_summary_cost


def test_deepseek_usage_breakdown_uses_hit_and_miss_tokens():
    usage = _usage_breakdown(
        {
            "prompt_cache_hit_tokens": 120_000,
            "prompt_cache_miss_tokens": 1_000,
            "completion_tokens": 2_500,
            "reasoning_tokens": 500,
        },
        cache_hit=False,
    )

    assert usage["input_tokens"] == 121_000
    assert usage["cached_input_tokens"] == 120_000
    assert usage["uncached_input_tokens"] == 1_000
    assert usage["output_tokens"] == 2_500
    assert usage["thinking_tokens"] == 500
    assert usage["total_tokens"] == 123_500


def test_usage_aggregation_preserves_deepseek_hit_and_miss_fields():
    aggregate = _aggregate_usage_metadata(
        [
            {
                "prompt_cache_hit_tokens": 100,
                "prompt_cache_miss_tokens": 900,
                "prompt_tokens": 1_000,
                "completion_tokens": 50,
                "total_tokens": 1_050,
            },
            {
                "prompt_cache_hit_tokens": 800,
                "prompt_cache_miss_tokens": 200,
                "prompt_tokens": 1_000,
                "completion_tokens": 60,
                "total_tokens": 1_060,
            },
        ]
    )

    assert aggregate is not None
    assert aggregate["prompt_cache_hit_tokens"] == 900
    assert aggregate["prompt_cache_miss_tokens"] == 1_100
    assert aggregate["prompt_tokens"] == 2_000
    assert aggregate["completion_tokens"] == 110
    assert aggregate["total_tokens"] == 2_110


def test_deepseek_v4_pro_cost_uses_current_discounted_hit_rate():
    usage = {
        "input_tokens": 121_000,
        "cached_input_tokens": 120_000,
        "uncached_input_tokens": 1_000,
        "output_tokens": 2_500,
    }

    benchmark_cost = estimate_benchmark_cost("deepseek/deepseek-v4-pro", usage)
    summary_cost = estimate_summary_cost("deepseek/deepseek-v4-pro", usage)

    assert benchmark_cost["input_per_m"] == 0.435
    assert benchmark_cost["cached_input_per_m"] == 0.003625
    assert benchmark_cost["output_per_m"] == 0.87
    assert benchmark_cost["cache_hit_rate"] == 0.991736
    assert benchmark_cost["cache_adjusted_estimated_usd"] == 0.003045
    assert summary_cost is not None
    assert summary_cost["cache_adjusted_estimated_usd"] == benchmark_cost["cache_adjusted_estimated_usd"]


def test_deepseek_context_pruning_replaces_only_older_tool_results():
    from google.genai import types

    contents = [
        types.Content(
            role="user",
            parts=[types.Part(text="Keep the original task and system-derived user content.")],
        )
    ]
    for idx in range(12):
        contents.append(
            types.Content(
                role="user",
                parts=[
                    types.Part(
                        functionResponse=types.FunctionResponse(
                            name=f"tool_{idx}",
                            response={"payload": "x" * 2_000, "idx": idx},
                        )
                    )
                ],
            )
        )

    result = _prune_request_contents_for_context_window(
        contents,
        context_limit_tokens=10_000,
        reserve_ratio=0.30,
        preserve_recent_tool_results=4,
    )

    assert result is not None
    assert result["pruned_tool_results"] > 0
    assert contents[0].parts[0].text == "Keep the original task and system-derived user content."
    older_response = contents[1].parts[0].function_response.response
    recent_response = contents[-1].parts[0].function_response.response
    assert older_response["lumibot_context_pruned"] is True
    assert recent_response["payload"] == "x" * 2_000


def test_deepseek_context_string_pruning_preserves_edges():
    payload, pruned = _prune_large_context_strings(
        {
            "evidence_pack": "start-" + ("x" * 1_000) + "-end",
            "small": "keep",
        },
        max_string_chars=120,
    )

    assert pruned == 1
    assert payload["small"] == "keep"
    assert payload["evidence_pack"].startswith("start-")
    assert payload["evidence_pack"].endswith("-end")
    assert "Pruned" in payload["evidence_pack"]
    assert len(payload["evidence_pack"]) <= 120


def test_deepseek_tool_response_pruning_returns_excerpt():
    result = _prune_tool_response_for_context_window(
        {"rows": ["start", "x" * 10_000, "end"]},
        tool_name="market_load_history_table",
        max_chars=500,
    )

    assert result is not None
    assert result["lumibot_tool_result_pruned"] is True
    assert result["tool_name"] == "market_load_history_table"
    assert result["original_chars"] > 500
    assert len(result["excerpt"]) <= 500
