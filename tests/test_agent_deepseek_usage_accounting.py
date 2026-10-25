from types import SimpleNamespace

import pytest

from lumibot.components.agents.manager import _usage_breakdown
from lumibot.components.agents.runtime import (
    GoogleADKRuntime,
    RuntimeRequest,
    _aggregate_usage_metadata,
    _prune_large_context_strings,
    _prune_request_contents_for_context_window,
    _request_contents_length,
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


def test_context_pruning_does_not_expand_small_tool_results():
    from google.genai import types

    contents = [
        types.Content(
            role="user",
            parts=[types.Part(text="Use compact tool results without expanding them.")],
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
                            response={"value": idx},
                        )
                    )
                ],
            )
        )

    before = _request_contents_length(contents)
    result = _prune_request_contents_for_context_window(
        contents,
        context_limit_tokens=1_000,
        reserve_ratio=0.01,
        preserve_recent_tool_results=4,
        always_prune_older_tool_results=True,
    )
    after = _request_contents_length(contents)

    assert result is None
    assert after == before
    assert contents[1].parts[0].function_response.response == {"value": 0}


def test_context_pruning_does_not_repeat_prune_already_pruned_results():
    from google.genai import types

    contents = [
        types.Content(
            role="user",
            parts=[types.Part(text="Keep task text.")],
        )
    ]
    for idx in range(8):
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

    first = _prune_request_contents_for_context_window(
        contents,
        context_limit_tokens=10_000,
        reserve_ratio=0.30,
        preserve_recent_tool_results=4,
    )
    assert first is not None

    before_second = _request_contents_length(contents)
    second = _prune_request_contents_for_context_window(
        contents,
        context_limit_tokens=10_000,
        reserve_ratio=0.30,
        preserve_recent_tool_results=4,
        always_prune_older_tool_results=True,
    )
    after_second = _request_contents_length(contents)

    assert second is None
    assert after_second == before_second


def test_gemini_context_pruning_uses_realistic_default_char_budget():
    from google.genai import types

    contents = [
        types.Content(
            role="user",
            parts=[types.Part(text="Gemini 3.1 has a large context window.")],
        )
    ]
    for idx in range(5):
        contents.append(
            types.Content(
                role="user",
                parts=[
                    types.Part(
                        functionResponse=types.FunctionResponse(
                            name=f"tool_{idx}",
                            response={"payload": "x" * 15_000, "idx": idx},
                        )
                    )
                ],
            )
        )

    before = _request_contents_length(contents)
    result = _prune_request_contents_for_context_window(
        contents,
        context_limit_tokens=1_048_576,
        preserve_recent_tool_results=4,
    )

    assert result is None
    assert _request_contents_length(contents) == before


def test_context_pruning_preserves_tool_evidence_until_request_is_oversized():
    from google.genai import types

    contents = [
        types.Content(role="user", parts=[types.Part(text="Manage signed positions safely.")])
    ]
    for idx in range(8):
        contents.append(
            types.Content(
                role="user",
                parts=[
                    types.Part(
                        functionResponse=types.FunctionResponse(
                            name=f"tool_{idx}",
                            response={"position": idx, "payload": "x" * 1_000},
                        )
                    )
                ],
            )
        )

    before = _request_contents_length(contents)
    result = _prune_request_contents_for_context_window(
        contents,
        context_limit_tokens=1_000_000,
    )

    assert result is None
    assert _request_contents_length(contents) == before
    assert all(
        not part.function_response.response.get("lumibot_context_pruned", False)
        for content in contents[1:]
        for part in content.parts
    )


def test_skill_loading_results_remain_complete_before_model_call():
    request = RuntimeRequest(
        agent_name="test",
        model="gemini-3.5-flash-lite",
        system_prompt="test",
        task_prompt="test",
        context=None,
        runtime_context=None,
        memory_state=None,
        memory_notes=[],
        bound_tools=[],
    )
    callback = GoogleADKRuntime()._before_model_context_pruning_callback(request)
    assert callback is not None

    from google.genai import types
    response = {"instructions": "x" * 10_000}
    content = types.Content(role="user", parts=[types.Part(function_response=types.FunctionResponse(
        name="load_skill", response=response,
    ))])
    assert callback(llm_request=SimpleNamespace(contents=[content])) is None
    assert response["instructions"] == "x" * 10_000
    assert content.parts[0].function_response.response == response


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


def test_context_pruning_keeps_recent_large_results_when_removing_old_history():
    from google.genai import types

    contents = [types.Content(role="user", parts=[types.Part(function_response=types.FunctionResponse(
        name=f"records_{i}", response={"rows": [f"row-{i}-start", "x" * 10_000, f"row-{i}-end"]},
    ))]) for i in range(8)]
    recent = [part.parts[0].function_response.response.copy() for part in contents[-4:]]
    result = _prune_request_contents_for_context_window(contents, context_limit_tokens=15_000)
    assert result is not None and result["pruned_tool_results"] > 0
    assert [part.parts[0].function_response.response for part in contents[-4:]] == recent


@pytest.mark.parametrize("model", ["chatgpt/gpt-6-luna", "gemini-3.5-flash-lite", "deepseek/deepseek-chat"])
@pytest.mark.parametrize("tool_name", ["records", "read_document", "account_positions", "load_skill"])
def test_current_tool_results_preserve_complete_typed_rows(model, tool_name):
    from google.genai import types

    request = RuntimeRequest(
        agent_name="test", model=model, system_prompt="test", task_prompt="test",
        context=None, runtime_context=None, memory_state=None, memory_notes=[], bound_tools=[],
    )
    runtime = GoogleADKRuntime()
    rows = [{"record_id": i, "quantity": i + 1, "description": "detail " * 12} for i in range(100)]
    response = {"ok": True, "rows": rows, "row_count": len(rows)}
    factory = getattr(runtime, "_after_tool_context_pruning_callback", None)
    after_tool = factory(request) if factory else None
    transformed = after_tool(tool=SimpleNamespace(name=tool_name), tool_response=response) if after_tool else None
    content = types.Content(role="user", parts=[types.Part(function_response=types.FunctionResponse(
        name=tool_name, response=transformed if transformed is not None else response,
    ))])
    before_model = runtime._before_model_context_pruning_callback(request)
    if before_model:
        before_model(llm_request=SimpleNamespace(contents=[content]))
    visible = content.parts[0].function_response.response
    assert visible.get("rows") == rows
    assert visible["rows"][50]["record_id"] == 50
    assert visible["rows"][50]["quantity"] == 51
    assert visible["row_count"] == 100
