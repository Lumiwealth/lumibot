import pytest

from scripts.agent_eval_rate_pacing import InputPacer


def test_pacer_shares_window_across_instances_and_waits_without_reset(tmp_path):
    now = [100.0]
    waits = []

    def sleep(seconds):
        waits.append(seconds)
        now[0] += seconds

    path = tmp_path / "rate.jsonl"
    first = InputPacer(path, tokens_per_minute=100, clock=lambda: now[0], sleep=sleep)
    second = InputPacer(path, tokens_per_minute=100, clock=lambda: now[0], sleep=sleep)
    first.admit("actor", 60)
    second.admit("judge", 60)
    assert waits == []
    second.admit("actor", 60)
    assert waits == [60.0]
    assert len(path.read_text().splitlines()) == 3


@pytest.mark.parametrize("tokens", [None, 0, -1, True, 101])
def test_pacer_rejects_unrepresentable_input_without_waiting(tmp_path, tokens):
    pacer = InputPacer(
        tmp_path / "rate.jsonl", tokens_per_minute=100, sleep=lambda _: pytest.fail("must fail before waiting")
    )
    with pytest.raises(ValueError):
        pacer.admit("actor", tokens)


def test_counter_preserves_system_tools_and_continuation_contents(monkeypatch):
    from types import SimpleNamespace

    import requests as http
    from google.genai import types

    from scripts.agent_eval_rate_pacing import count_native_request

    requests = []
    monkeypatch.setattr(
        http,
        "post",
        lambda url, **kw: requests.append(kw) or SimpleNamespace(status_code=200, json=lambda: {"totalTokens": 42}),
    )
    config = types.GenerateContentConfig(
        system_instruction="Read the account.",
        tools=[
            types.Tool(
                function_declarations=[
                    types.FunctionDeclaration(name="account_portfolio", parameters={"type": "object"})
                ]
            )
        ],
    )
    contents = [types.Content(role="user", parts=[types.Part(text="Inspect.")])]
    assert count_native_request("gemini-fixture", SimpleNamespace(config=config, contents=contents)) == 42
    native = requests[0]["json"]["generateContentRequest"]
    assert native["contents"] == [item.model_dump(mode="json", by_alias=True, exclude_none=True) for item in contents]
    assert native["systemInstruction"]["parts"][0]["text"] == config.system_instruction
    assert native["tools"][0]["functionDeclarations"][0]["name"] == "account_portfolio"
    assert requests[0]["timeout"] == 30
