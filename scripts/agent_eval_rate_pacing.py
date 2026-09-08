"""Eval-only, process-shared input pacing. Never changes a provider/account quota."""

import fcntl
import json
import threading
import time
from pathlib import Path

_LOCK = threading.Lock()


class InputPacer:
    def __init__(self, path, *, tokens_per_minute=200_000, clock=time.time, sleep=time.sleep):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.limit = tokens_per_minute
        self.clock = clock
        self.sleep = sleep

    def admit(self, model, tokens):
        if type(tokens) is not int or tokens <= 0 or tokens > self.limit:
            raise ValueError("Input count is missing or exceeds the eval pacing window.")
        while True:
            with _LOCK, self.path.open("a+", encoding="utf-8") as stream:
                fcntl.flock(stream, fcntl.LOCK_EX)
                try:
                    stream.seek(0)
                    rows = [json.loads(line) for line in stream if line.strip()]
                    now = self.clock()
                    active = [row for row in rows if row["model"] == model and row["at"] > now - 60]
                    if sum(row["tokens"] for row in active) + tokens <= self.limit:
                        stream.seek(0, 2)
                        stream.write(json.dumps({"model": model, "tokens": tokens, "at": now}) + "\n")
                        stream.flush()
                        import os

                        os.fsync(stream.fileno())
                        return
                    delay = max(0.01, min(row["at"] for row in active) + 60 - now)
                finally:
                    fcntl.flock(stream, fcntl.LOCK_UN)
            self.sleep(min(delay, 60))


def count_native_request(model, llm_request):
    """Use the provider's count-only endpoint, with the exact system/tools/history."""
    import os
    from urllib.parse import quote

    import requests
    from google.genai import types

    config = llm_request.config
    system = config.system_instruction
    if isinstance(system, str):
        system = types.Content(parts=[types.Part(text=system)])
    native = {
        "model": "models/" + model,
        "contents": [
            content.model_dump(mode="json", by_alias=True, exclude_none=True) for content in llm_request.contents
        ],
        "tools": [tool.model_dump(mode="json", by_alias=True, exclude_none=True) for tool in config.tools or []],
    }
    if system is not None:
        native["systemInstruction"] = system.model_dump(mode="json", by_alias=True, exclude_none=True)
    # The installed SDK rejects system_instruction/tools on the Developer API
    # count helper. The documented REST generateContentRequest accepts both.
    response = requests.post(
        "https://generativelanguage.googleapis.com/v1beta/models/" + quote(model, safe="") + ":countTokens",
        headers={"x-goog-api-key": os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY", "")},
        json={"generateContentRequest": native},
        timeout=30,
    )
    if response.status_code != 200:
        raise ValueError(f"Provider token-count preflight returned HTTP {response.status_code}.")
    count = response.json().get("totalTokens")
    if type(count) is not int or count <= 0:
        raise ValueError("The provider returned no authoritative input-token count.")
    return count
