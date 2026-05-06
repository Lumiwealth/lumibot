# AI Agent Memory

Lumibot native memory is local and JSONL-based. It is available in both backtests and live trading.

Default storage:

```text
.lumibot/memory/<strategy_name>/
  memories.jsonl
  decisions.jsonl
  lessons.jsonl
  theses.jsonl
```

Override the root with:

```bash
export LUMIBOT_MEMORY_DIR=/path/to/memory
```

Strategy API:

```python
self.memory.remember("...")
self.memory.remember_decision("...", symbol="AAPL", action="buy")
self.memory.remember_lesson("...", symbol="AAPL")
self.memory.open_thesis("...", symbol="AAPL")
self.memory.search("AAPL margin")
```

Agent tools expose the same behavior so the model can store decisions, lessons, and thesis updates when useful.
