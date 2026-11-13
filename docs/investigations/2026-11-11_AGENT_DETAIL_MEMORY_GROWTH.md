# AGENT DETAIL MEMORY GROWTH

Why long multi-agent backtests ran out of memory, and how the agent detail recorder now stays flat.

**Last Updated:** 2026-11-11
**Status:** Fixed
**Audience:** Contributors and AI agents

## Overview

A backtest that runs two AI agents on every bar for several hours was killed at its container memory limit. The process got SIGKILL, so no Python error was raised and the last log line was unrelated. The agent detail parquet uploaded after the kill was truncated.

## Cause

`AgentManager._write_detail_rows` appended every observability row (call summary, thinking, tool calls, tool results with their full payloads, usage, text, plus the prompts on each row) to `_observability_rows` and `_observability_all_rows`. These lists were never trimmed. After each call it built a DataFrame from all rows and rewrote the whole `*_agent_detail.parquet` file.

- Memory grew by the size of every past call.
- Write time per call grew with the run length.
- `write_parquet_with_logging` wrote straight to the target path, so a kill during the rewrite left a truncated file.

## Measurement

Synthetic two-agent strategy (stub runtime, no model calls), daily bars, each call returning three 300-bar tool results. Local cache only. 480 agent calls:

| | RSS growth | Growth per call | ms per call, last quarter |
|---|---|---|---|
| Before | 1,013 MB | 2.15 MB | 362 |
| After | 49 MB | 0.10 MB | 92 |

tracemalloc after the fix shows under 0.2 MB of Python growth across 200 calls; the remaining RSS growth is allocator and Arrow buffer reuse, not retained rows.

## Fix

- `_AgentDetailSpool` keeps only the current call's rows in memory. Each call is written to `<stem>.parts/NNNNNNNN.parquet`. The main file is rebuilt from the previous main file plus the new parts, one batch at a time, when the number of new calls reaches the number already combined (1, 2, 4, 8, ...), so total rebuild work is linear. `AgentManager.finalize_artifacts()` (called from `Strategy._dump_stats`) combines the rest and removes the parts folder.
- `write_parquet_with_logging` writes to a temporary file in the same folder and swaps it in with `os.replace`.

## Tests

- `tests/backtest/test_agent_long_run_memory.py`: two-agent backtest; retained memory per call stays under 100 KB (it was about 280 KB in tracemalloc before) and the final file holds every call for both agents.
- `tests/test_parquet_atomic_write.py`: a failed write keeps the previous complete file and leaves no temporary file.
