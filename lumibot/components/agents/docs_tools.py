from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Any


_REPO_ROOT = Path(__file__).resolve().parents[3]
_DOC_SEARCH_ROOTS = (
    _REPO_ROOT / "README.md",
    _REPO_ROOT / "docs",
    _REPO_ROOT / "docsrc",
)
_DOC_SUFFIXES = {".md", ".rst", ".txt"}
_MAX_FILE_BYTES = 2_000_000
_SNIPPET_CHARS = 500


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _title_for_text(path: Path, text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip().strip("#").strip()
        if stripped:
            return stripped[:120]
    return path.name


def _iter_doc_files() -> tuple[Path, ...]:
    files: list[Path] = []
    for root in _DOC_SEARCH_ROOTS:
        if not root.exists():
            continue
        if root.is_file():
            files.append(root)
            continue
        for candidate in sorted(root.rglob("*")):
            if not candidate.is_file():
                continue
            if candidate.suffix.lower() not in _DOC_SUFFIXES:
                continue
            if any(part.startswith(".") for part in candidate.parts):
                continue
            if "_build" in candidate.parts:
                continue
            try:
                if candidate.stat().st_size > _MAX_FILE_BYTES:
                    continue
            except OSError:
                continue
            files.append(candidate)
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in files:
        resolved = str(candidate.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(candidate)
    return tuple(deduped)


@lru_cache(maxsize=1)
def _docs_corpus() -> tuple[dict[str, Any], ...]:
    corpus: list[dict[str, Any]] = []
    for path in _iter_doc_files():
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            continue
        corpus.append(
            {
                "path": path,
                "relative_path": path.resolve().relative_to(_REPO_ROOT.resolve()).as_posix(),
                "title": _title_for_text(path, text),
                "text": text,
                "normalized": text.lower(),
            }
        )
    return tuple(corpus)


def _query_terms(query: str) -> tuple[str, list[str]]:
    normalized_query = _normalize_space(query).lower()
    tokens = [
        token
        for token in re.findall(r"[A-Za-z0-9_./-]+", normalized_query)
        if len(token) >= 2
    ]
    if not normalized_query or not tokens:
        raise ValueError("docs_search requires a non-empty query with at least one meaningful term.")
    return normalized_query, tokens


def _snippet(text: str, index: int, *, width: int = _SNIPPET_CHARS) -> str:
    start = max(0, index - width // 2)
    end = min(len(text), start + width)
    excerpt = text[start:end]
    return _normalize_space(excerpt)


def search_lumibot_docs(*, query: str, max_results: int = 5) -> dict[str, Any]:
    normalized_query, tokens = _query_terms(query)
    if max_results <= 0:
        raise ValueError("docs_search max_results must be greater than 0.")

    matches: list[dict[str, Any]] = []
    for entry in _docs_corpus():
        normalized = entry["normalized"]
        phrase_hits = normalized.count(normalized_query)
        token_hits = {token: normalized.count(token) for token in tokens if token in normalized}
        if phrase_hits <= 0 and not token_hits:
            continue
        first_index = normalized.find(normalized_query)
        if first_index < 0:
            first_index = min(normalized.find(token) for token in token_hits)
        score = (phrase_hits * 100) + (len(token_hits) * 10) + min(sum(token_hits.values()), 50)
        matches.append(
            {
                "path": entry["relative_path"],
                "title": entry["title"],
                "score": score,
                "matched_terms": sorted(token_hits.keys()),
                "snippet": _snippet(entry["text"], first_index),
            }
        )

    matches.sort(key=lambda item: (-int(item["score"]), str(item["path"])))
    limited = matches[: max_results or 5]
    return {
        "query": query,
        "result_count": len(limited),
        "results": limited,
        "searched_documents": len(_docs_corpus()),
    }
