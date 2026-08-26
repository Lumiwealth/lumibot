"""Built-in, progressively loaded trading skills for LumiBot agents."""

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path
from typing import Any


BUILTIN_SKILL_NAMES = ("options-trading", "stock-trading")
BUILTIN_SKILLS_ROOT = Path(__file__).with_name("skills")


def builtin_skill_directories() -> tuple[Path, ...]:
    """Return built-in skill folders in stable catalog order."""
    return tuple(BUILTIN_SKILLS_ROOT / name for name in BUILTIN_SKILL_NAMES)


@lru_cache(maxsize=1)
def builtin_skill_fingerprint() -> str:
    """Hash every model-visible built-in skill file for cache/eval provenance."""
    digest = hashlib.sha256()
    for skill_dir in builtin_skill_directories():
        for path in sorted(skill_dir.rglob("*")):
            if not path.is_file() or path.name == "openai.yaml":
                continue
            digest.update(path.relative_to(BUILTIN_SKILLS_ROOT).as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    return digest.hexdigest()


@lru_cache(maxsize=1)
def load_builtin_skills() -> tuple[Any, ...]:
    """Load the packaged ADK skill definitions once per process."""
    try:
        from google.adk.skills import load_skill_from_dir
    except ImportError as exc:  # pragma: no cover - dependency error surfaced to users
        raise RuntimeError(
            "LumiBot agent skills require google-adk 2.1.0 or newer."
        ) from exc

    skills = []
    for skill_dir in builtin_skill_directories():
        if not (skill_dir / "SKILL.md").is_file():
            raise RuntimeError(f"Packaged LumiBot skill is missing SKILL.md: {skill_dir}")
        skills.append(load_skill_from_dir(skill_dir))
    return tuple(skills)


def build_builtin_skill_toolset() -> Any:
    """Build an isolated ADK SkillToolset for one agent run."""
    try:
        from google.adk.tools.skill_toolset import SkillToolset
    except ImportError as exc:  # pragma: no cover - dependency error surfaced to users
        raise RuntimeError(
            "LumiBot agent skills require google-adk 2.1.0 or newer."
        ) from exc
    return SkillToolset(skills=list(load_builtin_skills()))
