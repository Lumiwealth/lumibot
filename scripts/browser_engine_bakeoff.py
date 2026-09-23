"""Mac-only bakeoff of Patchright, Camoufox, and nodriver.

Nodriver is AGPL-3.0. This script stays on the Mac. It does not select a
hosted default and it does not start an AWS task. Profiles live under /tmp.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
import traceback
from pathlib import Path

NORMAL_URL = "https://disclosures-clerk.house.gov/PublicDisclosure/FinancialDisclosure"
HARD_URL = "https://seekingalpha.com/symbol/AAPL/earnings/transcripts"
BLOCK_MARKERS = (
    "just a moment",
    "attention required",
    "access to this page has been denied",
    "cf-browser-verification",
    "checking your browser",
    "enable javascript and cookies",
    "sorry, you have been blocked",
    "_pxappid",
    "px-captcha",
)
FINGERPRINT_JS = """() => JSON.stringify({
  title: document.title || "",
  url: location.href,
  webdriver: navigator.webdriver,
  userAgent: navigator.userAgent || "",
  languages: navigator.languages ? Array.from(navigator.languages) : [],
  plugins: navigator.plugins ? navigator.plugins.length : 0,
  text: (document.body && (document.body.innerText || document.body.textContent) || "").slice(0, 2000)
})"""
NODRIVER_FINGERPRINT_JS = """JSON.stringify({
  title: document.title || "",
  url: location.href,
  webdriver: navigator.webdriver,
  userAgent: navigator.userAgent || "",
  languages: navigator.languages ? Array.from(navigator.languages) : [],
  plugins: navigator.plugins ? navigator.plugins.length : 0,
  text: (document.body && (document.body.innerText || document.body.textContent) || "").slice(0, 2000)
})"""


def child_rss_mb(root_pid: int) -> float:
    raw = subprocess.check_output(["ps", "-ax", "-o", "pid=,ppid=,rss="], text=True)
    children: dict[int, list[tuple[int, int]]] = {}
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        pid, ppid, rss = int(parts[0]), int(parts[1]), int(parts[2])
        children.setdefault(ppid, []).append((pid, rss))
    total = 0
    stack = [root_pid]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        for child, rss in children.get(current, []):
            total += rss
            stack.append(child)
    return round(total / 1024, 1)


def classify(title: str, text: str, page_kind: str) -> str:
    blob = f"{title}\n{text}".lower()
    if any(marker in blob for marker in BLOCK_MARKERS):
        return "block"
    if page_kind == "normal":
        if "financial disclosure" in blob or "periodic transaction" in blob:
            return "pass"
        return "block"
    if "earnings call transcript" in blob or "transcripts & earnings" in blob:
        return "pass"
    return "block"


def pack(page_kind: str, payload: dict, memory_mb: float, error: str | None = None) -> dict:
    title = str(payload.get("title") or "")
    text = str(payload.get("text") or "")
    url = str(payload.get("url") or "")
    user_agent = str(payload.get("userAgent") or "")
    result = "error" if error else classify(title, text, page_kind)
    bot_fingerprint = payload.get("webdriver") is True or "headlesschrome" in user_agent.lower()
    return {
        "result": result,
        "title": title[:180],
        "final_url": url[:300],
        "webdriver": payload.get("webdriver"),
        "user_agent": user_agent[:240],
        "languages": payload.get("languages") or [],
        "plugin_count": payload.get("plugins"),
        "memory_mb": memory_mb,
        "bot_fingerprint": bot_fingerprint,
        "snippet": " ".join(text.split())[:240],
        "error": error,
    }


def run_patchright_family(engine_name: str, engine, page_kind: str, url: str, profile: Path) -> dict:
    session_id = f"{engine_name}-{page_kind}"
    try:
        engine.open(session_id=session_id, profile_dir=profile, headless=True)
        engine.navigate(session_id, url, "domcontentloaded")
        raw = engine._call(engine._page(session_id).evaluate, FINGERPRINT_JS)
        payload = json.loads(raw)
        memory = child_rss_mb(os.getpid())
        return pack(page_kind, payload, memory)
    except Exception as exc:
        return pack(page_kind, {}, child_rss_mb(os.getpid()), error=f"{type(exc).__name__}: {exc}")
    finally:
        try:
            engine.close(session_id)
        except Exception:
            pass


async def run_nodriver(page_kind: str, url: str, profile: Path) -> dict:
    import nodriver as uc

    profile.mkdir(parents=True, exist_ok=True)
    if "Google/Chrome" in str(profile) or "Application Support" in str(profile):
        raise RuntimeError("Refusing a personal Chrome profile.")
    browser = await uc.start(
        headless=True,
        user_data_dir=str(profile),
        browser_args=["--no-first-run", "--no-default-browser-check"],
    )
    try:
        page = await asyncio.wait_for(browser.get(url), timeout=40)
        await asyncio.sleep(2)
        raw = await asyncio.wait_for(
            page.evaluate(NODRIVER_FINGERPRINT_JS, return_by_value=True),
            timeout=20,
        )
        if not isinstance(raw, str):
            raise RuntimeError(f"nodriver evaluate returned {type(raw).__name__}")
        payload = json.loads(raw)
        return pack(page_kind, payload, child_rss_mb(os.getpid()))
    except Exception as exc:
        return pack(page_kind, {}, child_rss_mb(os.getpid()), error=f"{type(exc).__name__}: {exc}")
    finally:
        try:
            browser.stop()
        except Exception:
            pass


def main() -> None:
    from lumibot.components.agents.browser_tools import CamoufoxEngine, PatchrightEngine

    root = Path(tempfile.mkdtemp(prefix="lumibot-browser-bakeoff-"))
    report = {
        "date": "2026-09-22",
        "machine": "mac",
        "hosted_default": False,
        "aws_task": False,
        "nodriver_license": "AGPL-3.0",
        "profile_root": str(root),
        "pages": {
            "normal": NORMAL_URL,
            "hard": HARD_URL,
        },
        "engines": {},
    }
    engines = {
        "patchright": PatchrightEngine(),
        "camoufox": CamoufoxEngine(),
    }
    for name, engine in engines.items():
        report["engines"][name] = {}
        for kind, url in (("normal", NORMAL_URL), ("hard", HARD_URL)):
            profile = root / name / kind
            profile.mkdir(parents=True, exist_ok=True)
            report["engines"][name][kind] = run_patchright_family(name, engine, kind, url, profile)
            print(name, kind, report["engines"][name][kind]["result"], flush=True)
    report["engines"]["nodriver"] = {}
    for kind, url in (("normal", NORMAL_URL), ("hard", HARD_URL)):
        profile = root / "nodriver" / kind
        report["engines"]["nodriver"][kind] = asyncio.run(run_nodriver(kind, url, profile))
        print("nodriver", kind, report["engines"]["nodriver"][kind]["result"], flush=True)

    out = Path(__file__).resolve().parents[1] / "docs" / "research" / "2026-09-22-browser-engine-bakeoff.json"
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"WROTE {out}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
