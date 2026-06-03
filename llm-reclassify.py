#!/usr/bin/env python3
"""llm-reclassify.py — LLM-based reclassifier for the _unclassified/ bucket.

When the keyword rules in screen-and-route.py don't match, transcripts land
in _unclassified/. This script asks Claude Haiku 4.5 to read the title + AI
summary excerpt and pick a category, then moves the file using the same
sidecar-bundling logic as L2.

Uses prompt caching on the system prompt — the per-call delta is just the
title + 1500-char summary excerpt, so batches stay cheap (~$0.0003/file).

Runs ONLY on _unclassified/ — never re-classifies already-routed files. Safe
to run repeatedly; idempotent (no _unclassified left = no calls made).

Requires ANTHROPIC_API_KEY in the environment. If unset, exits silently
(logs a notice) so the upstream pipeline isn't blocked.

Usage:
  ./llm-reclassify.py             # process all _unclassified
  ./llm-reclassify.py --dry-run   # call LLM but don't move files
  ./llm-reclassify.py --max 10    # cap at N files this run
"""
from __future__ import annotations
import argparse
import datetime
import json
import os
import pathlib
import re
import shutil
import sys

VAULT = pathlib.Path.home() / "vault"
TRANSCRIPTS = VAULT / "999 Inbox/Transcripts"
UNCLASSIFIED = TRANSCRIPTS / "_unclassified"
LOG = TRANSCRIPTS / ".ingestion.log"

MODEL = "claude-haiku-4-5-20251001"

# Categories the LLM may choose. Mirrors screen-and-route.py.
VALID_CATEGORIES = ["Personal-Health", "Casual", "Learning", "Business"]
SYNC_BLOCKED = {"Personal-Health", "Casual"}

SYSTEM_PROMPT = """You are a classifier for personal voice-recording transcripts being routed into a personal Obsidian vault.

Pick exactly ONE category for each recording based on the title and a short summary excerpt:

- **Personal-Health** — anything medical/clinical: doctor visits, ultrasounds, OB/GYN appointments, prescriptions, lab results, mental-health sessions, dental, fitness consultations with a medical professional.
- **Casual** — informal personal conversations: family chats, social hangouts, life-plan discussions with friends/partner, casual meals, date-night reflections, NON-business catch-ups.
- **Learning** — lectures, courses, training, tutorials, webinars, workshops, conference talks, podcast-style deep dives, recorded study sessions.
- **Business** — anything work-related: client calls, internal team meetings, strategy sessions, project scoping, sales pitches, board discussions, vendor evaluations, product demos, partner conversations, business-focused brainstorming.

Decision rules:
1. If a recording contains both business and casual elements but the dominant subject is work outcomes, classify Business.
2. If it contains health information AND business (e.g. a business call where someone mentions an illness in passing), classify by the dominant subject — usually Business.
3. If you genuinely cannot tell from the title + excerpt, default to Business (it's the safest fallback — never causes sync-blocking).
4. Never invent a fifth category.

Examples:
- Title: "Catch-up coffee with Sara on baby names" — **Casual**
- Title: "Q3 revenue review with Maria and the SDR team" — **Business**
- Title: "Andrew Huberman interview clip on sleep" — **Learning**
- Title: "OB visit — 28-week growth scan" — **Personal-Health**
- Title: "Recording 2026-05-12 14:33:21" — Default to **Business** unless the summary excerpt contradicts.

Output format: a single JSON object on one line, nothing else.
Schema: {"category": "<one of: Personal-Health|Casual|Learning|Business>", "confidence": <0.0-1.0>, "reason": "<one short sentence>"}"""

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)


def log(msg: str) -> None:
    line = f"{datetime.datetime.now().isoformat(timespec='seconds')} {msg}\n"
    sys.stderr.write(line)
    with LOG.open("a") as f:
        f.write(line)


def get_client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    import anthropic
    return anthropic.Anthropic(api_key=api_key)


def read_md(md_path: pathlib.Path) -> tuple[dict, str]:
    """Return (frontmatter dict, body text)."""
    text = md_path.read_text()
    m = FRONTMATTER_RE.match(text)
    if not m:
        return {}, text
    fm: dict[str, str] = {}
    for line in m.group(1).splitlines():
        if ":" not in line or line.startswith("  "):
            continue
        k, _, v = line.partition(":")
        fm[k.strip()] = v.strip().strip('"')
    return fm, text


def read_summary_excerpt(md_path: pathlib.Path, max_chars: int = 1500) -> str:
    """Read the .summary.md sidecar; truncate for cost control."""
    sidecar = md_path.parent / f"{md_path.stem}.summary.md"
    if not sidecar.exists():
        return ""
    text = sidecar.read_text()
    return text[:max_chars]


def classify(client, title: str, summary_excerpt: str) -> dict:
    """One LLM call. Returns {category, confidence, reason}."""
    user_msg = f"Title: {title}\n\nSummary excerpt:\n{summary_excerpt or '(no summary available)'}"
    resp = client.messages.create(
        model=MODEL,
        max_tokens=200,
        system=[
            {"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}
        ],
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = resp.content[0].text.strip()
    # Strip any code fences
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.M).strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        # Last-ditch: look for the category string in the response
        for cat in VALID_CATEGORIES:
            if cat in raw:
                parsed = {"category": cat, "confidence": 0.5, "reason": "JSON parse fallback"}
                break
        else:
            parsed = {"category": "Business", "confidence": 0.0, "reason": f"unparseable: {raw[:80]}"}
    # Telemetry on cache hits
    cache_read = getattr(resp.usage, "cache_read_input_tokens", 0) or 0
    cache_create = getattr(resp.usage, "cache_creation_input_tokens", 0) or 0
    parsed["_cache_read"] = cache_read
    parsed["_cache_create"] = cache_create
    return parsed


def move_to_category(md_path: pathlib.Path, category: str, confidence: float,
                     reason: str, dry_run: bool) -> pathlib.Path | None:
    target_dir = TRANSCRIPTS / category
    if dry_run:
        return None
    target_dir.mkdir(parents=True, exist_ok=True)
    stem = md_path.stem
    for sibling in md_path.parent.glob(f"{stem}*"):
        shutil.move(str(sibling), str(target_dir / sibling.name))
    new_md = target_dir / md_path.name
    # Update frontmatter
    text = new_md.read_text()
    m = FRONTMATTER_RE.match(text)
    if m:
        fm = m.group(1)
        fm = re.sub(r"^category: .*$", f"category: {category}", fm, flags=re.M)
        sync_blocked = "true" if category in SYNC_BLOCKED else "false"
        fm = re.sub(r"^sync_blocked: .*$", f"sync_blocked: {sync_blocked}", fm, flags=re.M)
        # Append LLM provenance
        fm += "\n" + "\n".join([
            f"llm_reclassified: true",
            f"llm_model: {MODEL}",
            f"llm_confidence: {confidence}",
            f'llm_reason: "{reason.replace(chr(34), chr(39))}"',
            f"llm_reclassified_at: {datetime.datetime.now().isoformat(timespec='seconds')}",
        ])
        text = f"---\n{fm}\n---\n" + text[m.end():]
        new_md.write_text(text)
    return new_md


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max", type=int, default=50, help="Max files this run")
    args = parser.parse_args()

    if not UNCLASSIFIED.exists():
        log("llm reclassify: _unclassified/ does not exist — nothing to do")
        return 0

    candidates = sorted(
        p for p in UNCLASSIFIED.glob("*.md")
        if not p.name.endswith(".summary.md")
    )[: args.max]

    if not candidates:
        log("llm reclassify: no candidates in _unclassified/")
        return 0

    client = get_client()
    if not client:
        log("llm reclassify: ANTHROPIC_API_KEY not set — skipping (set in ~/.config/plaud-sync/env)")
        return 0

    log(f"llm reclassify: {len(candidates)} candidate(s), model={MODEL}")
    counts: dict[str, int] = {}
    total_cache_read = 0
    total_cache_create = 0
    for md in candidates:
        try:
            fm, _ = read_md(md)
            title = fm.get("title", md.stem)
            excerpt = read_summary_excerpt(md)
            result = classify(client, title, excerpt)
            cat = result["category"]
            conf = float(result.get("confidence", 0.0))
            reason = str(result.get("reason", ""))[:200]
            total_cache_read += result.get("_cache_read", 0)
            total_cache_create += result.get("_cache_create", 0)
            counts[cat] = counts.get(cat, 0) + 1
            verb = "would route" if args.dry_run else "routed"
            log(f"  → {verb} {md.name} → {cat} (conf={conf:.2f}) — {reason}")
            move_to_category(md, cat, conf, reason, args.dry_run)
        except Exception as e:
            log(f"  ✗ failed {md.name}: {e!r}")
    log(f"llm reclassify done: {counts} cache_read={total_cache_read} cache_create={total_cache_create}")


if __name__ == "__main__":
    sys.exit(main())
