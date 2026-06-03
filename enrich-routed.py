#!/usr/bin/env python3
"""enrich-routed.py — L3 of the Plaud ingestion pipeline.

For each routed transcript that hasn't yet been enriched
(`ingestion_status: routed`), runs three independent gates:

  (a) Entity-mention backlinks
      Scans AI Summary + first 8KB of transcript for vault entity names from
      300 Entities/{People,Companies,Projects}. For each match, appends a
      dated backlink line under `## Recent Mentions` in the entity file.

  (b) Action-item extraction (Business + Personal-Health only)
      Parses Plan/Next-Steps bullets from AI Summary and appends rows to
      ~/vault/200 Notes/_action-items/<date>.md.

  (c) Case-study candidate hook (Business only)
      If a Business transcript mentions a Company entity AND contains
      closed-deal/delivery language ("closed", "shipped", "delivered",
      "wrapped", "live", "launched"), appends a candidate row to
      ~/vault/200 Notes/Value-Delivered/_candidates.md.

Idempotent — frontmatter `ingestion_status` flips routed → enriched. Files
with `ingestion_status: enriched` are skipped.

Usage:
  ./enrich-routed.py             # process all routed files
  ./enrich-routed.py --dry-run   # preview without writing
"""
from __future__ import annotations
import argparse
import datetime
import pathlib
import re
import sys
from typing import NamedTuple

VAULT = pathlib.Path.home() / "vault"
TRANSCRIPTS = VAULT / "999 Inbox/Transcripts"
ENTITIES_ROOT = VAULT / "300 Entities"
ENTITY_DIRS = ["People", "Companies", "Projects"]
ACTION_ITEMS_DIR = VAULT / "200 Notes/_action-items"
VDC_QUEUE = VAULT / "200 Notes/Value-Delivered/_candidates.md"
LOG = TRANSCRIPTS / ".ingestion.log"

# Buckets eligible for routing (skip _raw and any non-bucket files)
BUCKETS = ["Business", "Personal-Health", "Learning", "Casual", "_unclassified"]

# Categories that get action-item extraction
ACTION_CATS = {"Business", "Personal-Health"}

# Case-study delivery-signal phrases
DELIVERY_SIGNALS = re.compile(
    r"\b(closed|shipped|delivered|wrapped|live|launched|signed|deployed|in production|went live)\b",
    re.I,
)

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)
SECTION_RE = re.compile(r"^## (.+)$", re.M)


class Entity(NamedTuple):
    name: str        # canonical wikilink target (= filename stem)
    type: str        # People | Companies | Projects
    path: pathlib.Path
    rx: re.Pattern   # word-boundary case-insensitive regex


def log(msg: str) -> None:
    line = f"{datetime.datetime.now().isoformat(timespec='seconds')} {msg}\n"
    sys.stderr.write(line)
    with LOG.open("a") as f:
        f.write(line)


def load_entities() -> list[Entity]:
    out: list[Entity] = []
    seen_names: set[str] = set()
    for sub in ENTITY_DIRS:
        d = ENTITIES_ROOT / sub
        if not d.exists():
            continue
        for p in d.glob("*.md"):
            name = p.stem
            # Skip ambiguous super-short names (≤ 3 chars or single word ≤ 4 chars unless qualifier in parens)
            if len(name) <= 3:
                continue
            if name.lower() in seen_names:
                continue
            seen_names.add(name.lower())
            # Build word-boundary regex; escape the name; handle parenthetical qualifiers
            esc = re.escape(name)
            # If the filename has a parenthetical qualifier, ALSO accept the bare prefix
            # only if it's unique across all entities. For v1 keep it strict: full name only.
            rx = re.compile(r"\b" + esc + r"\b", re.I)
            out.append(Entity(name=name, type=sub, path=p, rx=rx))
    return out


def read_frontmatter(md_path: pathlib.Path) -> tuple[dict[str, str], str]:
    text = md_path.read_text()
    m = FRONTMATTER_RE.match(text)
    if not m:
        return {}, text
    fm_raw = m.group(1)
    fm: dict[str, str] = {}
    for line in fm_raw.splitlines():
        if ":" not in line or line.startswith("  "):
            continue
        k, _, v = line.partition(":")
        fm[k.strip()] = v.strip().strip('"')
    return fm, text


def extract_section(md_text: str, heading: str) -> str:
    """Return content under '## <heading>' until the next '## ' or EOF."""
    headings = list(SECTION_RE.finditer(md_text))
    for i, h in enumerate(headings):
        if h.group(1).strip().lower() == heading.lower():
            start = h.end()
            end = headings[i + 1].start() if i + 1 < len(headings) else len(md_text)
            return md_text[start:end].strip()
    return ""


def find_entity_mentions(text: str, entities: list[Entity]) -> list[Entity]:
    hits = []
    for ent in entities:
        if ent.rx.search(text):
            hits.append(ent)
    return hits


def append_entity_backlink(ent: Entity, transcript_md: pathlib.Path,
                            transcript_title: str, date_str: str,
                            dry_run: bool) -> None:
    if dry_run:
        return
    # Vault-relative path for the wikilink target (Obsidian default = stem)
    backlink_target = transcript_md.stem
    snippet = transcript_title
    line = f"- {date_str} — [[{backlink_target}]] — {snippet}"
    text = ent.path.read_text()
    if line in text:
        return  # idempotent
    if "## Recent Mentions" in text:
        # Insert under that heading
        text = re.sub(
            r"(## Recent Mentions\n)",
            rf"\1{line}\n",
            text,
            count=1,
        )
    else:
        # Append section at end
        if not text.endswith("\n"):
            text += "\n"
        text += f"\n## Recent Mentions\n\n{line}\n"
    ent.path.write_text(text)


def parse_action_items(summary: str) -> list[str]:
    """Extract bullets from a Plan / Next Steps / Action Items section of an AI summary."""
    items: list[str] = []
    # Look for any of these section headings (## or ### level), grab bullets
    section_patterns = [
        r"(?:^|\n)(?:##+|>?\s*##+)\s*(?:Plan|Next Steps?|Action Items?|To-?Do)\b[^\n]*\n",
        r"(?:^|\n)-\s*(?:Plan|Next Steps?|Action Items?)\s*:\s*\n",
    ]
    for pat in section_patterns:
        m = re.search(pat, summary, re.I)
        if not m:
            continue
        chunk = summary[m.end():]
        # Stop at next heading
        next_h = re.search(r"\n(?:##+|---)\s", chunk)
        if next_h:
            chunk = chunk[:next_h.start()]
        # Bullets — accept -, *, or numbered
        for bm in re.finditer(r"^[\s>]*[-*]\s+(.+)$", chunk, re.M):
            item = bm.group(1).strip()
            # Strip leading checkbox/marker if present (we re-add - [ ] when writing)
            item = re.sub(r"^\[[ xX]\]\s*", "", item).strip()
            # Filter noise:
            if "insert more" in item.lower(): continue            # placeholder lines
            if item.lower().endswith(":"): continue               # sub-section labels
            if re.fullmatch(r"@\w[\w\s-]{0,30}", item): continue  # bare "@Person" assignment headers
            if len(item) < 8: continue                            # too short to be actionable
            items.append(item)
        if items:
            break
    return items


def append_action_items(transcript_md: pathlib.Path, items: list[str],
                         recorded_date: str, dry_run: bool) -> pathlib.Path | None:
    if not items or dry_run:
        return None
    ACTION_ITEMS_DIR.mkdir(parents=True, exist_ok=True)
    target = ACTION_ITEMS_DIR / f"{recorded_date}.md"
    backlink_target = transcript_md.stem
    if not target.exists():
        target.write_text(
            "---\n"
            f"date: {recorded_date}\n"
            "type: action-items-rollup\n"
            "tags: [action-items, plaud-derived]\n"
            "---\n\n"
            f"# Action Items — {recorded_date}\n\n"
        )
    block_marker = f"## From [[{backlink_target}]]"
    text = target.read_text()
    if block_marker in text:
        return target  # idempotent — already appended for this transcript
    block = [block_marker, ""]
    for item in items:
        block.append(f"- [ ] {item}")
    block.append("")
    target.write_text(text + "\n".join(block) + "\n")
    return target


def append_vdc_candidate(transcript_md: pathlib.Path, transcript_title: str,
                          recorded_date: str, company_hits: list[Entity],
                          dry_run: bool) -> bool:
    if not company_hits or dry_run:
        return False
    VDC_QUEUE.parent.mkdir(parents=True, exist_ok=True)
    backlink_target = transcript_md.stem
    company_links = ", ".join(f"[[{c.name}]]" for c in company_hits)
    line = (
        f"- {recorded_date} — [[{backlink_target}]] — companies: {company_links} — "
        f"title: {transcript_title}"
    )
    if not VDC_QUEUE.exists():
        VDC_QUEUE.write_text(
            "---\n"
            "type: vdc-candidates-queue\n"
            "tags: [vdc, case-study-candidates, plaud-derived]\n"
            "---\n\n"
            "# Value-Delivered Candidates Queue\n\n"
            "> Auto-appended by `enrich-routed.py` when a Business-category Plaud "
            "transcript mentions a Company entity AND contains delivery-signal "
            "language. Operator triage → promote to `/case-study-extract`.\n\n"
        )
    text = VDC_QUEUE.read_text()
    if line in text:
        return False
    VDC_QUEUE.write_text(text + line + "\n")
    return True


def patch_frontmatter(md_path: pathlib.Path, entity_names: list[str],
                       action_items_target: pathlib.Path | None,
                       vdc_candidate: bool) -> None:
    text = md_path.read_text()
    m = FRONTMATTER_RE.match(text)
    if not m:
        return
    fm = m.group(1)
    fm = re.sub(r"^ingestion_status: .*$", "ingestion_status: enriched", fm, flags=re.M)
    extras = [
        f"enriched_at: {datetime.datetime.now().isoformat(timespec='seconds')}",
        f"entity_mentions: [{', '.join(entity_names)}]" if entity_names else "entity_mentions: []",
    ]
    if action_items_target:
        extras.append(f'action_items_rollup: "[[{action_items_target.stem}]]"')
    if vdc_candidate:
        extras.append("vdc_candidate: true")
    fm = fm + "\n" + "\n".join(extras)
    new_text = f"---\n{fm}\n---\n" + text[m.end():]
    md_path.write_text(new_text)


def process(md_path: pathlib.Path, entities: list[Entity], dry_run: bool) -> dict:
    fm, full_text = read_frontmatter(md_path)
    if fm.get("ingestion_status") == "enriched":
        return {"skipped": True}
    category = fm.get("category", "")
    title = fm.get("title", md_path.stem)
    recorded_at = fm.get("recorded_at", "") or fm.get("uploaded_at", "")
    recorded_date = recorded_at.split("T")[0] if recorded_at else datetime.date.today().isoformat()

    # Read content from sidecars directly — the unified .md uses `##` for top-level
    # sections (AI Summary, Outline, Transcript) but the summary file itself ALSO
    # uses `##` headings internally, which would confuse a section-bounded parser.
    stem = md_path.stem
    summary_sidecar = md_path.parent / f"{stem}.summary.md"
    transcript_sidecar = md_path.parent / f"{stem}.transcript.txt"
    summary = summary_sidecar.read_text() if summary_sidecar.exists() else ""
    transcript_section = transcript_sidecar.read_text() if transcript_sidecar.exists() else ""
    search_text = (summary + "\n\n" + transcript_section[:8000]).strip()

    # (a) Entity mentions
    hits = find_entity_mentions(search_text, entities)
    for ent in hits:
        append_entity_backlink(ent, md_path, title, recorded_date, dry_run)

    # (b) Action items
    action_target = None
    if category in ACTION_CATS:
        items = parse_action_items(summary)
        if items:
            action_target = append_action_items(md_path, items, recorded_date, dry_run)

    # (c) VDC candidate
    vdc_added = False
    if category == "Business":
        company_hits = [e for e in hits if e.type == "Companies"]
        if company_hits and DELIVERY_SIGNALS.search(summary + transcript_section[:4000]):
            vdc_added = append_vdc_candidate(md_path, title, recorded_date, company_hits, dry_run)

    # Patch frontmatter
    if not dry_run:
        patch_frontmatter(md_path, [e.name for e in hits], action_target, vdc_added)

    return {
        "skipped": False,
        "category": category,
        "entity_hits": [e.name for e in hits],
        "action_items": bool(action_target),
        "vdc_candidate": vdc_added,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    entities = load_entities()
    log(f"enrich batch: loaded {len(entities)} entities")

    candidates: list[pathlib.Path] = []
    for bucket in BUCKETS:
        d = TRANSCRIPTS / bucket
        if not d.exists():
            continue
        for p in d.glob("*.md"):
            if p.name.endswith(".summary.md"):
                continue
            candidates.append(p)

    if not candidates:
        log("enrich batch: no candidates")
        return 0

    log(f"enrich batch: {len(candidates)} candidate file(s)")
    counts = {"enriched": 0, "skipped": 0, "entity_hits": 0, "action_rollups": 0, "vdc_candidates": 0}
    for md in candidates:
        try:
            res = process(md, entities, args.dry_run)
            if res.get("skipped"):
                counts["skipped"] += 1
                continue
            counts["enriched"] += 1
            counts["entity_hits"] += len(res["entity_hits"])
            counts["action_rollups"] += 1 if res["action_items"] else 0
            counts["vdc_candidates"] += 1 if res["vdc_candidate"] else 0
            verb = "would enrich" if args.dry_run else "enriched"
            log(f"  → {verb} {md.name} cat={res['category']} hits={len(res['entity_hits'])} action_items={res['action_items']} vdc={res['vdc_candidate']}")
        except Exception as e:
            log(f"  ✗ failed {md.name}: {e!r}")
    log(f"enrich batch done: {counts}")


if __name__ == "__main__":
    sys.exit(main())
