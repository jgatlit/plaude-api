#!/usr/bin/env python3
"""screen-and-route.py — L2 of the Plaud ingestion pipeline.

Scans ~/vault/999 Inbox/Transcripts/_raw/ for newly-pulled .md files,
classifies each by title keyword rules, moves it (with its sidecars) into
the matching category subfolder, and logs each route to .ingestion.log.

Categories (first match wins):
  Personal-Health/  — clinical, medical, health-care
  Casual/           — personal, family, social
  Learning/         — lectures, courses, training
  Business/         — strategy, project, meeting, scoping, consultation
  _unclassified/    — fallback

Sensitive categories (Personal-Health, Casual) get a frontmatter flag
`sync_blocked: true` so any future vault-sync downstream knows to skip them.

Usage: just run with no args; reads ~/vault/999 Inbox/Transcripts/_raw/.
       --dry-run to preview without moving.
"""
from __future__ import annotations
import argparse
import datetime
import pathlib
import re
import shutil
import sys

VAULT_DIR = pathlib.Path.home() / "vault/999 Inbox/Transcripts"
RAW_DIR = VAULT_DIR / "_raw"
LOG = VAULT_DIR / ".ingestion.log"

# (category, sync_blocked, regex on title). First match wins.
RULES = [
    ("Personal-Health", True,  re.compile(r"\b(clinical|doctor|medical|ultrasound|appointment|visit|patient|diagnosis|prescription|OB|obstetric|fetal|pregnancy|gynec)\b", re.I)),
    ("Casual",          True,  re.compile(r"\b(casual chat|gathering|family|personal|life plan|date night|hangout|catch[- ]up)\b", re.I)),
    ("Learning",        False, re.compile(r"\b(lecture|course|training|tutorial|webinar|workshop|class|seminar)\b", re.I)),
    ("Business",        False, re.compile(r"\b(strategy|project|meeting|scoping|consultation|discovery|business|client|proposal|onboarding|kickoff|standup|review|integration|deployment|automation|pipeline|product demo|onsultation)\b", re.I)),
]
FALLBACK = ("_unclassified", False)

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)


def log(msg: str) -> None:
    line = f"{datetime.datetime.now().isoformat(timespec='seconds')} {msg}\n"
    sys.stderr.write(line)
    with LOG.open("a") as f:
        f.write(line)


def title_of(md_path: pathlib.Path) -> str:
    text = md_path.read_text()
    m = FRONTMATTER_RE.match(text)
    if not m:
        return md_path.stem
    for line in m.group(1).splitlines():
        if line.startswith("title:"):
            return line.split(":", 1)[1].strip().strip('"')
    return md_path.stem


def classify(title: str) -> tuple[str, bool]:
    for cat, blocked, rx in RULES:
        if rx.search(title):
            return cat, blocked
    return FALLBACK


def update_frontmatter(md_path: pathlib.Path, category: str, sync_blocked: bool) -> None:
    text = md_path.read_text()
    m = FRONTMATTER_RE.match(text)
    if not m:
        return
    fm = m.group(1)
    # Replace ingestion_status
    fm = re.sub(r"^ingestion_status: .*$", f"ingestion_status: routed", fm, flags=re.M)
    # Append routing info
    extra = [f"category: {category}", f"sync_blocked: {'true' if sync_blocked else 'false'}",
             f"routed_at: {datetime.datetime.now().isoformat(timespec='seconds')}"]
    fm = fm + "\n" + "\n".join(extra)
    new_text = f"---\n{fm}\n---\n" + text[m.end():]
    md_path.write_text(new_text)


def route_one(md_path: pathlib.Path, dry_run: bool = False) -> tuple[str, bool]:
    title = title_of(md_path)
    category, sync_blocked = classify(title)
    target_dir = VAULT_DIR / category
    if dry_run:
        return category, sync_blocked
    target_dir.mkdir(parents=True, exist_ok=True)
    # Move md + sidecars
    stem = md_path.stem
    moved = []
    for sibling in md_path.parent.glob(f"{stem}*"):
        dest = target_dir / sibling.name
        shutil.move(str(sibling), str(dest))
        moved.append(sibling.name)
    # Update frontmatter on the moved .md (in new location)
    new_md = target_dir / md_path.name
    if new_md.exists():
        update_frontmatter(new_md, category, sync_blocked)
    return category, sync_blocked


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not RAW_DIR.exists():
        log(f"_raw dir does not exist: {RAW_DIR}")
        return 0

    # Only the unified .md files — exclude .summary.md sidecars and any other suffixed siblings
    candidates = sorted(
        p for p in RAW_DIR.glob("*.md")
        if not p.name.endswith(".summary.md")
    )
    if not candidates:
        log("screen batch: no candidates in _raw/")
        return 0

    log(f"screen batch: {len(candidates)} candidate(s)")
    counts: dict[str, int] = {}
    for md in candidates:
        try:
            cat, blocked = route_one(md, dry_run=args.dry_run)
            counts[cat] = counts.get(cat, 0) + 1
            verb = "would route" if args.dry_run else "routed"
            log(f"  → {verb} {md.name} → {cat}{' [SYNC-BLOCKED]' if blocked else ''}")
        except Exception as e:
            log(f"  ✗ failed {md.name}: {e!r}")
    log(f"screen batch done: {dict(counts)}")


if __name__ == "__main__":
    sys.exit(main())
