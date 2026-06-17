#!/usr/bin/env python3
"""ledger.py — structured per-record event ledger for the Plaud ingestion pipeline.

Every stage appends one JSON object per line to the events ledger on each
per-record state transition. This is the queryable, record-level audit trail
that complements the human-readable batch log (`.ingestion.log`).

  Ledger file:  ~/vault/999 Inbox/Transcripts/.ingestion-events.jsonl
                (override with $PLAUD_EVENTS)

Why this exists
---------------
`.ingestion.log` is batch-oriented free text — good for `tail`, useless for
"give me the last 6 Business records and their current paths". This ledger is
record-level and structured: one event per (record, transition), each carrying
the record's FULL asset bundle discovered from disk — never the hardcoded
"transcript + summary" assumption. Plaud "Generate" templates emit different
shapes (extra notes, .json, .vtt, formatted .md, mind-maps); we track whatever
actually landed next to the record's base name.

Two interfaces
--------------
  Python (stages import this module):
      import ledger
      ledger.emit("pulled", stage="L1-pull", file_id=fid, record=base_path,
                  title=name, category=cat, sync_blocked=False, detail={...})

  CLI / bash (L1 shell stage calls a subprocess):
      python3 ledger.py emit --event pulled --stage L1-pull \
          --file-id <id> --record /abs/<base-no-ext> --title "..." \
          [--category Business] [--sync-blocked false] [--detail '{"k":"v"}']

  Maintenance / consumption:
      python3 ledger.py reconcile          # snapshot current on-disk state of every record
      python3 ledger.py query --latest 6   # see the search endpoints below

An `emit` failure must NEVER break the pipeline — all writes are best-effort and
swallow their own exceptions (a warning goes to stderr).
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import re
import sys

SCHEMA = 1
VAULT = pathlib.Path.home() / "vault"
TRANSCRIPTS = VAULT / "999 Inbox/Transcripts"
# Where relocated/curated records typically come to rest (for reconcile scan).
SCAN_ROOTS = [TRANSCRIPTS, VAULT / "400 Resources/Transcripts"]
# Archive subfolders holding dedup/fragment junk — not active records; skipped
# by reconcile so they don't pollute "latest record" queries.
SKIP_DIRS = {"_no-transcript-fragments", "_ingestion-duplicates",
             "_inbox-duplicates-already-processed"}

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)


def events_path() -> pathlib.Path:
    override = os.environ.get("PLAUD_EVENTS")
    return pathlib.Path(override) if override else TRANSCRIPTS / ".ingestion-events.jsonl"


def now() -> str:
    """Local time with UTC offset, e.g. 2026-06-08T10:15:03-04:00."""
    return datetime.datetime.now().astimezone().isoformat(timespec="seconds")


def vault_rel(p: pathlib.Path) -> str:
    p = pathlib.Path(p)
    try:
        return str(p.relative_to(VAULT))
    except ValueError:
        return str(p)


# --- Asset model --------------------------------------------------------------
# Anchoring principle (per Plaud API): the ONLY deterministic primitives Plaud
# exposes per recording are the 32-hex `id` (→ `short_id` = first 8) and the
# content *surfaces* it reports available — `audio`, `transcript`, `summary`
# (the AI note / auto_sum_note, whose CONTENT shape varies by Generate template).
# Plaud has NO slug; our `<date>--<slug>--<shortid>` filename slug is synthesized
# locally and is NOT a reliable key. Therefore:
#   * record identity is the file_id / short_id, never the slug;
#   * asset *discovery* globs by the short_id token, so any file the template
#     emitted under that id is captured — we never enumerate expected extensions;
#   * asset *labelling* prefers the Plaud surface (mapped from how L1 pulled it);
#     `kind_for` is only a local fallback heuristic for files we didn't pull
#     ourselves (e.g. manually-added, non-Plaud sidecars).
SHORT_ID_RE = re.compile(r"(?<![0-9a-f])([0-9a-f]{8})(?![0-9a-f])")

# Map our L1 pull-output suffixes → the Plaud content surface they came from.
# This is authoritative provenance (we know what we fetched); extensions below
# are only consulted for files of unknown origin.
PLAUD_SURFACE_BY_SUFFIX = [
    (".transcript.txt", "transcript"),   # plaud transcript  (prose, speaker-attributed)
    (".transcript.md",  "transcript"),
    (".summary.md",     "summary"),      # plaud summary / get_note  (auto_sum_note)
    (".summary.json",   "summary"),
]
AUDIO_EXTS = {"mp3", "wav", "m4a", "aac", "ogg", "opus"}


def surface_for(name: str) -> str | None:
    """The Plaud content surface this file represents, or None if not a known
    Plaud pull output (template-specific or non-Plaud files return None)."""
    n = name.lower()
    for suffix, surface in PLAUD_SURFACE_BY_SUFFIX:
        if n.endswith(suffix):
            return surface
    ext = n.rsplit(".", 1)[-1] if "." in n else ""
    if ext in AUDIO_EXTS:
        return "audio"
    # The unified note we assemble at L1 (base `<...>.md`, no special suffix).
    if n.endswith(".md"):
        return "note-assembled"
    return None


def kind_for(name: str) -> str:
    """Local fallback label for files whose Plaud surface is unknown. NOT used
    as the source of truth — `surface_for` is, when it returns non-None."""
    n = name.lower()
    ext = n.rsplit(".", 1)[-1] if "." in n else ""
    if ext in AUDIO_EXTS:
        return "audio"
    if ext in {"png", "jpg", "jpeg", "gif", "webp", "pdf"}:
        return "attachment"
    if ext == "json":
        return "structured"
    if ext in {"vtt", "srt"}:
        return "captions"
    if ext == "txt":
        return "text"
    if ext == "md":
        return "note"
    return ext or "other"


def base_prefix(record) -> tuple[pathlib.Path, str]:
    """Return (parent_dir, base_name) for a record reference.

    Accepts either the record's base path with no extension
    (e.g. .../_raw/2026-06-02--slug--3fe92965) or its unified note .md path
    (e.g. .../Business/2026-06-02 Plaud - Foo.md). The base name is the prefix
    that all of the record's sibling assets share.
    """
    p = pathlib.Path(record)
    if p.suffix == ".md" and not p.name.endswith(".summary.md"):
        return p.parent, p.name[:-3]  # strip ".md"
    return p.parent, p.name


def short_id_in(base_name: str) -> str | None:
    """Recover the 8-hex Plaud short_id embedded in a pipeline filename, if any."""
    hits = SHORT_ID_RE.findall(base_name)
    return hits[-1] if hits else None  # the id is the last hex token in our scheme


def discover_assets(record, file_id: str | None = None) -> list[dict]:
    """Every on-disk asset belonging to this record, found by the DETERMINISTIC
    id token (not by enumerating expected extensions). Whatever a Generate
    template produced under the record's short_id is captured. Each asset is
    labelled with its Plaud `surface` (authoritative when known) and a fallback
    `kind`."""
    parent, prefix = base_prefix(record)
    if not parent.exists():
        return []
    sid = (file_id[:8] if file_id else None) or short_id_in(prefix)
    matches: dict[str, pathlib.Path] = {}
    # Primary: glob by the deterministic id token (survives slug edits).
    if sid:
        for p in parent.glob(f"*{sid}*"):
            matches[p.name] = p
    # Fallback / union: files sharing the exact base name (covers post-rename
    # bundles where the id token was stripped from the human-readable filename).
    for p in parent.glob(prefix + "*"):
        matches[p.name] = p
    out: list[dict] = []
    for name in sorted(matches):
        p = matches[name]
        if not p.is_file():
            continue
        try:
            size = p.stat().st_size
        except OSError:
            size = None
        out.append({
            "file": name,
            "surface": surface_for(name),
            "kind": kind_for(name),
            "bytes": size,
        })
    return out


def current_dir(record) -> str:
    parent, _ = base_prefix(record)
    return vault_rel(parent)


def short_id_of(file_id: str | None, record) -> str | None:
    if file_id:
        return file_id[:8]
    # Recover the 8-hex id baked into the pipeline filename (deterministic token).
    _, prefix = base_prefix(record)
    return short_id_in(prefix)


# --- Emit ---------------------------------------------------------------------
def emit(event: str, *, stage: str, record, file_id: str | None = None,
         title: str | None = None, category: str | None = None,
         sync_blocked=None, entities: list[str] | None = None,
         recorded_at: str | None = None,
         detail: dict | None = None, assets: list[dict] | None = None) -> None:
    """Append one event line. Best-effort: never raises into the caller.

    `recorded_at` (the recording's own timestamp) is captured on EVERY event so
    queries can rank records by true recency — the event `ts` is the wall-clock
    of the transition and is identical across a reconcile batch, so it can't."""
    try:
        # Resolve recorded_at deterministically from the record itself when the
        # caller didn't pass it (downstream stages have the .md path to read).
        rec_at = recorded_at or (detail or {}).get("recorded_at")
        if not rec_at:
            p = pathlib.Path(record)
            if p.suffix == ".md" and p.exists():
                rec_at = _read_frontmatter(p).get("recorded_at")
        rec = {
            "schema": SCHEMA,
            "ts": now(),
            "event": event,
            "stage": stage,
            "file_id": file_id,
            "short_id": short_id_of(file_id, record),
            "record": base_prefix(record)[1],
            "title": title,
            "category": category,
            "sync_blocked": _as_bool(sync_blocked),
            "recorded_at": rec_at,
            "dir": current_dir(record),
            "assets": assets if assets is not None else discover_assets(record, file_id),
        }
        if entities is not None:
            rec["entities"] = entities
        if detail:
            rec["detail"] = detail
        path = events_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:  # logging must never break ingestion
        sys.stderr.write(f"[ledger] emit failed ({event}/{stage}): {e!r}\n")


def _as_bool(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"true", "1", "yes"}


# --- Reconcile (snapshot current on-disk state) -------------------------------
def _read_frontmatter(md_path: pathlib.Path) -> dict:
    try:
        text = md_path.read_text()
    except Exception:
        return {}
    m = FRONTMATTER_RE.match(text)
    if not m:
        return {}
    fm: dict[str, str] = {}
    for line in m.group(1).splitlines():
        if ":" not in line or line.startswith("  "):
            continue
        k, _, v = line.partition(":")
        fm[k.strip()] = v.strip().strip('"')
    return fm


def _is_record_note(md_path: pathlib.Path, fm: dict) -> bool:
    if md_path.name.endswith(".summary.md"):
        return False
    # Plaud pipeline record, or any transcript-tagged note we can index.
    if fm.get("source") == "plaud" or fm.get("plaud_file_id"):
        return True
    tags = fm.get("tags", "")
    return "transcript" in tags or "plaud" in tags


def reconcile() -> int:
    """Emit one `observed` event per record reflecting its CURRENT path + assets.
    Closes the outbound blind spot: records relocated out of the inbox (which
    no stage logs) get their live location captured here."""
    count = 0
    seen: set[str] = set()
    for root in SCAN_ROOTS:
        if not root.exists():
            continue
        for md in sorted(root.rglob("*.md")):
            if md.name.endswith(".summary.md"):
                continue
            if SKIP_DIRS & set(part for part in md.parts):
                continue  # dedup/fragment archive — not an active record
            fm = _read_frontmatter(md)
            if not _is_record_note(md, fm):
                continue
            fid = fm.get("plaud_file_id")
            key = fid or str(md)
            if key in seen:
                continue
            seen.add(key)
            entities = _parse_list(fm.get("entity_mentions", ""))
            emit(
                "observed",
                stage="reconcile",
                record=md,
                file_id=fid,
                title=fm.get("title"),
                category=fm.get("category"),
                sync_blocked=fm.get("sync_blocked"),
                entities=entities,
                recorded_at=fm.get("recorded_at"),
                detail={
                    "ingestion_status": fm.get("ingestion_status"),
                    "recorded_at": fm.get("recorded_at"),
                    "duration_human": fm.get("duration_human"),
                    "source": fm.get("source", "unknown"),
                },
            )
            count += 1
    sys.stderr.write(f"[ledger] reconcile: emitted {count} observed event(s)\n")
    return count


def _parse_list(v: str) -> list[str]:
    v = (v or "").strip()
    if v.startswith("[") and v.endswith("]"):
        v = v[1:-1]
    return [x.strip().strip('"').strip("[]") for x in v.split(",") if x.strip()]


# --- Query (powers the handoff search endpoints) ------------------------------
def _load_events() -> list[dict]:
    path = events_path()
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def _latest_per_record(events: list[dict]) -> list[dict]:
    """Collapse to the most recent event per record (by file_id or record name)."""
    latest: dict[str, dict] = {}
    for e in events:
        key = e.get("file_id") or e.get("record") or ""
        latest[key] = e  # events are append-order; last wins
    return list(latest.values())


def query(args) -> int:
    events = _load_events()
    if args.collapse:
        events = _latest_per_record(events)

    def keep(e: dict) -> bool:
        if args.event and e.get("event") != args.event:
            return False
        if args.stage and e.get("stage") != args.stage:
            return False
        if args.category and (e.get("category") or "").lower() != args.category.lower():
            return False
        if args.id and not ((e.get("file_id") or "").startswith(args.id)
                            or (e.get("short_id") or "") == args.id):
            return False
        if args.title and args.title.lower() not in (e.get("title") or "").lower():
            return False
        if args.attendee:
            ents = [x.lower() for x in e.get("entities", [])]
            if not any(args.attendee.lower() in x for x in ents):
                return False
        if args.sync_blocked is not None and bool(e.get("sync_blocked")) != args.sync_blocked:
            return False
        # "By date" means the RECORDING date, not the event wall-clock (which is
        # uniform across a reconcile batch). Fall back to ts only if unknown.
        day = (e.get("recorded_at") or e.get("ts") or "")[:10]
        if args.since and day and day < args.since:
            return False
        if args.until and day and day > args.until:
            return False
        return True

    rows = [e for e in events if keep(e)]
    # Rank by record recency (recorded_at) by default — the event `ts` is the
    # transition wall-clock and is uniform across a reconcile batch, so it can't
    # order records. Fall back to ts when recorded_at is absent.
    if args.sort == "ts":
        rows.sort(key=lambda e: e.get("ts") or "")
    else:
        rows.sort(key=lambda e: (e.get("recorded_at") or e.get("ts") or ""))
    if args.latest:
        rows = rows[-args.latest:]

    if args.json:
        for e in rows:
            print(json.dumps(e, ensure_ascii=False))
        return 0
    if args.paths:
        for e in rows:
            d = e.get("dir", "")
            for a in e.get("assets", []):
                print(f"{d}/{a['file']}")
        return 0
    for e in rows:
        blocked = " [SYNC-BLOCKED]" if e.get("sync_blocked") else ""
        cat = e.get("category") or "-"
        n_assets = len(e.get("assets", []))
        print(f"{e.get('ts','')}  {e.get('event',''):11} {cat:16} "
              f"assets={n_assets}  {e.get('title') or e.get('record')}{blocked}")
        if args.show_assets:
            for a in e.get("assets", []):
                label = a.get("surface") or a.get("kind") or "?"
                print(f"      - {label:18} {e.get('dir','')}/{a['file']}")
    if not rows:
        sys.stderr.write("[ledger] query: no matching events\n")
    return 0


# --- CLI ----------------------------------------------------------------------
def _cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Plaud ingestion event ledger")
    sub = parser.add_subparsers(dest="cmd", required=True)

    pe = sub.add_parser("emit", help="append one event")
    pe.add_argument("--event", required=True)
    pe.add_argument("--stage", required=True)
    pe.add_argument("--record", required=True, help="record base path (no ext) or its .md path")
    pe.add_argument("--file-id")
    pe.add_argument("--title")
    pe.add_argument("--category")
    pe.add_argument("--sync-blocked")
    pe.add_argument("--recorded-at")
    pe.add_argument("--entities", help="comma-separated")
    pe.add_argument("--detail", help="JSON object string")

    sub.add_parser("reconcile", help="snapshot current on-disk state of every record")

    pq = sub.add_parser("query", help="search the ledger")
    pq.add_argument("--event")
    pq.add_argument("--stage")
    pq.add_argument("--category")
    pq.add_argument("--id", help="full or 8-char file id")
    pq.add_argument("--title", help="substring match")
    pq.add_argument("--attendee", help="entity/attendee substring match")
    pq.add_argument("--since", help="YYYY-MM-DD (inclusive)")
    pq.add_argument("--until", help="YYYY-MM-DD (inclusive)")
    pq.add_argument("--latest", type=int, help="keep only the N most recent matches")
    pq.add_argument("--sort", choices=["recorded_at", "ts"], default="recorded_at",
                    help="rank by recording time (default) or event wall-clock")
    pq.add_argument("--sync-blocked", dest="sync_blocked", action="store_const", const=True)
    pq.add_argument("--public", dest="sync_blocked", action="store_const", const=False)
    pq.add_argument("--collapse", action="store_true", help="latest event per record only")
    pq.add_argument("--paths", action="store_true", help="print current asset paths")
    pq.add_argument("--show-assets", action="store_true")
    pq.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)
    if args.cmd == "emit":
        emit(
            args.event,
            stage=args.stage,
            record=args.record,
            file_id=args.file_id,
            title=args.title,
            category=args.category,
            sync_blocked=args.sync_blocked,
            recorded_at=args.recorded_at,
            entities=_parse_list(args.entities) if args.entities else None,
            detail=json.loads(args.detail) if args.detail else None,
        )
        return 0
    if args.cmd == "reconcile":
        reconcile()
        return 0
    if args.cmd == "query":
        return query(args)
    return 1


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
