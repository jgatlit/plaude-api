#!/usr/bin/env python3
"""seed_retry_state.py — one-off migration: build the retry-state file from the history
already recorded in .ingestion-events.jsonl.

WHY: without this, every recording that has been failing for weeks looks "first-sight" to
the new backoff gate. Two bad consequences: they would each get hot-retried every 30
minutes for another 2 hours, and their 14-day stall clock would restart from today — so the
nine recordings Plaud has never transcribed (the oldest failing since 2026-08-10) would not
be reported to the operator until mid-October. Seeding from real history means they are
correctly classified on the very first run.

Recordings that later pulled successfully (present in .pulled-ids) are skipped — their
deferrals are resolved history, not open work.

Escalation is pre-marked rather than left to fire per-recording, deliberately: ten separate
Slack DMs in one minute is a worse notification than one summary. This script prints that
summary for the caller to send once.

    python3 seed_retry_state.py            # dry run: show what would be written
    python3 seed_retry_state.py --write    # write the state file
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

VAULT = Path(os.environ.get("PLAUD_VAULT_DIR", str(Path.home() / "vault/999 Inbox/Transcripts")))
EVENTS = Path(os.environ.get("PLAUD_EVENTS", str(VAULT / ".ingestion-events.jsonl")))
LEDGER = Path(os.environ.get("PLAUD_LEDGER", str(VAULT / ".pulled-ids")))
STATE = Path(os.environ.get("PLAUD_RETRY_STATE", str(VAULT / ".retry-state.json")))
STALL_AFTER = float(os.environ.get("PLAUD_STALL_AFTER_SEC", 14 * 86400))
VALID_FID = re.compile(r"[0-9a-f]{32}")

WRITE = "--write" in sys.argv[1:]


def to_epoch(ts: str) -> float | None:
    if not ts:
        return None
    raw = str(ts).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    # Ledger timestamps are written by `date -Is` (local, offset-bearing) or naive local.
    return dt.timestamp()


def main() -> int:
    if not EVENTS.exists():
        print(f"no event ledger at {EVENTS} — nothing to seed")
        return 0

    done = set()
    if LEDGER.exists():
        done = {ln.strip() for ln in LEDGER.read_text().splitlines() if ln.strip()}

    rows: dict[str, dict] = {}
    for line in EVENTS.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ev.get("event") not in ("deferred", "error"):
            continue
        fid = str(ev.get("file_id") or "").removeprefix("of_")
        # Guard against malformed ledger rows: one historical event carries file_id "2",
        # which would otherwise be seeded as a permanently stalled "recording".
        if not VALID_FID.fullmatch(fid) or fid in done:
            continue
        ts = to_epoch(ev.get("ts") or ev.get("timestamp") or "")
        if ts is None:
            continue
        r = rows.setdefault(fid, {"first_seen": ts, "last_attempt": ts, "attempts": 0,
                                  "meta_fails": 0, "state": "pending", "escalated": False})
        r["first_seen"] = min(r["first_seen"], ts)
        r["last_attempt"] = max(r["last_attempt"], ts)
        r["attempts"] += 1
        r["last_outcome"] = "meta-fail" if ev.get("event") == "error" else "pending"

    now = time.time()
    stalled = []
    for fid, r in rows.items():
        age = now - r["first_seen"]
        if age >= STALL_AFTER:
            r["state"] = "stalled"
            r["escalated"] = True          # pre-marked: reported once, in the summary below
            r["escalated_at"] = now
            stalled.append((fid, age, r["attempts"]))

    print(f"seeding {len(rows)} unresolved recording(s) from {EVENTS.name}")
    print(f"  pending (still inside the 14-day window): {len(rows) - len(stalled)}")
    print(f"  stalled (past 14 days, pre-marked as reported): {len(stalled)}")
    for fid, age, n in sorted(stalled, key=lambda t: -t[1]):
        print(f"    {fid[:8]}  stuck {int(age // 86400)}d  {n} wasted attempt(s)")
    total_attempts = sum(r["attempts"] for r in rows.values())
    print(f"  historical attempts represented: {total_attempts}")

    if not WRITE:
        print("\n(dry run — pass --write to create the state file)")
        return 0
    if STATE.exists():
        print(f"\nREFUSING to overwrite existing {STATE} — move it aside first")
        return 1
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix(".json.tmp")
    with tmp.open("w") as fh:
        json.dump(rows, fh, indent=1, sort_keys=True)
        fh.write("\n")
    os.replace(tmp, STATE)
    print(f"\nwrote {STATE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
