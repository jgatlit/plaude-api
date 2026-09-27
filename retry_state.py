#!/usr/bin/env python3
"""retry_state.py — bounded, backing-off retry bookkeeping for the Plaud L1 pull.

WHY THIS EXISTS
---------------
pull-to-vault.sh already retries anything it could not pull, on EVERY run — and the
timer runs every 30 minutes. That is right for a transcript still being generated
upstream and wrong for one that will never arrive. Measured 2026-09-27: the pipeline
had logged 8,331 `deferred` events across 34 recordings, 10 of them 200-1,680 times
each, the oldest re-attempted every 30 min since 2026-08-10 — and not one of them
escalated to a human. Nine of those ten report `transcript: unavailable` at Plaud
(clips of 2-5s it will not transcribe, plus one 23m recording that failed upstream).

So the retry is not missing; it is unbounded, uniform and silent. This module makes
it bounded, backing-off and loud exactly once:

    check <fid>            -> ATTEMPT | SKIP <reason>
    note  <fid> <outcome>  -> OK | ESCALATE <state> | RECOVERED
    report                 -> human-readable state of every tracked recording

Backoff is measured from the FIRST failed attempt, not the last, so a recording that
has been failing for a month cannot reset itself to hot-retry by failing again:

    age < 2h    -> every run (~30 min)   transcription normally lands in this window
    age < 24h   -> every 2h
    age < 3d    -> every 6h
    age < 14d   -> every 24h
    age >= 14d  -> still daily until the next attempt escalates it, then `stalled`:
                   one notice, thereafter a weekly probe (we never give up SILENTLY,
                   and never stop entirely — a transcript that appears on day 40 is
                   still picked up)

A recording whose metadata fetch keeps failing is a different animal — it has been
deleted or hidden upstream (observed: fb1c0c63). After GONE_STRIKES consecutive
metadata failures spanning at least 24h it is marked `gone`, escalated once, and
probed weekly.

Success always wins: a pulled recording's row is deleted, and if it had been
escalated we emit RECOVERED so the operator hears the end of the story too.

PRIVACY: this state file is keyed by recording id and carries NO titles. Plaud titles
routinely describe clinical content; ids and dates only, per the standing vault rule.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

# --- Tunables (env-overridable so a probe can exercise the schedule without waiting) --
STATE_PATH = Path(
    os.environ.get(
        "PLAUD_RETRY_STATE",
        str(Path(os.environ.get("PLAUD_VAULT_DIR", str(Path.home() / "vault/999 Inbox/Transcripts"))) / ".retry-state.json"),
    )
)
STALL_AFTER = float(os.environ.get("PLAUD_STALL_AFTER_SEC", 14 * 86400))   # -> `stalled`
GONE_STRIKES = int(os.environ.get("PLAUD_GONE_STRIKES", 3))               # consecutive meta-fails
GONE_MIN_AGE = float(os.environ.get("PLAUD_GONE_MIN_AGE_SEC", 86400))     # ...spanning at least this
COLD_PROBE = float(os.environ.get("PLAUD_COLD_PROBE_SEC", 7 * 86400))     # stalled/gone re-probe

# (max age, min interval) — first matching row wins.
SCHEDULE = [
    (2 * 3600, 0),
    (24 * 3600, 2 * 3600),
    (3 * 86400, 6 * 3600),
    (14 * 86400, 24 * 3600),
]

TERMINAL = ("stalled", "gone")


def _load() -> dict:
    try:
        with STATE_PATH.open() as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError):
        # A corrupt state file must never wedge the pipeline: start clean, keep the
        # damaged copy for forensics. Worst case we retry a few records early.
        try:
            STATE_PATH.replace(STATE_PATH.with_suffix(".json.corrupt"))
        except OSError:
            pass
        return {}


def _save(data: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".json.tmp")
    with tmp.open("w") as fh:
        json.dump(data, fh, indent=1, sort_keys=True)
        fh.write("\n")
    os.replace(tmp, STATE_PATH)  # atomic: a reader never sees a half-written file


def _interval(age: float) -> float:
    """Minimum gap between attempts for a record this old.

    Beyond the last schedule row the gap STAYS at that row's value (daily) rather than
    dropping to COLD_PROBE. That distinction matters: COLD_PROBE belongs to a record we
    have already given up on and escalated. A record that has quietly passed STALL_AFTER
    but has not been escalated yet still needs its next attempt soon, because the
    escalation is emitted BY an attempt — putting it on a weekly gap first would delay the
    operator notice from day 14 to about day 21. (Caught in testing, 2026-09-27.)
    """
    gap = 0.0
    for max_age, row_gap in SCHEDULE:
        gap = row_gap
        if age < max_age:
            return row_gap
    return gap


def _human(seconds: float) -> str:
    seconds = int(max(0, seconds))
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def cmd_check(fid: str) -> int:
    row = _load().get(fid)
    now = time.time()
    if not row:
        print("ATTEMPT first-sight")
        return 0

    first = float(row.get("first_seen", now))
    last = float(row.get("last_attempt", 0))
    age = now - first
    since = now - last
    state = row.get("state", "pending")

    gap = COLD_PROBE if state in TERMINAL else _interval(age)
    if since >= gap:
        print(f"ATTEMPT state={state} age={_human(age)} attempts={row.get('attempts', 0)}")
        return 0
    print(
        f"SKIP state={state} age={_human(age)} attempts={row.get('attempts', 0)} "
        f"next-in={_human(gap - since)}"
    )
    return 0


def cmd_note(fid: str, outcome: str) -> int:
    data = _load()
    now = time.time()
    row = data.get(fid)

    if outcome == "pulled":
        # Success clears the row. If we had cried wolf, say so — an operator who was
        # told a recording stalled is owed the resolution.
        if row and row.get("escalated"):
            age = now - float(row.get("first_seen", now))
            print(f"RECOVERED after={_human(age)} attempts={row.get('attempts', 0)}")
        else:
            print("OK cleared")
        data.pop(fid, None)
        _save(data)
        return 0

    if outcome not in ("pending", "meta-fail"):
        print(f"OK ignored-unknown-outcome={outcome}", file=sys.stderr)
        return 0

    if not row:
        row = {"first_seen": now, "attempts": 0, "meta_fails": 0, "state": "pending", "escalated": False}

    row["attempts"] = int(row.get("attempts", 0)) + 1
    row["last_attempt"] = now
    row["last_outcome"] = outcome
    row["meta_fails"] = int(row.get("meta_fails", 0)) + 1 if outcome == "meta-fail" else 0
    age = now - float(row.get("first_seen", now))

    new_state = row.get("state", "pending")
    if outcome == "meta-fail" and row["meta_fails"] >= GONE_STRIKES and age >= GONE_MIN_AGE:
        new_state = "gone"
    elif age >= STALL_AFTER:
        new_state = "stalled"
    row["state"] = new_state

    escalate = new_state in TERMINAL and not row.get("escalated")
    if escalate:
        row["escalated"] = True
        row["escalated_at"] = now

    data[fid] = row
    _save(data)

    if escalate:
        print(f"ESCALATE {new_state} age={_human(age)} attempts={row['attempts']}")
    else:
        print(f"OK state={new_state} age={_human(age)} attempts={row['attempts']}")
    return 0


def cmd_report() -> int:
    data = _load()
    if not data:
        print("retry state: empty — nothing pending")
        return 0
    now = time.time()
    rows = sorted(data.items(), key=lambda kv: float(kv[1].get("first_seen", 0)))
    buckets: dict[str, int] = {}
    for _, row in rows:
        buckets[row.get("state", "pending")] = buckets.get(row.get("state", "pending"), 0) + 1
    print(f"retry state: {len(rows)} recording(s) tracked — " + ", ".join(f"{k}={v}" for k, v in sorted(buckets.items())))
    for fid, row in rows:
        age = now - float(row.get("first_seen", now))
        print(
            f"  {fid[:8]}  {row.get('state', 'pending'):<8} age={_human(age):>4} "
            f"attempts={int(row.get('attempts', 0)):>4} "
            f"last={row.get('last_outcome', '?')}"
            + ("  [escalated]" if row.get("escalated") else "")
        )
    return 0


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__ or "", file=sys.stderr)
        print("usage: retry_state.py check <fid> | note <fid> <outcome> | report", file=sys.stderr)
        return 64
    cmd = argv[1]
    if cmd == "check" and len(argv) == 3:
        return cmd_check(argv[2])
    if cmd == "note" and len(argv) == 4:
        return cmd_note(argv[2], argv[3])
    if cmd == "report":
        return cmd_report()
    print("usage: retry_state.py check <fid> | note <fid> <outcome> | report", file=sys.stderr)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv))
