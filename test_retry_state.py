#!/usr/bin/env python3
"""Exercise the backoff schedule without waiting real days: write a row, rewind its
clock, and assert what check/note decide. Run: python3 test_retry_state.py"""
import json, os, subprocess, sys, tempfile, time

TMP = tempfile.mkdtemp()
STATE = os.path.join(TMP, "retry-state.json")
ENV = {**os.environ, "PLAUD_RETRY_STATE": STATE}
FID = "abc123deadbeef"
fails = []

def run(*args):
    r = subprocess.run([sys.executable, "retry_state.py", *args], capture_output=True, text=True, env=ENV)
    return r.stdout.strip(), r.stderr.strip(), r.returncode

def check(expect_prefix, label):
    out, err, rc = run("check", FID)
    ok = rc == 0 and out.startswith(expect_prefix)
    print(f"{'PASS' if ok else 'FAIL'}  {label}: expected {expect_prefix!r}, got {out!r}")
    if not ok: fails.append(label)

def note(outcome, expect_prefix, label):
    out, err, rc = run("note", FID, outcome)
    ok = rc == 0 and out.startswith(expect_prefix)
    print(f"{'PASS' if ok else 'FAIL'}  {label}: expected {expect_prefix!r}, got {out!r}")
    if not ok: fails.append(label)

def rewind(age_sec, since_last_sec):
    """Pretend the row was first seen age_sec ago and last tried since_last_sec ago."""
    now = time.time()
    d = json.load(open(STATE))
    d[FID]["first_seen"] = now - age_sec
    d[FID]["last_attempt"] = now - since_last_sec
    json.dump(d, open(STATE, "w"))

# 1. An unknown recording is always attempted.
check("ATTEMPT", "unseen recording is attempted")

# 2. Fresh failure (<2h old) keeps hot-retrying every run — transcription usually lands here.
note("pending", "OK", "first pending recorded")
rewind(30 * 60, 60)
check("ATTEMPT", "fresh (<2h) retries every run")

# 3. Past 2h it must back off to 2-hourly: 1h since last try = skip, 3h = attempt.
rewind(6 * 3600, 3600)
check("SKIP", "6h old, tried 1h ago -> skip")
rewind(6 * 3600, 3 * 3600)
check("ATTEMPT", "6h old, tried 3h ago -> attempt")

# 4. Days-old records fall to 6-hourly, then daily.
rewind(2 * 86400, 3 * 3600)
check("SKIP", "2d old, tried 3h ago -> skip")
rewind(2 * 86400, 7 * 3600)
check("ATTEMPT", "2d old, tried 7h ago -> attempt")
rewind(5 * 86400, 10 * 3600)
check("SKIP", "5d old, tried 10h ago -> skip")
rewind(5 * 86400, 26 * 3600)
check("ATTEMPT", "5d old, tried 26h ago -> attempt")

# 5. At 14 days it escalates exactly ONCE, then goes cold (weekly probe).
rewind(15 * 86400, 26 * 3600)
note("pending", "ESCALATE stalled", "14d+ escalates once")
rewind(15 * 86400, 26 * 3600)
note("pending", "OK state=stalled", "second 14d+ failure does NOT re-escalate")
rewind(15 * 86400, 2 * 86400)
check("SKIP", "stalled, tried 2d ago -> skip (weekly probe)")
rewind(15 * 86400, 8 * 86400)
check("ATTEMPT", "stalled, tried 8d ago -> probe")

# 5b. REGRESSION (found 2026-09-27): a record past the 14-day stall threshold that has
#     NOT yet been escalated must still be attempted on the daily cadence — otherwise it
#     silently falls onto the weekly cold probe and the operator notice slips to ~day 21.
note("pulled", "RECOVERED", "clear the stalled row from step 5 (reports recovery)")
note("pending", "OK", "seed fresh row")
rewind(15 * 86400, 26 * 3600)
check("ATTEMPT", "15d old + not escalated + tried 26h ago -> attempt (not cold)")
note("pending", "ESCALATE stalled", "that attempt is what escalates it")
rewind(15 * 86400, 26 * 3600)
check("SKIP", "only AFTER escalation does it go cold (26h < weekly)")

# 6. Success clears the row and reports the recovery.
note("pulled", "RECOVERED", "success after escalation reports RECOVERED")
check("ATTEMPT", "cleared row is attempted again")

# 7. Repeated metadata failures = deleted upstream -> `gone`, escalated once.
for i in range(2):
    note("meta-fail", "OK", f"meta-fail {i+1} of 3 (too young for gone)")
rewind(2 * 86400, 0)
note("meta-fail", "ESCALATE gone", "3rd meta-fail past 24h -> gone")

# 8. A corrupt state file must not wedge the pipeline.
open(STATE, "w").write("{ this is not json")
out, err, rc = run("check", FID)
ok = rc == 0 and out.startswith("ATTEMPT")
print(f"{'PASS' if ok else 'FAIL'}  corrupt state file recovers: got {out!r}")
if not ok: fails.append("corrupt state")

# 9. report must not crash and must not leak anything but ids.
run("note", FID, "pending")
out, err, rc = run("report")
ok = rc == 0 and "tracked" in out
print(f"{'PASS' if ok else 'FAIL'}  report renders: got {out.splitlines()[0]!r}")
if not ok: fails.append("report")

print()
print(f"{'ALL TESTS PASSED' if not fails else 'FAILURES: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
