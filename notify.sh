#!/usr/bin/env bash
# notify.sh — one-shot operator escalation for the Plaud pipeline. Best-effort by design:
# it must NEVER fail its caller, so every path ends in success.
#
# WHY THIS EXISTS
# ---------------
# Measured 2026-09-19 and again 2026-09-27: plaud-sync had NO failure notification of any
# kind. Six unit failures in 2,719 runs and 8,331 silent retries produced zero operator
# notices, while sibling gmail-sync posted 11 over the same period. Verified 2026-09-27
# that the xt12 assistant bot token is live (auth.test ok as xt12assistant in noboxAI, DM
# channel configured), so Slack is a real channel here rather than an aspiration.
#
# PRIVACY CONTRACT: callers pass recording IDS, DATES and DURATIONS only. Plaud titles
# routinely describe clinical content (prenatal visits, SOAP notes) and must never leave
# the machine this way. This script never reads a title or a transcript.
#
#   notify.sh <dedupe-key> <message...>
#
# Every notice is also appended to a local log, so an escalation survives a Slack outage
# and can be reconciled later.
set -uo pipefail

KEY="${1:-plaud-sync}"
shift || true
MSG="${*:-(no message)}"
[[ -z "${MSG// }" ]] && MSG="(no message)"

VAULT_DIR="${PLAUD_VAULT_DIR:-$HOME/vault/999 Inbox/Transcripts}"
NOTICE_LOG="${PLAUD_NOTICE_LOG:-$VAULT_DIR/.operator-notices.log}"
SLACK_LIB="${PLAUD_SLACK_LIB:-$HOME/apps/afk-engine/scripts}"

# 1. Durable local record first — this is the copy that cannot be lost to a network fault.
mkdir -p "$(dirname "$NOTICE_LOG")" 2>/dev/null || true
printf '%s\t%s\t%s\n' "$(date -Is)" "$KEY" "$MSG" >> "$NOTICE_LOG" 2>/dev/null || true

# 2. Then try Slack. slack_notify carries its own 429 retry budget and its own fallback
#    spool, so we neither reimplement retries nor care if it degrades.
if [[ -f "$SLACK_LIB/slack_notify.py" ]]; then
  PLAUD_NOTIFY_KEY="$KEY" PLAUD_NOTIFY_MSG="[plaud-sync] $MSG" \
  python3 - "$SLACK_LIB" <<'PY' >/dev/null 2>&1 || true
import os, sys
sys.path.insert(0, sys.argv[1])
try:
    from slack_notify import send_dm
    send_dm(os.environ["PLAUD_NOTIFY_MSG"], batch_id=os.environ["PLAUD_NOTIFY_KEY"])
except Exception:
    pass
PY
fi

exit 0
