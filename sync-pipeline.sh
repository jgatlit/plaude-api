#!/usr/bin/env bash
# sync-pipeline.sh — the single entry point for plaud-sync: runs L1-L4 with per-stage
# retries, stage isolation, and one throttled operator notice when something really breaks.
#
# WHY THIS EXISTS
# ---------------
# plaud-sync.service used to list five ExecStart= lines. systemd runs those sequentially
# and ABORTS THE REST on the first non-zero exit. So a transient failure in L1 (the Plaud
# listing, a DNS blip) did not merely skip a pull — it also skipped L2 routing, the LLM
# reclassifier, L3 enrichment and the L4 vault federation push for notes that were already
# on disk and perfectly ready to process. One flaky call stalled the whole pipeline for 30
# minutes, silently.
#
# Three things change here:
#   1. ISOLATION  — every stage runs, even if an earlier one failed. Failures are collected
#                   and reported at the end rather than truncating the run.
#   2. RETRY      — each stage gets a small retry budget. Every stage is idempotent (L1
#                   dedups on a durable ledger, L2/L3 skip processed files, L4 upserts by
#                   ext_id), so a second attempt is always safe. This is what would have
#                   fixed 2026-09-18, where /source-ingest returned one HTTP 500 and a
#                   manual re-run seconds later succeeded with ingested=118.
#   3. VOICE      — a stage that still fails after its retries produces ONE Slack notice,
#                   throttled to at most one per stage per NOTIFY_GAP, plus a recovery
#                   notice when it starts working again. Previously: six unit failures in
#                   2,719 runs, zero notices.
#
# Exit codes: 0 = every stage succeeded. 2 = at least one stage failed after its retries
# (2, not 1, because the unit's SuccessExitStatus= treats 1 as success).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VAULT_DIR="${PLAUD_VAULT_DIR:-$HOME/vault/999 Inbox/Transcripts}"
LOG="${PLAUD_PIPELINE_LOG:-$VAULT_DIR/.ingestion.log}"
STATE_DIR="${PLAUD_PIPELINE_STATE_DIR:-$HOME/.local/state/plaud-sync}"
NODE_BIN="${PLAUD_NODE_BIN:-/opt/node/bin/node}"
PUSH_SCRIPT="${PLAUD_PUSH_SCRIPT:-$HOME/Documents/aiChemist.agency/teamwork-sync/ops/plaud-push.mjs}"
DAYS="${PLAUD_DAYS:-90}"

STAGE_TRIES="${PLAUD_STAGE_TRIES:-2}"      # attempts per stage
STAGE_BACKOFF="${PLAUD_STAGE_BACKOFF:-10}" # seconds before the second attempt
NOTIFY_GAP="${PLAUD_NOTIFY_GAP:-21600}"    # 6h — don't DM every 30 minutes about the same stage

mkdir -p "$STATE_DIR" "$(dirname "$LOG")" 2>/dev/null || true

log() { printf '%s\n' "$(date -Is) $*" | tee -a "$LOG" >&2; }

# Preflight. A half-finished deploy (helper copied, driver not, or vice versa) would
# otherwise die before any notification path exists, which is the one failure mode this
# whole change is meant to eliminate. Check first, and shout through whatever channel is
# actually present.
for required in _retry.sh retry_state.py pull-to-vault.sh; do
  if [[ ! -e "$HERE/$required" ]]; then
    log "FATAL: $HERE/$required is missing — refusing to run a partial pipeline"
    [[ -x "$HERE/notify.sh" ]] && "$HERE/notify.sh" "plaud-broken-install" \
      "plaud-sync cannot run: $required is missing from $HERE. The pipeline is stopped until that is fixed." || true
    exit 2
  fi
done

# shellcheck source=_retry.sh
source "$HERE/_retry.sh"

# One notice per key per NOTIFY_GAP. The stamp file is removed on success, so a stage that
# breaks, is fixed, and breaks again alerts immediately rather than waiting out the window.
notify_throttled() {
  local key="$1"; shift
  local stamp="$STATE_DIR/notified-$key"
  if [[ -f "$stamp" ]]; then
    local age=$(( $(date +%s) - $(stat -c %Y "$stamp" 2>/dev/null || echo 0) ))
    if [[ $age -lt $NOTIFY_GAP ]]; then
      log "  (notice for $key suppressed — already sent $((age / 60))m ago)"
      return 0
    fi
  fi
  : > "$stamp"
  "$HERE/notify.sh" "plaud-$key" "$@" || true
}

# Stage table: label|command. Order matters; isolation does not change it.
STAGES=(
  "L1-pull|$HERE/pull-to-vault.sh --days $DAYS"
  "L2-route|$HERE/screen-and-route.py"
  "L2b-reclassify|$HERE/llm-reclassify.py"
  "L3-enrich|$HERE/enrich-routed.py"
  "L4-push|$NODE_BIN $PUSH_SCRIPT"
)

log "pipeline start: ${#STAGES[@]} stage(s), stage-tries=$STAGE_TRIES"
failed_stages=()

for entry in "${STAGES[@]}"; do
  label="${entry%%|*}"
  cmd="${entry#*|}"
  stamp="$STATE_DIR/notified-$label"

  if retry_cmd "$label" "$STAGE_TRIES" "$STAGE_BACKOFF" -- bash -c "$cmd"; then
    # Recovery notice, but only if we had actually complained about this stage.
    if [[ -f "$stamp" ]]; then
      rm -f "$stamp"
      "$HERE/notify.sh" "plaud-$label-recovered" \
        "$label is working again — the failure I reported has cleared and the pipeline completed this stage normally." || true
      log "  ✓ $label recovered (operator notified)"
    fi
  else
    rc=$?
    failed_stages+=("$label")
    log "  ✗ $label failed after $STAGE_TRIES attempt(s) (exit $rc) — continuing with the remaining stages"
    notify_throttled "$label" \
      "$label failed after $STAGE_TRIES attempts (exit $rc). The other stages still ran. Check: journalctl --user -u plaud-sync.service -n 80"
  fi
done

if [[ ${#failed_stages[@]} -gt 0 ]]; then
  log "pipeline done with failures: ${failed_stages[*]}"
  exit 2
fi
log "pipeline done: all stages ok"
exit 0
