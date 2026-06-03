#!/usr/bin/env bash
# pull-to-vault.sh — Standard Plaud → Vault pull (L1 of the ingestion pipeline)
#
# Pulls transcript + AI summary for each recent (or specified) Plaud recording
# into the vault inbox as a unified markdown artifact. Idempotent — files
# already pulled (by file_id) are skipped.
#
# Output layout:
#   ~/vault/999 Inbox/Transcripts/_raw/<date>--<slug>--<short-id>.md
#       frontmatter + ## AI Summary + ## Transcript
#   ~/vault/999 Inbox/Transcripts/_raw/<date>--<slug>--<short-id>.transcript.txt   (CLI raw)
#   ~/vault/999 Inbox/Transcripts/_raw/<date>--<slug>--<short-id>.summary.md       (CLI raw)
#
# Usage:
#   ./pull-to-vault.sh                  # last 1 day (default)
#   ./pull-to-vault.sh --days 7         # last N days
#   ./pull-to-vault.sh <file_id> ...    # specific IDs
#
# Requires: @plaud-ai/cli authenticated (~/.plaud/tokens.json), python3.
set -euo pipefail

VAULT_DIR="${PLAUD_VAULT_DIR:-$HOME/vault/999 Inbox/Transcripts}"
RAW_DIR="$VAULT_DIR/_raw"
PLAUD="${PLAUD_CLI:-npx -y @plaud-ai/cli@latest}"
LOG="$VAULT_DIR/.ingestion.log"
# Durable dedup ledger: full file_id per line. Survives downstream moves/renames
# out of VAULT_DIR (the filename scan below only sees the inbox tree). This is the
# authoritative skip gate that makes a wide discovery window (--days 90) safe.
LEDGER="${PLAUD_LEDGER:-$VAULT_DIR/.pulled-ids}"
DRY_RUN="${DRY_RUN:-0}"

mkdir -p "$RAW_DIR"

log() { printf '%s\n' "$(date -Is) $*" | tee -a "$LOG" >&2; }

# --- Resolve target file_ids ---------------------------------------------------
ids=()
days=1
if [[ $# -gt 0 ]]; then
  if [[ "$1" == "--days" ]]; then
    days="$2"; shift 2
  fi
fi

if [[ $# -gt 0 ]]; then
  ids=("$@")
else
  # Parse `plaud recent --days N` — first whitespace-token of each data line is the 32-char hex id
  mapfile -t ids < <($PLAUD recent --days "$days" 2>/dev/null \
    | awk '/^[[:space:]]+[0-9a-f]{32}/{print $1}')
fi

[[ ${#ids[@]} -eq 0 ]] && { log "no recordings to pull"; exit 0; }
log "pull batch: ${#ids[@]} candidate id(s)"

# --- Per-file pull -------------------------------------------------------------
pulled=0
skipped=0
failed=0

for fid in "${ids[@]}"; do
  # Skip if already pulled. Two gates, OR'd:
  #   1. Ledger (authoritative) — survives downstream moves/renames/deletes.
  #   2. Inbox filename scan (legacy fallback) — catches files still awaiting routing.
  if grep -qxF "$fid" "$LEDGER" 2>/dev/null \
     || find "$VAULT_DIR" -type f -name "*--${fid:0:8}.md" -print -quit 2>/dev/null | grep -q .; then
    skipped=$((skipped+1))
    continue
  fi

  if [[ "$DRY_RUN" == "1" ]]; then
    log "  ⟂ DRY-RUN would pull $fid"
    pulled=$((pulled+1))
    continue
  fi

  # Fetch canonical metadata via `plaud file` (key: value lines)
  meta=$($PLAUD file "$fid" 2>/dev/null || true)
  if ! grep -q "^  name:" <<<"$meta"; then
    log "  ✗ metadata fetch failed for $fid"
    failed=$((failed+1)); continue
  fi

  name=$(grep -E "^  name:" <<<"$meta" | sed -E 's/^  name:[[:space:]]+//')
  start=$(grep -E "^  start_at:" <<<"$meta" | sed -E 's/^  start_at:[[:space:]]+//')
  created=$(grep -E "^  created_at:" <<<"$meta" | sed -E 's/^  created_at:[[:space:]]+//')
  dur=$(grep -E "^  duration:" <<<"$meta" | sed -E 's/^  duration:[[:space:]]+//')
  serial=$(grep -E "^  serial_number:" <<<"$meta" | sed -E 's/^  serial_number:[[:space:]]+//')

  date_prefix=$(echo "${start:-$created}" | cut -dT -f1)
  short_id="${fid:0:8}"
  slug=$(echo "$name" | tr '[:upper:]' '[:lower:]' \
        | sed -E 's/[^a-z0-9]+/-/g; s/^-+|-+$//g' | cut -c1-60)
  base="$RAW_DIR/${date_prefix}--${slug}--${short_id}"
  md="$base.md"
  tx_raw="$base.transcript.txt"
  sum_raw="$base.summary.md"

  # Pull content
  $PLAUD transcript "$fid" -o "$tx_raw" >/dev/null 2>&1 || \
    { log "  ✗ transcript pull failed for $fid"; failed=$((failed+1)); continue; }
  $PLAUD summary "$fid" -o "$sum_raw" >/dev/null 2>&1 || \
    log "  ⚠ summary pull failed for $fid (continuing without)"

  # Assemble unified .md
  {
    echo "---"
    echo "source: plaud"
    echo "plaud_file_id: $fid"
    echo "plaud_serial: $serial"
    # Use bash printf to safely embed double-quoted title
    printf 'title: "%s"\n' "${name//\"/\\\"}"
    echo "recorded_at: $start"
    echo "uploaded_at: $created"
    echo "duration_human: $dur"
    echo "pulled_at: $(date -Is)"
    echo "pulled_via: pull-to-vault.sh"
    echo "ingestion_status: untriaged"
    if [[ -s "$sum_raw" ]]; then
      echo "ai_summary: true"
    else
      echo "ai_summary: false"
    fi
    echo "tags: [transcript, plaud, capture]"
    echo "---"
    echo
    echo "# $name"
    echo
    echo "> Pulled via \`pull-to-vault.sh\`. Raw sidecars: \`.transcript.txt\`, \`.summary.md\`. Awaiting L2 routing."
    echo
    if [[ -s "$sum_raw" ]]; then
      echo "## AI Summary"
      echo
      echo "*Source: Plaud \`auto_sum_note\` (CLI \`plaud summary\`). Verbatim.*"
      echo
      cat "$sum_raw"
      echo
    fi
    echo "## Transcript"
    echo
    echo "*Source: Plaud \`plaud transcript\` (formatted text with speaker attribution).*"
    echo
    echo '```text'
    cat "$tx_raw"
    echo '```'
  } > "$md"

  printf '%s\n' "$fid" >> "$LEDGER"   # record only on successful pull → failed transcripts retry next run
  log "  ✓ pulled $fid → $(basename "$md")"
  pulled=$((pulled+1))
done

log "pull batch done: pulled=$pulled skipped=$skipped failed=$failed"
echo "$pulled $skipped $failed"
