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
# Pinned, not @latest: an unpinned CLI changed its id format under us on
# 2026-09-15 and the pipeline went silently dry for 10 days (see the parser note
# below). Bump deliberately, after checking `plaud recent` output still parses.
PLAUD="${PLAUD_CLI:-npx -y @plaud-ai/cli@0.3.14}"
LOG="$VAULT_DIR/.ingestion.log"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Structured per-record event ledger (see ledger.py). Best-effort — a ledger
# failure must never fail a pull, hence the trailing `|| true` at each call site.
ledger_emit() { python3 "$HERE/ledger.py" emit "$@" >/dev/null 2>&1 || true; }
# Durable dedup ledger: full file_id per line. Survives downstream moves/renames
# out of VAULT_DIR (the filename scan below only sees the inbox tree). This is the
# authoritative skip gate that makes a wide discovery window (--days 90) safe.
LEDGER="${PLAUD_LEDGER:-$VAULT_DIR/.pulled-ids}"
DRY_RUN="${DRY_RUN:-0}"

mkdir -p "$RAW_DIR"

log() { printf '%s\n' "$(date -Is) $*" | tee -a "$LOG" >&2; }

# --- Retry machinery ----------------------------------------------------------
# Three distinct problems, three mechanisms — they are not interchangeable:
#   _retry.sh      transient faults (network blip, upstream 5xx): retry NOW, in-run.
#   retry_state.py definitive-for-now faults (transcript not generated yet): retry LATER,
#                  on a widening schedule, and give up loudly instead of silently forever.
#   notify.sh      the giving-up part: one operator notice per recording, ids only.
# Before this, every failure class got the same treatment — one attempt, then wait 30
# minutes and repeat identically, forever, telling nobody.
# shellcheck source=_retry.sh
source "$HERE/_retry.sh"
CLI_TRIES="${PLAUD_CLI_TRIES:-3}"       # attempts per Plaud CLI call
CLI_BACKOFF="${PLAUD_CLI_BACKOFF:-3}"   # seconds; 3s then 9s
retry_state() { python3 "$HERE/retry_state.py" "$@" 2>/dev/null || true; }
escalate() { "$HERE/notify.sh" "$@" >/dev/null 2>&1 || true; }

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
  # Parse `plaud recent --days N` — first whitespace-token of each data line is the
  # recording id. Until ~2026-09-15 the CLI printed a bare 32-char hex id; it now
  # prints `of_<32hex>`. The old bare-only regex matched NOTHING against the new
  # shape, so every run from 2026-09-15 logged "no recordings to pull" and exited 0
  # while recordings sat unpulled in Plaud. Accept both shapes, and never let a
  # parse miss masquerade as an empty listing.
  recent_err=$(mktemp)
  # The listing is the single point of failure for the entire run: if it fails, nothing
  # downstream even learns there are recordings to fetch. Worse, this unit is a chain of
  # ExecStart= lines, so a failure here also skipped L2-L4 for already-pulled notes.
  # Wrapped in a function so the CLI's stderr still goes to $recent_err while retry_cmd's
  # own diagnostics go to the journal.
  _recent_once() { $PLAUD recent --days "$days" 2>"$recent_err"; }
  set +e
  recent_out=$(retry_cmd "plaud recent" "$CLI_TRIES" "$CLI_BACKOFF" -- _recent_once)
  rc=$?
  set -e
  if [[ $rc -ne 0 ]]; then
    log "✗ plaud recent failed (exit $rc): $(head -c 300 "$recent_err" | tr '\n' ' ')"
    rm -f "$recent_err"; exit 2
  fi
  rm -f "$recent_err"
  # (mawk-safe: no alternation-with-anchor inside the regex — xt12's mawk panics on it)
  mapfile -t ids < <(awk '/^[[:space:]]/ { t=$1; h=t; sub(/^of_/, "", h); if (h ~ /^[0-9a-f]+$/ && length(h) == 32) print t }' <<<"$recent_out")
  # Fail LOUD, not empty: the listing's own count (header) and the number of lines
  # carrying a 32-hex id are both independent of the parser. If either says there
  # are recordings and the parser extracted none, the CLI output shape changed
  # again — exit non-zero so systemd records a failure instead of "nothing new".
  listed=$(grep -cE '[0-9a-f]{32}' <<<"$recent_out" || true)
  header_n=$(sed -nE 's/.*Recordings in the last [0-9]+ days?: ([0-9]+).*/\1/p' <<<"$recent_out" | head -1)
  if [[ ${#ids[@]} -eq 0 && ( "${listed:-0}" -gt 0 || "${header_n:-0}" -gt 0 ) ]]; then
    log "✗ parser extracted 0 ids but the listing shows ${header_n:-?} recording(s) / ${listed:-0} id line(s) — plaud CLI output format changed? Refusing to report 'no recordings'."
    exit 3
  fi
  if [[ -n "$header_n" && "$header_n" -ne ${#ids[@]} ]]; then
    log "⚠ plaud recent reports $header_n recording(s) but the parser extracted ${#ids[@]} — check the CLI output shape"
  fi
fi

[[ ${#ids[@]} -eq 0 ]] && { log "no recordings to pull"; exit 0; }
log "pull batch: ${#ids[@]} candidate id(s)"

# --- Per-file pull -------------------------------------------------------------
pulled=0
skipped=0
failed=0
backoff_held=0   # skipped because their retry window has not reopened yet

for raw_id in "${ids[@]}"; do
  # Ledger rows, filenames (*--<8hex>.md) and the L4 ext_id all key on the BARE hex
  # id — the 184 ledger rows predate the `of_` prefix — while the CLI's file /
  # transcript / summary commands now reject a bare id and need `of_<hex>`.
  fid="${raw_id#of_}"
  if [[ "$raw_id" == of_* ]]; then cli_id="$raw_id"; else cli_id="${PLAUD_ID_PREFIX-of_}$fid"; fi
  # Skip if already pulled. Two gates, OR'd:
  #   1. Ledger (authoritative) — survives downstream moves/renames/deletes.
  #   2. Inbox filename scan (legacy fallback) — catches files still awaiting routing.
  if grep -qxF "$fid" "$LEDGER" 2>/dev/null \
     || find "$VAULT_DIR" -type f -name "*--${fid:0:8}.md" -print -quit 2>/dev/null | grep -q .; then
    skipped=$((skipped+1))
    continue
  fi

  # Third gate: bounded retry. A recording that has already failed carries a backoff
  # window, and hammering it before that window reopens is precisely what produced 8,331
  # pointless attempts across 34 recordings (10 of them 200-1,680 times each, the oldest
  # every 30 minutes since 2026-08-10). A recording never seen before passes straight
  # through, so a genuinely new capture is never delayed by this.
  decision=$(retry_state check "$fid")
  if [[ "$decision" == SKIP* ]]; then
    log "  ⏸ $fid not due for retry (${decision#SKIP })"
    backoff_held=$((backoff_held+1))
    skipped=$((skipped+1))
    continue
  fi

  if [[ "$DRY_RUN" == "1" ]]; then
    log "  ⟂ DRY-RUN would pull $fid"
    pulled=$((pulled+1))
    continue
  fi

  # Fetch canonical metadata via `plaud file` (key: value lines)
  meta=$(retry_cmd "metadata $fid" "$CLI_TRIES" "$CLI_BACKOFF" -- quietly $PLAUD file "$cli_id" || true)
  if ! grep -q "^  name:" <<<"$meta"; then
    # Metadata that keeps failing after in-run retries is not a flaky network — the
    # recording has been deleted or hidden upstream (observed: fb1c0c63, which vanished
    # from Plaud during 2026-09 and had been re-attempted 249 times). retry_state marks it
    # `gone` after 3 such runs spanning 24h, escalates once, then probes weekly.
    log "  ✗ metadata fetch failed for $fid after $CLI_TRIES attempt(s)"
    ledger_emit --event error --stage L1-pull --file-id "$fid" --record "$fid" \
      --detail '{"reason":"metadata fetch failed"}'
    verdict=$(retry_state note "$fid" meta-fail)
    if [[ "$verdict" == ESCALATE* ]]; then
      escalate "plaud-gone-${fid:0:8}" "Recording ${fid:0:8} looks gone from Plaud: its metadata fetch has failed on 3 separate runs over 24h+ (${verdict#ESCALATE }). I have stopped retrying it every 30 minutes and will probe it weekly instead. If you deleted it in Plaud, nothing to do."
    fi
    failed=$((failed+1)); continue
  fi

  name=$(grep -E "^  name:" <<<"$meta" | sed -E 's/^  name:[[:space:]]+//')
  start=$(grep -E "^  start_at:" <<<"$meta" | sed -E 's/^  start_at:[[:space:]]+//')
  created=$(grep -E "^  created_at:" <<<"$meta" | sed -E 's/^  created_at:[[:space:]]+//')
  dur=$(grep -E "^  duration:" <<<"$meta" | sed -E 's/^  duration:[[:space:]]+//')
  serial=$(grep -E "^  serial_number:" <<<"$meta" | sed -E 's/^  serial_number:[[:space:]]+//')
  # Plaud's deterministic content-surface availability flags (audio/transcript/
  # summary) — the upstream-defined asset map, captured verbatim for the ledger.
  avail_audio=$(grep -E "^  audio:" <<<"$meta" | sed -E 's/^  audio:[[:space:]]+//')
  avail_transcript=$(grep -E "^  transcript:" <<<"$meta" | sed -E 's/^  transcript:[[:space:]]+//')
  avail_summary=$(grep -E "^  summary:" <<<"$meta" | sed -E 's/^  summary:[[:space:]]+//')

  date_prefix=$(echo "${start:-$created}" | cut -dT -f1)
  short_id="${fid:0:8}"
  slug=$(echo "$name" | tr '[:upper:]' '[:lower:]' \
        | sed -E 's/[^a-z0-9]+/-/g; s/^-+|-+$//g' | cut -c1-60)
  base="$RAW_DIR/${date_prefix}--${slug}--${short_id}"
  md="$base.md"
  tx_raw="$base.transcript.txt"
  sum_raw="$base.summary.md"

  # Ask before fetching. Plaud publishes its own per-surface availability flags, and they
  # are the authoritative readiness signal — cheaper and more honest than pulling and
  # discovering an empty file. All nine recordings that were being re-fetched every 30
  # minutes for weeks report `transcript: unavailable` here: eight are 2-5 second clips
  # Plaud will not transcribe at all, one is a real 23m27s recording whose upstream
  # transcription failed. No number of retries turns any of them into a transcript.
  if [[ -n "$avail_transcript" && "$avail_transcript" != "available" ]]; then
    log "  ⏳ upstream transcript $avail_transcript for $fid (${dur:-?}) — deferring, not fetching"
    ledger_emit --event deferred --stage L1-pull --file-id "$fid" --record "$base" \
      --title "$name" \
      --detail "$(printf '{"reason":"upstream transcript %s","plaud_surfaces":{"audio":"%s","transcript":"%s","summary":"%s"}}' \
                  "$avail_transcript" "$avail_audio" "$avail_transcript" "$avail_summary")"
    verdict=$(retry_state note "$fid" pending)
    if [[ "$verdict" == ESCALATE* ]]; then
      escalate "plaud-stalled-${fid:0:8}" "Recording ${fid:0:8} (recorded ${start:-?}, ${dur:-?}) has waited 14+ days for a transcript and Plaud still reports transcript: $avail_transcript (${verdict#ESCALATE }). Backing off to a weekly probe. The audio is still in Plaud if you want to re-run transcription there."
    fi
    skipped=$((skipped+1)); continue
  fi

  # Pull content
  retry_cmd "transcript $fid" "$CLI_TRIES" "$CLI_BACKOFF" -- quietly $PLAUD transcript "$cli_id" -o "$tx_raw" || \
    { log "  ✗ transcript pull failed for $fid"; \
      ledger_emit --event error --stage L1-pull --file-id "$fid" --record "$base" \
        --title "$name" --detail '{"reason":"transcript pull failed"}'; \
      failed=$((failed+1)); continue; }

  # An un-transcribed recording returns exit 0 with an EMPTY transcript. Left
  # unchecked, the loop below writes a content-free stub at "$base.md" — and the
  # skip gate at the top of this loop then matches that stub by filename
  # (*--${fid:0:8}.md) on every future run, so the recording can never be pulled
  # again once transcription finishes. Recovery required moving the stub out of
  # VAULT_DIR by hand (observed 2026-08-06: 3275cfe9, a 1h01m call, recovered
  # manually the next morning). Treat empty as failure so it retries, per this
  # loop's own contract: record only on successful pull.
  # Test for the file rather than leaning on a failure path: when the CLI writes no file
  # at all, `wc -c <"$tx_raw"` prints a bash redirect error into the journal on EVERY run
  # (observed for fbb0d0cb and fcea9ddb) before `|| echo 0` quietly masks it.
  if [[ -f "$tx_raw" ]]; then tx_bytes=$(wc -c <"$tx_raw"); else tx_bytes=0; fi
  if [[ "$tx_bytes" -lt 32 ]]; then
    log "  ⏳ transcript not ready for $fid (${tx_bytes}B) — no stub written, will retry"
    rm -f "$tx_raw" "$sum_raw"
    ledger_emit --event deferred --stage L1-pull --file-id "$fid" --record "$base" \
      --title "$name" --detail "$(printf '{"reason":"transcript empty — upstream transcription pending","transcript_bytes":%s}' "$tx_bytes")"
    verdict=$(retry_state note "$fid" pending)
    if [[ "$verdict" == ESCALATE* ]]; then
      escalate "plaud-stalled-${fid:0:8}" "Recording ${fid:0:8} (recorded ${start:-?}, ${dur:-?}) has waited 14+ days and still returns an empty transcript (${verdict#ESCALATE }). Backing off to a weekly probe."
    fi
    skipped=$((skipped+1)); continue
  fi
  # Non-fatal by design (a note is worth having without its AI summary), but still worth
  # a retry budget: a blip here silently downgrades the note's ai_summary to false.
  retry_cmd "summary $fid" "$CLI_TRIES" "$CLI_BACKOFF" -- quietly $PLAUD summary "$cli_id" -o "$sum_raw" || \
    log "  ⚠ summary pull failed for $fid after $CLI_TRIES attempt(s) (continuing without)"

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
  # Clear any backoff row. If we had escalated this recording, close the loop — an
  # operator told a recording stalled is owed the end of the story too.
  verdict=$(retry_state note "$fid" pulled)
  if [[ "$verdict" == RECOVERED* ]]; then
    escalate "plaud-recovered-${fid:0:8}" "Recording ${fid:0:8} finally came through (${verdict#RECOVERED }) — the stall I flagged earlier has resolved itself and the note is in the vault inbox."
  fi
  # Structured event: assets auto-discovered from "$base"* (whatever the
  # Generate template produced — not assumed to be transcript + summary only).
  ledger_emit --event pulled --stage L1-pull --file-id "$fid" --record "$base" \
    --title "$name" --recorded-at "$start" \
    --detail "$(printf '{"recorded_at":"%s","duration_human":"%s","serial":"%s","plaud_surfaces":{"audio":"%s","transcript":"%s","summary":"%s"},"ai_summary":%s}' \
                "$start" "$dur" "$serial" "$avail_audio" "$avail_transcript" "$avail_summary" \
                "$([[ -s "$sum_raw" ]] && echo true || echo false)")"
  pulled=$((pulled+1))
done

log "pull batch done: pulled=$pulled skipped=$skipped (backoff-held=$backoff_held) failed=$failed"
# Print the retry ledger every run: a stalled recording should be visible in the journal
# without anyone knowing to go looking for a state file.
retry_state report | while IFS= read -r line; do log "  $line"; done
echo "$pulled $skipped $failed"
