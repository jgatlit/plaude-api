# _retry.sh — shared transient-failure retry for the Plaud pipeline. Source it; don't run it.
#
# WHY THIS EXISTS
# ---------------
# Every Plaud CLI call and the L4 federation push got exactly ONE attempt per run, so any
# blip cost a full 30-minute cycle — and because plaud-sync.service is a chain of
# ExecStart= lines, a failure in the FIRST stage also skipped L2-L4 entirely. Measured
# cases: 2026-09-18, one non-JSON HTTP 500 from /source-ingest failed the unit while a
# manual re-run seconds later succeeded (ingested=118); five earlier journal failures were
# transport-level `fetch failed`. All six were transient. None was retried.
#
# retry_cmd gives a call a small exponential-backoff budget so a blip self-heals inside the
# same run, and says so in the journal when it does.
#
#   retry_cmd <label> <attempts> <base_delay_sec> -- <command...>
#
# The command's stdout passes through untouched (so `x=$(retry_cmd ... -- cmd)` works);
# retry chatter goes to stderr via log(). Returns the last attempt's exit code.
#
# Deliberately NOT retried: a definitive "no" (a 404, an auth rejection, a parse refusal).
# Retrying those just burns time — that class is handled by retry_state.py's backoff and
# escalation instead.

# Fallback log() so this file is safe to source from a script that has none yet.
if ! declare -F log >/dev/null 2>&1; then
  log() { printf '%s\n' "$(date -Is) $*" >&2; }
fi

# Run a command with its stderr discarded (for CLI calls whose noise we don't want in the
# journal, while retry_cmd's own diagnostics stay visible).
quietly() { "$@" 2>/dev/null; }

retry_cmd() {
  local label="$1" attempts="$2" base="$3"
  shift 3
  [[ "${1:-}" == "--" ]] && shift
  local n=1 rc=0 delay
  while :; do
    set +e
    "$@"
    rc=$?
    set -e
    if [[ $rc -eq 0 ]]; then
      [[ $n -gt 1 ]] && log "  ↻ $label recovered on attempt $n/$attempts"
      return 0
    fi
    if [[ $n -ge $attempts ]]; then
      break
    fi
    # 3s, 9s, 27s... — long enough for a router/upstream hiccup, short enough that a run
    # started at :00 still finishes well inside its 30-minute slot.
    delay=$(( base ** n ))
    log "  ↻ $label failed (exit $rc) on attempt $n/$attempts — retrying in ${delay}s"
    sleep "$delay"
    n=$(( n + 1 ))
  done
  log "  ✗ $label failed $attempts/$attempts attempt(s) (last exit $rc)"
  return $rc
}
