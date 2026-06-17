---
handoff_type: plaud-record-consumption
audience: downstream AI agent (scan + consume Plaud record outputs)
authority: live — backed by the structured event ledger, regenerate anytime
index_primary: "~/vault/999 Inbox/Transcripts/.ingestion-events.jsonl"
index_tooling: "~/apps/plaude-api/ledger.py  (emit | reconcile | query)"
refresh: "python3 ~/apps/plaude-api/ledger.py reconcile"
record_store: "~/vault (local Obsidian) — NOT the nobox/teamwork-vault MCP"
source_of_truth: "Plaud cloud library (account 00c8bcf85ea94be19d22d4a363f1fd02, jgatlit@gmail.com)"
access_local: "ledger.py query — no creds, reads on-disk index"
access_source_cli: "@plaud-ai/cli — OAuth tokens at ~/.plaud/tokens.json"
access_source_mcp: "Claude Code MCP server `plaud` — separate OAuth, port 8199"
---

# Plaud Records — Agent Handoff & Search Guide

You are consuming voice-recording records ingested from **Plaud** by the 4-stage
pipeline at `~/apps/plaude-api/` (L1 pull → L2 route → LLM fallback → L3 enrich).
This document is the **stable contract** for finding and reading them. Do not
hand-walk the filesystem — query the ledger (Section 2). It already knows every
record's current path, even after records are relocated/renamed out of the inbox.

**Decision rule — which surface to query:**
- **Existing / already-ingested records** → the **local ledger** (Section 0.A → §2–3).
  Default. No credentials. Covers everything pulled into the vault.
- **Latest from source / not-yet-ingested / audio / raw assets** → the **Plaud
  live API** via MCP or CLI (Section 0.B). Needs OAuth.

---

## 0. Access surfaces — endpoints & credentials

> ⚠️ **"Vault" is overloaded — do not confuse two stores.** Plaud records live in
> the **local Obsidian vault at `~/vault/`**, indexed by `ledger.py`. The
> **`nobox-vault` / `teamwork-vault` MCP** (tools `artifact_*`, `task_*`,
> `project_*`) is the team *knowledge* workspace — it does **not** hold Plaud
> recordings. Querying it for recordings will return nothing relevant.

### 0.A — Local index (PRIMARY; no credentials)

The ingested record corpus is on disk; query it with the ledger tooling. This is
the answer for "existing recordings/assets" and needs no auth.

| Thing | Value |
|---|---|
| Tooling | `python3 ~/apps/plaude-api/ledger.py query [...]` |
| Event index | `~/vault/999 Inbox/Transcripts/.ingestion-events.jsonl` |
| Records at rest | `~/vault/400 Resources/Transcripts/` (curated) · `~/vault/999 Inbox/Transcripts/<category>/` (fresh) |
| Refresh if stale | `python3 ~/apps/plaude-api/ledger.py reconcile` |
| Pipeline liveness | `systemctl --user status plaud-sync.timer` (runs every 30 min) |

### 0.B — Plaud live source (OAuth — for newest/source/audio)

Use this only when the ledger can't answer: a recording too new to be ingested,
**audio** (never pulled to disk), or a Plaud-side asset the CLI can't fetch.

**Account:** `jgatlit@gmail.com` · id `00c8bcf85ea94be19d22d4a363f1fd02` ("Jonathan Gudger").

| Surface | How to reach it | Credential |
|---|---|---|
| **MCP** (in-session, read-only) | server name **`plaud`** — tools `mcp__plaud__list_files`, `get_file`, `get_note`, `get_transcript`, `get_current_user`, `login`, `logout` | own OAuth state; first call may need `mcp__plaud__login` (browser, **port 8199**) |
| **CLI** (terminal/scripts) | `npx -y @plaud-ai/cli@latest` — `recent --days N`, `today`, `files`, `search <kw>`, `file <id>`, `transcript <id>`, `summary <id>`, `audio <id>`, `me` | OAuth tokens at **`~/.plaud/tokens.json`** |
| **B2B Transcription API** | `platform-us.plaud.ai/developer/api` — submit arbitrary audio | **separate API keys — NOT provisioned**; don't assume it |

**Credential notes (read before calling source):**
- **MCP and CLI hold SEPARATE auth.** CLI tokens at `~/.plaud/tokens.json` do *not*
  carry to the MCP server, and vice-versa.
- If a source call fails with **"Not authenticated"**, tokens expired: run
  `mcp__plaud__login` (MCP) or `npx -y @plaud-ai/cli@latest login` (CLI). The
  systemd pipeline does **not** self-re-auth — it silently drops pulls, so watch
  `~/vault/999 Inbox/Transcripts/.ingestion.log` for auth errors.
- OAuth callback is hardcoded to **port 8199**. On `PORT_IN_USE`:
  `lsof -nP -iTCP:8199 -sTCP:LISTEN` then kill the holder.
- **URL expiry:** presigned **audio** URLs last 24h; the polished-transcript S3
  URL (`data_type: transaction_polish`) expires in **5 minutes** — consume it
  immediately, don't cache.
- **Anthropic key** (only the L3 LLM-reclassify stage needs it):
  `~/.config/plaud-sync/env` (mode 600). Not needed to read or query records.

**Asset reachability ceiling (don't promise what the API can't give):** the CLI/MCP
expose only **transcript + AI summary + metadata** (and a time-limited audio *URL*).
There is **no** endpoint for Plaud user-notes, mind-maps, custom-template summaries,
or highlights — those are unreachable regardless of pipeline changes. "Packages" of
assets = whatever surfaces the ledger captured per record (§1), not a fixed bundle.

---

## 1. The record model (read this first — it is NOT "transcript + summary")

**Record identity is the Plaud `file_id`** — a 32-char lowercase hex string
(`short_id` = first 8 chars). This is the ONLY deterministic key. The filename
slug (`<date>--<slug>--<shortid>`) is synthesized locally and is NOT reliable —
records get renamed to human titles downstream. **Always key on `file_id`.**

**Assets are Plaud content *surfaces*, discovered — never assumed.** Plaud exposes
three deterministic surfaces per recording, reported as availability flags:

| Surface      | What it is                          | Typical file              |
|--------------|-------------------------------------|---------------------------|
| `transcript` | speaker-attributed prose            | `<base>.transcript.txt`   |
| `summary`    | AI note (`auto_sum_note`)           | `<base>.summary.md`       |
| `audio`      | recording (24h presigned URL)       | (not pulled to disk by default) |
| `note-assembled` | our unified note (frontmatter + summary + transcript) | `<base>.md` |

⚠️ **The summary's SHAPE varies by the Plaud "Generate" template** (SOAP for
medical, synopsis/action-items for business, etc.), and future templates may emit
ADDITIONAL assets (mind-maps, structured JSON, formatted variants). The ledger
captures **whatever files exist under the record's id token** — each tagged with
its `surface` (authoritative when known) and a fallback `kind`. An asset with
`surface: null` is a template-specific or non-Plaud file; consume it by `kind`.
**Never assume a fixed file count or extension set.**

> Non-Plaud records (e.g. Zoom `.vtt`/`.json` exports) also live in the
> transcript tree and are indexed by `reconcile`. Distinguish them by
> `detail.source` (`plaud` vs `unknown`) — they have no `file_id`.

---

## 2. The indexes (in priority order)

1. **Structured event ledger** — `~/vault/999 Inbox/Transcripts/.ingestion-events.jsonl`
   One JSON object per record state-transition (`pulled`/`routed`/`reclassified`/
   `enriched`/`observed`/`error`). Each line carries `file_id`, `title`,
   `category`, `sync_blocked`, `recorded_at`, current `dir`, full `assets[]`, and
   `entities[]`. **This is your primary index. Query it with `ledger.py query`.**
2. **Per-record frontmatter** (in each `<base>.md`) — `plaud_file_id`, `recorded_at`,
   `category`, `sync_blocked`, `entity_mentions`, `action_items_rollup`,
   `ingestion_status`. Use for record-level detail when reading a note.
3. **Human batch log** — `.ingestion.log` — free-text run history. For ops/debug,
   not for querying records.

If the ledger looks stale (records moved since last write), refresh it:
```bash
python3 ~/apps/plaude-api/ledger.py reconcile      # re-snapshots current on-disk state
```

---

## 3. Search endpoints

All via `python3 ~/apps/plaude-api/ledger.py query [...]`. Add `--collapse` to get
the latest state per record (deduped); add `--json` for machine output; add
`--paths` to print current asset paths; add `--show-assets` for a path tree.
Ranking is by `recorded_at` (true record recency) unless `--sort ts`.

| Intent | Command |
|---|---|
| **Latest N records** | `query --collapse --latest 6` |
| **By date range** | `query --collapse --since 2026-06-01 --until 2026-06-07` |
| **By label / category** | `query --collapse --category Business` (Business\|Personal-Health\|Casual\|Learning) |
| **Public only (safe to surface)** | `query --collapse --public` |
| **Private only (sync-blocked)** | `query --collapse --sync-blocked` |
| **By attendee / entity** | `query --collapse --attendee "Jonathan Gudger"` |
| **By title / topic keyword** | `query --collapse --title "funnel"` |
| **By Plaud id** | `query --id 3fe92965` (8 or 32 hex) |
| **Current paths of all matches** | `query --collapse --category Business --paths` |
| **Errors / failed pulls** | `query --event error` |

**By frontmatter / deep content** (when the ledger fields aren't enough), search
the notes directly:
```bash
# by any frontmatter field
rg -l '^category: Business' ~/vault/"400 Resources"/Transcripts ~/vault/"999 Inbox"/Transcripts
# by transcript/summary CONTEXT (topic, phrase, decision)
rg -i "go-?high-?level|MVP|RFP" ~/vault/"400 Resources"/Transcripts --glob '*.summary.md'
```

**Raw `jq`** against the ledger (for custom joins):
```bash
EV=~/vault/"999 Inbox"/Transcripts/.ingestion-events.jsonl
# every record + its asset surfaces, newest first
jq -rs 'map(select(.event=="observed" or .event=="enriched"))
        | group_by(.file_id) | map(last) | sort_by(.recorded_at) | reverse[]
        | "\(.recorded_at)  \(.category)  \(.title)  [\([.assets[].surface]|join(","))]"' "$EV"
# all records mentioning an attendee
jq -rs 'map(select(.entities|index("Jay Samit"))) | .[].title' "$EV"
```

---

## 4. Consuming records downstream

1. **Resolve the record** with a query above → get `file_id`, `category`,
   `sync_blocked`, `dir`, `assets[]`.
2. **Read the `summary` surface first** (`*.summary.md`) for the gist; open the
   `transcript` surface (`*.transcript.txt`) only for verbatim detail. Iterate
   `assets[]` by `surface`/`kind` — handle whatever is present, don't assume.
3. **Reuse the pipeline's enrichment** instead of re-deriving:
   - `entities[]` / frontmatter `entity_mentions` → who/what was referenced
     (already backlinked in `300 Entities/<name>.md` under `## Recent Mentions`).
   - `action_items_rollup` → `200 Notes/_action-items/<date>.md` (already extracted).
   - `vdc_candidate: true` → flagged in `200 Notes/Value-Delivered/_candidates.md`.

### Guardrails
- **`sync_blocked: true` (Personal-Health, Casual) = PRIVATE.** Never publish,
  send externally, or include in client-facing output. Filter with `--public`.
- **Public categories (Business, Learning, `sync_blocked: false`)** are the
  candidates for client/value-delivered/outbound downstream work.
- **`detail.source != "plaud"`** → non-Plaud record (e.g. Zoom); no `file_id`,
  may be summary-only with no transcript pair. Handle as a single document.
- Asset paths in the ledger are **vault-relative**; prefix with `~/vault/`.

---

## 5. Pointers

- Pipeline code & this guide: `~/apps/plaude-api/` (`pull-to-vault.sh`,
  `screen-and-route.py`, `llm-reclassify.py`, `enrich-routed.py`, `ledger.py`).
- Records at rest: `~/vault/400 Resources/Transcripts/` (relocated/curated) and
  `~/vault/999 Inbox/Transcripts/<category>/` (freshly routed, pre-relocation).
- Dedup ledger (idempotency, not for querying): `.pulled-ids`.
- To see the live pipeline status: `systemctl --user status plaud-sync.timer`.
