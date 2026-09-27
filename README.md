# plaude-api — Plaud → Vault ingestion pipeline

Continuously pulls Plaud voice recordings into the Obsidian vault, classifies them by topic, enriches with entity backlinks + action-item rollups + case-study candidates, and falls back to LLM classification when keyword rules miss. A final stage pushes every recording into the nobox-vault `plaud` federated source, tagged with its category. Runs every 30 minutes via systemd user timer.

## Pipeline stages

```
[plaud cloud]
      │
      │ every 30 min (systemd-user timer)
      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ L1  pull-to-vault.sh         discover + fetch transcript+summary             │
│        → writes 999 Inbox/Transcripts/_raw/<date>--<slug>--<short-id>.md     │
│        → idempotent: durable .pulled-ids ledger (discovery window --days 90) │
├──────────────────────────────────────────────────────────────────────────────┤
│ L2  screen-and-route.py      keyword classifier (4 buckets + fallback)       │
│        → moves to Personal-Health / Casual / Learning / Business             │
│        → unmatched → _unclassified/                                          │
│        → patches frontmatter: category, sync_blocked, routed_at              │
├──────────────────────────────────────────────────────────────────────────────┤
│ LLM llm-reclassify.py        Haiku 4.5 classifier for _unclassified/         │
│        → JSON-mode output, prompt-cached system rules                        │
│        → noops silently if ANTHROPIC_API_KEY unset                           │
│        → patches frontmatter: llm_reclassified, llm_confidence, llm_reason   │
├──────────────────────────────────────────────────────────────────────────────┤
│ L3  enrich-routed.py         entity backlinks + action items + VDC queue     │
│        → scans AI summary + transcript head for entity-name mentions         │
│        → appends `## Recent Mentions` lines to 300 Entities/<entity>.md      │
│        → (Business+Personal-Health) parses Plan/Next-Steps → 200 Notes/     │
│          _action-items/<date>.md                                             │
│        → (Business) if delivery-signal language + Company hit, appends to   │
│          200 Notes/Value-Delivered/_candidates.md                            │
│        → flips frontmatter: ingestion_status routed → enriched               │
└──────────────────────────────────────────────────────────────────────────────┘
      │
      ▼
 L4  ops/plaud-push.mjs  (teamwork-sync repo, run from its main checkout)
        → pushes EVERY recording note to nobox-vault federated source `plaud`
        → kind = category: business / learning / personal-health / casual / unclassified
        → readable only by jonathan@noboxai.com (source owner scoping)
```

## Filesystem layout

```
~/apps/plaude-api/
├── pull-to-vault.sh                # L1
├── screen-and-route.py             # L2
├── llm-reclassify.py               # LLM fallback
├── enrich-routed.py                # L3
├── ledger.py                       # structured event ledger (emit | reconcile | query)
├── AGENT-HANDOFF.md                # how downstream agents find and read records
└── README.md

~/Documents/aiChemist.agency/teamwork-sync/ops/
└── plaud-push.mjs                  # L4 (lives in the teamwork-sync repo)

~/.config/systemd/user/
├── plaud-sync.service              # oneshot, 5 ExecStarts (L1 → L2 → LLM → L3 → L4)
└── plaud-sync.timer                # OnCalendar=*:0/30, Persistent=true

~/.config/plaud-sync/
├── env                             # ANTHROPIC_API_KEY=..., VAULT_BROKER_KEY=... (mode 600)
└── env.example

~/vault/999 Inbox/Transcripts/
├── _raw/                           # L1 staging — L2 drains this
├── _unclassified/                  # L2 fallback — LLM drains this
├── Personal-Health/                # [sync_blocked: true]
├── Casual/                         # [sync_blocked: true]
├── Learning/
├── Business/
└── .ingestion.log                  # append-only audit log

~/vault/200 Notes/_action-items/<date>.md            # L3 output (Business + Personal-Health)
~/vault/200 Notes/Value-Delivered/_candidates.md     # L3 output (Business + delivery signal)
~/vault/300 Entities/{People,Companies,Projects}/    # L3 appends `## Recent Mentions`
```

## Operations

```bash
# Trigger manual full-cycle run
systemctl --user start plaud-sync.service

# Watch live progress
journalctl --user -u plaud-sync.service -f

# Tail the ingestion log
tail -f "$HOME/vault/999 Inbox/Transcripts/.ingestion.log"

# Timer status
systemctl --user list-timers plaud-sync.timer

# Pull a specific recording ad-hoc (bypasses timer + L2 + LLM + L3)
./pull-to-vault.sh <file_id>

# Re-classify _unclassified/ via LLM only (will no-op without API key)
./llm-reclassify.py --dry-run        # preview
./llm-reclassify.py                  # apply
./llm-reclassify.py --max 5          # cap calls

# Re-enrich (entity scan / action items / VDC) without re-pulling
./enrich-routed.py --dry-run
./enrich-routed.py

# Preview what L4 would push (counts by kind; no network, no credentials needed)
node ~/Documents/aiChemist.agency/teamwork-sync/ops/plaud-push.mjs --dry
```

## L2 keyword classifier rules (priority order)

| Category          | sync_blocked | Trigger keywords (case-insensitive whole-word) |
|---|---|---|
| `Personal-Health` | ✓ | clinical, doctor, medical, ultrasound, appointment, visit, patient, diagnosis, prescription, OB, obstetric, fetal, pregnancy, gynec |
| `Casual`          | ✓ | casual chat, gathering, family, personal, life plan, date night, hangout, catch-up |
| `Learning`        | – | lecture, course, training, tutorial, webinar, workshop, class, seminar |
| `Business`        | – | strategy, project, meeting, scoping, consultation, discovery, business, client, proposal, onboarding, kickoff, standup, review, integration, deployment, automation, pipeline, product demo |
| `_unclassified`   | – | (fallback, picked up by LLM next stage) |

Edit `screen-and-route.py` `RULES` to tune. Order matters — first match wins. `sync_blocked: true` is a frontmatter flag — no enforcement here, semantic only. L4 does not use it to exclude anything: every category is pushed, and the category travels as the item's `kind` (see L4 below).

## LLM classifier (Haiku 4.5)

- Only runs on files in `_unclassified/`. Never re-classifies already-routed files.
- Uses prompt caching on the system prompt (rules + few-shot examples) — per-call delta is just title + 1500-char summary excerpt.
- Returns JSON: `{"category": "...", "confidence": 0.0–1.0, "reason": "..."}` — provenance is preserved in transcript frontmatter (`llm_model`, `llm_confidence`, `llm_reason`, `llm_reclassified_at`).
- Cost: ~$0.0003/file at current Haiku 4.5 pricing once cache is warm.
- Silently no-ops if `ANTHROPIC_API_KEY` is unset — does not block the rest of the pipeline.
- Key is loaded by systemd via `EnvironmentFile=-/home/jgatlit/.config/plaud-sync/env` (the `-` makes the file optional).

To rotate the key: edit `~/.config/plaud-sync/env`. To disable LLM stage entirely: comment out the `ExecStart=…llm-reclassify.py` line in `plaud-sync.service` and `daemon-reload`.

## L3 enrichment

**Entity-mention backlinks** — scans the AI summary + first 8KB of transcript for any vault entity name from `300 Entities/{People,Companies,Projects}/`. Filename stem is the canonical wikilink target. Names ≤3 chars are skipped (too noisy). For each hit, appends a dated line under `## Recent Mentions` in the entity file. Idempotent — duplicate lines aren't written.

**Action-item extraction** — for Business + Personal-Health categories only. Parses bullets under any heading matching `Plan / Next Steps / Action Items / To-Do` in the AI summary. Strips `[ ]` / `[x]` prefixes, filters sub-section labels (lines ending with `:`), filters `@Person` assignment headers, filters placeholder `"insert more"` lines. Writes to `200 Notes/_action-items/<recorded-date>.md` under a `## From [[transcript-stem]]` block — one block per source, idempotent.

**VDC candidate hook** — for Business category only. If the transcript mentions a Company entity AND the summary contains delivery-signal language (`closed`, `shipped`, `delivered`, `wrapped`, `live`, `launched`, `signed`, `deployed`, `in production`, `went live`), appends a candidate row to `200 Notes/Value-Delivered/_candidates.md` for operator triage. Composes with the `/case-study-extract` skill — operator picks promising candidates and runs `/case-study-extract <client-slug> <delivery-event-slug>` to author the canonical case study.

After enrichment, transcript frontmatter is updated with `entity_mentions: [list]`, `action_items_rollup: [[date]]`, `vdc_candidate: true`, and `ingestion_status: enriched`. Files with `enriched` status are skipped on subsequent runs.

## L4 federation push (nobox-vault)

`ops/plaud-push.mjs` lives in the **teamwork-sync** repo; systemd runs it from the main checkout at `~/Documents/aiChemist.agency/teamwork-sync`, so a change to it is live only after it is merged **and** `git pull`ed there (no worker deploy).

- **What goes in:** every Plaud recording note under `999 Inbox/Transcripts/` and `400 Resources/Transcripts/`, one item per `plaud_file_id`. Notes are found by their frontmatter on disk, not by the ledger's recorded paths (those go stale when notes are archived or renamed). `300 Entities/` notes are derived and excluded.
- **Duplicates:** the fullest normal copy wins. A copy in `_no-transcript-fragments`, `_ingestion-duplicates`, `_inbox-duplicates-already-processed` or `_empty-stubs-superseded` counts only if it is the sole copy and still carries a real transcript.
- **Segments:** the note's `category` becomes the item `kind` — `business`, `learning`, `personal-health`, `casual`, `unclassified`. Filter at search time: `source_search {sources:["plaud"], kind:"personal-health"}`. Search is hybrid keyword + semantic.
- **Access:** the `plaud` source is owner-scoped, so only jonathan@noboxai.com can search or fetch these items — whichever key pushed them. An agent/admin token sees none, by design.
- **Auth:** static `VAULT_BROKER_KEY` from `~/.config/plaud-sync/env` → `POST /source-ingest`. Idempotent upsert by `ext_id`; unchanged items are no-ops. Batches of 25; any failure exits 2 so the service shows FAILED.

## Authentication

**Plaud CLI** — `@plaud-ai/cli` reads tokens from `~/.plaud/tokens.json`. When the token expires, run `plaud login` interactively (uses port 8199 — see `PORT_REALLOCATION_2026-05-23.md` in `~/projects/project-tracker/`).

**Plaud MCP** (separate from CLI auth) — for ad-hoc use from Claude Code, the MCP server has its own token cache. Call `mcp__plaud__login` once after install.

**Anthropic** — `~/.config/plaud-sync/env` mode 600. Currently sourced from the same key already in use by project-tracker. Rotating the project-tracker key requires updating both locations.

**nobox-vault** — `VAULT_BROKER_KEY` in the same env file, used only by L4. It is the vault's static `BROKER_API_KEY` (no expiry).

## Retry behaviour

Three different failure classes, three different mechanisms. They are not
interchangeable, and treating them alike is what went wrong before 2026-09-27.

| Failure | Example | Mechanism | Behaviour |
|---|---|---|---|
| **Transient** | DNS blip, upstream 5xx, `fetch failed` | `_retry.sh` → `retry_cmd` | Retried immediately in-run, exponential backoff (3s, 9s). Logs `↻ … recovered on attempt 2/3`. |
| **Not ready yet** | Plaud has the audio but no transcript | `retry_state.py` | Retried later on a widening schedule, then escalated once and probed weekly. |
| **Never going to work** | recording deleted upstream; a 3-second clip Plaud won't transcribe | `retry_state.py` → `notify.sh` | One Slack notice, then a weekly probe. Never abandoned outright. |

### The backoff schedule

Measured from a recording's **first** failure, not its last — otherwise a recording
that has been failing for a month resets itself to hot-retry every time it fails again.

```
age < 2h    every run (~30 min)   transcription normally lands in this window
age < 24h   every 2h
age < 3d    every 6h
age < 14d   every 24h
age >= 14d  escalate once -> `stalled` -> weekly probe
```

A recording whose **metadata** fetch keeps failing is a different animal: it has been
deleted or hidden upstream. Three strikes spanning at least 24h marks it `gone`,
escalates once, and probes weekly.

Success always wins: a pulled recording's row is deleted, and if it had been escalated
the operator gets a `RECOVERED` notice so they hear the end of the story too.

### Why this exists

`plaud recent --days 90` re-offers every recording on every run, and the skip gate only
knew about *successful* pulls. So anything that failed was re-attempted every 30 minutes
forever. Measured 2026-09-27: **8,331 deferred events across 34 recordings**, the worst
of them 1,682 attempts since 2026-08-10 — and not one operator notice, ever. Nine of the
ten worst report `transcript: unavailable` at Plaud and were never going to succeed.

At the same time, genuinely transient faults got **no** retry: one non-JSON HTTP 500
from `/source-ingest` on 2026-09-18 failed the whole unit, while a manual re-run seconds
later succeeded with `ingested=118`.

### Stage isolation

`plaud-sync.service` runs **one** `ExecStart`: `sync-pipeline.sh`. It used to list all
five stages as five `ExecStart=` lines, and systemd aborts the remainder on the first
non-zero exit — so a flaky Plaud listing silently skipped L2 routing, the reclassifier,
L3 enrichment and the L4 vault push for notes already on disk. The driver runs every
stage regardless, retries each one (all five are idempotent), and exits 2 if anything is
still broken at the end.

### Inspecting and operating it

```bash
# What is currently parked, and why
python3 retry_state.py report

# Force a recording back into the hot path (drops its backoff row)
python3 retry_state.py note <file_id> pulled

# Operator notices that were raised (survives a Slack outage)
tail ~/vault/999\ Inbox/Transcripts/.operator-notices.log

# Test the backoff logic without waiting days
python3 test_retry_state.py
```

State lives in `~/vault/999 Inbox/Transcripts/.retry-state.json`, keyed by recording id.
It deliberately carries **no titles** — Plaud titles routinely describe clinical content,
and the same rule applies to every Slack notice this pipeline sends.

Tunables (all env-overridable): `PLAUD_CLI_TRIES`, `PLAUD_CLI_BACKOFF`,
`PLAUD_STAGE_TRIES`, `PLAUD_STAGE_BACKOFF`, `PLAUD_STALL_AFTER_SEC`,
`PLAUD_GONE_STRIKES`, `PLAUD_COLD_PROBE_SEC`, `PLAUD_NOTIFY_GAP`.

## Idempotency

Every stage is safe to re-run:
- **L1** dedupes via the durable ledger `999 Inbox/Transcripts/.pulled-ids` (full `file_id` per line, appended only on successful pull), OR'd with a legacy inbox-tree filename scan. The ledger survives files being moved/renamed downstream out of the inbox — which is why the discovery window can safely be wide (`--days 90`, for late-transcription resilience) without re-pulling already-filed recordings. Seed/re-seed from existing vault frontmatter: `grep -rhoE '^plaud_file_id:[[:space:]]*[0-9a-f]{32}' ~/vault | awk '{print $2}' | sort -u >> "999 Inbox/Transcripts/.pulled-ids"`. Dry-run a window with `DRY_RUN=1 ./pull-to-vault.sh --days N`. Backfill specific old recordings by explicit id: `./pull-to-vault.sh <file_id> ...`
- **L2** only processes files in `_raw/` — once moved to a bucket, never touched again
- **LLM** only processes files in `_unclassified/`
- **L3** skips files with `ingestion_status: enriched`
- **L4** upserts by Plaud `file_id`; the vault writes nothing for an unchanged item
- Entity backlinks, action-item blocks, and VDC candidate rows all check for existing content before appending

## What's NOT included (deliberately deferred)

- **PII redaction enforcement** — `sync_blocked: true` is a semantic flag, not enforcement. Personal-Health and Casual recordings **are** pushed to nobox-vault, where owner scoping limits them to jonathan@noboxai.com. Any other sync (to a remote, GitHub, a shared or client-facing surface) must still honor the flag.
- **Pluggable provider for LLM** — currently Anthropic-direct. Could swap to AI Gateway for failover/cost-tracking. Not done because direct Haiku 4.5 is already <$0.001/file.
- **Webhook-driven ingestion** — Plaud's webhooks are on the B2B Transcription API, not the MCP/CLI surface. Would require a separate API key + provisioning. Polling at 30 min is fine for human-cadence recording.
- **Auto-promotion of VDC candidates to /case-study-extract** — operator-curated only by design (see `case-study-extract` skill: "Operator-curated, never auto-fired"). The queue is a triage prompt, not an autopilot.
- **Cross-recording entity disambiguation** — if "Maria" appears in two different recordings referring to two different people in the vault, both get the same backlink. Acceptable false-positive rate for v1.
- **Backfill of pre-mid-May 2026 recordings** — held; tracked as nobox-vault task `tsk_7b94b005fe564480a600`.
