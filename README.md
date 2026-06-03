# plaude-api — Plaud → Vault ingestion pipeline

Continuously pulls Plaud voice recordings into the Obsidian vault, classifies them by topic, enriches with entity backlinks + action-item rollups + case-study candidates, and falls back to LLM classification when keyword rules miss. Runs every 30 minutes via systemd user timer.

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
```

## Filesystem layout

```
~/apps/plaude-api/
├── pull-to-vault.sh                # L1
├── screen-and-route.py             # L2
├── llm-reclassify.py               # LLM fallback
├── enrich-routed.py                # L3
└── README.md

~/.config/systemd/user/
├── plaud-sync.service              # oneshot, 4 ExecStarts (L1 → L2 → LLM → L3)
└── plaud-sync.timer                # OnCalendar=*:0/30, Persistent=true

~/.config/plaud-sync/
├── env                             # ANTHROPIC_API_KEY=... (mode 600)
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
```

## L2 keyword classifier rules (priority order)

| Category          | sync_blocked | Trigger keywords (case-insensitive whole-word) |
|---|---|---|
| `Personal-Health` | ✓ | clinical, doctor, medical, ultrasound, appointment, visit, patient, diagnosis, prescription, OB, obstetric, fetal, pregnancy, gynec |
| `Casual`          | ✓ | casual chat, gathering, family, personal, life plan, date night, hangout, catch-up |
| `Learning`        | – | lecture, course, training, tutorial, webinar, workshop, class, seminar |
| `Business`        | – | strategy, project, meeting, scoping, consultation, discovery, business, client, proposal, onboarding, kickoff, standup, review, integration, deployment, automation, pipeline, product demo |
| `_unclassified`   | – | (fallback, picked up by LLM next stage) |

Edit `screen-and-route.py` `RULES` to tune. Order matters — first match wins. `sync_blocked: true` is a frontmatter flag for downstream vault-sync logic to honor — no enforcement here, semantic only.

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

## Authentication

**Plaud CLI** — `@plaud-ai/cli` reads tokens from `~/.plaud/tokens.json`. When the token expires, run `plaud login` interactively (uses port 8199 — see `PORT_REALLOCATION_2026-05-23.md` in `~/projects/project-tracker/`).

**Plaud MCP** (separate from CLI auth) — for ad-hoc use from Claude Code, the MCP server has its own token cache. Call `mcp__plaud__login` once after install.

**Anthropic** — `~/.config/plaud-sync/env` mode 600. Currently sourced from the same key already in use by project-tracker. Rotating the project-tracker key requires updating both locations.

## Idempotency

Every stage is safe to re-run:
- **L1** dedupes via the durable ledger `999 Inbox/Transcripts/.pulled-ids` (full `file_id` per line, appended only on successful pull), OR'd with a legacy inbox-tree filename scan. The ledger survives files being moved/renamed downstream out of the inbox — which is why the discovery window can safely be wide (`--days 90`, for late-transcription resilience) without re-pulling already-filed recordings. Seed/re-seed from existing vault frontmatter: `grep -rhoE '^plaud_file_id:[[:space:]]*[0-9a-f]{32}' ~/vault | awk '{print $2}' | sort -u >> "999 Inbox/Transcripts/.pulled-ids"`. Dry-run a window with `DRY_RUN=1 ./pull-to-vault.sh --days N`. Backfill specific old recordings by explicit id: `./pull-to-vault.sh <file_id> ...`
- **L2** only processes files in `_raw/` — once moved to a bucket, never touched again
- **LLM** only processes files in `_unclassified/`
- **L3** skips files with `ingestion_status: enriched`
- Entity backlinks, action-item blocks, and VDC candidate rows all check for existing content before appending

## What's NOT included (deliberately deferred)

- **PII redaction enforcement** — `sync_blocked: true` is a semantic flag, not enforcement. Any future vault-sync (to remote, GitHub, etc.) must honor it. The vault is operator-trusted local-only at present.
- **Pluggable provider for LLM** — currently Anthropic-direct. Could swap to AI Gateway for failover/cost-tracking. Not done because direct Haiku 4.5 is already <$0.001/file.
- **Webhook-driven ingestion** — Plaud's webhooks are on the B2B Transcription API, not the MCP/CLI surface. Would require a separate API key + provisioning. Polling at 30 min is fine for human-cadence recording.
- **Auto-promotion of VDC candidates to /case-study-extract** — operator-curated only by design (see `case-study-extract` skill: "Operator-curated, never auto-fired"). The queue is a triage prompt, not an autopilot.
- **Cross-recording entity disambiguation** — if "Maria" appears in two different recordings referring to two different people in the vault, both get the same backlink. Acceptable false-positive rate for v1.
