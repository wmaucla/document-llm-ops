# docpipeline/reconciliation/

Everything that keeps the system healthy, or fixes it by hand — the "Reconciliation
and operations" section plus the break-glass lane it depends on.

| File | What it does |
|---|---|
| `orphan_detector_0.py` | Numbered as the true step 0 — the actual local ingest loop. Polls `inbox/` every 10s and looks for objects the ledger doesn't know about yet; this is the standard fallback for a GCS emulator with no bucket-notification wiring, not a deviation. |
| `sweeper.py` | Stuck-state recovery — batch-capped, `SKIP LOCKED`, re-drives documents stuck past `STUCK_THRESHOLD_SECONDS`. `redrive_document` is shared with `operator.py`'s break-glass lane, not a second write path. |
| `dlq_replay.py` | Re-drives a `failed` document only when `build_sha`/`prompt_version` changed since the failing attempt — a no-op on a second failure at the same version. |
| `terminal_report.py` | Scheduled summary of what is parked in `failed` and `review` — counts, age, top `last_error` prefixes, and for `failed` how many `dlq_replay` will *not* pick up. `review` is the bigger blind spot: nothing automatic re-drives it ever. Read-only. Deliberately a report, not triage. Run daily by the `docpipeline-terminal-report` CronJob, or `make terminal-report` |
| `gpu_watchdog.py` | Polls ollama's `/api/ps` every 30s and deletes the pod when a model comes back CPU-resident (`size_vram == 0`), rate-limited by a cooldown so a genuinely GPU-less host degrades to periodic restarts rather than thrashing. The only module here that talks to the Kubernetes API, hence the only one with a ServiceAccount |
| `queries.py` + `sql/` | The multi-line SQL this tier issues — sweeper's stuck-batch claim, DLQ replay's candidate scan, the terminal report's two aggregates, the orphan detector's pending check — loaded from `sql/*.sql` by `-- name:` marker |
| `prune.py` | Retention for `outbox` and `attempt_log`, the two tables nothing else deletes from. Batched with commits between batches; the outbox predicate is on `published_at`, never `created_at`, so an old-but-undelivered message is never dropped. Nightly `docpipeline-prune` CronJob, or `make prune`; `--dry-run` to count without deleting |
| `deadmans_switch.py` | Reports unhealthy the moment something is ingested or in-flight with zero completions; healthy when either nothing is happening or things are completing. |
| `canary.py` | Injects one synthetic document and drives it through every stage, asserting it reaches a terminal state within an SLO. `--extraction-mode real` must be passed explicitly when watching a K8s pipeline — the canary process itself never calls the LLM, so its own ambient `config.EXTRACTION_MODE` says nothing about how the target pipeline actually processed the doc. |
| `operator.py` | The two operator lanes. Read-only replay (`replay_documents`) connects as `pipeline_ro` — structurally incapable of writing, not just a function that happens not to call `enqueue()`. Break-glass (`force_redrive`/`bulk_redrive`/`set_kill_switch`) requires a non-empty `reason`, writes an audit row, and enforces a blast-radius cap on bulk actions. |

See [AGENT.md](../../AGENT.md)'s "The two operator lanes" section for the confirmed-live gotcha
around `force_redrive` and a zero-byte upload's missing `page_count`.
