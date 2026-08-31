# local_scripts/

Utilities written for local, host-mode execution (`make e2e`'s `run_local.py`, `wait_for_drain.py`)
— named to make that intent explicit. Three of them (`create_topics.py`, `summarize.py`,
`replay_docs.py`) also get copied into the Docker image (`Dockerfile`'s `COPY local_scripts
local_scripts/`) and reused in-cluster via one-off Jobs or `kubectl exec`, since it's the same code
either way — that's reuse of convenience, not the reason this directory exists.

| File | What it does |
|---|---|
| `run_local.py` | Host-process orchestrator for `make e2e` — spawns all 8 consumers as subprocesses, `os.setpgrp()`s itself so the whole group can be killed at once (see [AGENT.md](../AGENT.md)'s PID-capture gotcha), own SIGTERM handler for clean `Ctrl-C` shutdown |
| `wait_for_drain.py` | Polls the ledger until every document reaches a terminal state or a timeout elapses — `make e2e` (host mode) only |
| `create_topics.py` | Creates every Kafka topic, idempotent |
| `summarize.py` | Prints the final per-document state report, and is the actual pass/fail gate for both `make e2e` and `make e2e-k8s` — exits 1 if anything is still in-flight |
| `replay_docs.py` | `make replay-docs COUNT=N` — injects N fresh synthetic documents into an already-running cluster, no redeploy. Each gets a unique invoice_no/upload path (same trick `canary.py` uses), since doc_id is a content checksum and literally re-uploading the same fixture bytes would just dedupe into a no-op. On a healthy GPU these reach `complete` (3/3 on 2026-08-31); a run where they pile into `review` instead means inference has degraded to CPU (AGENT.md bug #2), not that the documents are unusual — 13/13 went to `review` under deliberate forced-CPU on the same day |
| `measure_pending_lag.py` | Diagnostic for AGENT.md bug #3's open question — samples `extract_pending` counts and `ocr.completed` consumer-group lag together, to decide whether piled-up documents are *queued* (lag > 0, current 1500s threshold is correct) or *stranded* (lag ≈ 0, a per-state threshold split is justified). Read-only. Needs a real backlog to say anything: run it under bug #1's forced-CPU recipe plus `make replay-docs COUNT=10`, otherwise it exits 1 `INCONCLUSIVE` rather than guessing |
