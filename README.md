# document-llm-ops

A document-extraction pipeline built to production shape rather than demo shape, sized for a
~20k docs/day workload — and actually run, not just designed. Everything below is exercised
against real Postgres, real Kafka (Redpanda), a real GCS-compatible object store and a real
local LLM, on a laptop.

![Architecture: Ansible/ArgoCD provisioning a minikube cluster containing four bands — ingest and
ledger, text production with OCR fan-out and scatter-gather join, extraction with five quality
gates and the outbox, and an async reconciliation strip](docs/architecture.png)

## The parts worth looking at

- **A ledger with a real state machine.** `documents.state` moves only along a legal-move table,
  and a `_running` state is enterable only from its own `_pending`. Every re-drive therefore
  targets `_pending` and structurally cannot race a live worker.
- **Concurrency lives in SQL, not application logic.** `UPDATE ... WHERE state = ANY(allowed)
  RETURNING` *is* the lock. The scatter-gather join that reassembles sharded OCR increments and
  compares under one row lock — never `SELECT count(*)`, which can read a stale count while a
  concurrent final shard commits.
- **A transactional outbox**, so a document is never marked done without its message being
  durably queued, and never marked published without delivery being confirmed.
- **Deterministic quality gates.** Five of them, three blocking. The one that defeats prompt
  injection is `arithmetic` — it recomputes totals from line items and trusts arithmetic over
  anything the model claims.
- **A reconciliation tier that recovers lost work** — sweeper, orphan detector, DLQ replay,
  dead man's switch, synthetic canary — none of which consume Kafka, deliberately: the bus is
  one of the things this tier exists to recover from.
- **Operator lanes whose guardrails are structural.** The read-only replay lane connects as a
  role with no `INSERT` grant, so it is incapable of writing rather than merely choosing not to.

Documents move through four in-flight states and settle in one of three. Only `complete` is truly
final; `review` and `failed` are parking states that are legal to leave, and the operational
question is whether anything ever does:

![Document end states: complete is absorbing and posts downstream; review is entered by triage
rejects, exhausted gates, duplicate detection or the kill switch, and nothing automatic ever leaves
it; failed is entered by the attempt cap and left only when the build or prompt version
changes](docs/document-states.png)

`review` and `failed` are not bugs — they are the pipeline correctly declining to auto-trust a
result. A run where everything lands in `complete` would be suspicious for fixtures designed to
exercise the reject paths. The states that indicate trouble are the *in-flight* ones.

## Status

**Live-verified end to end**, twice over via two paths that share no infrastructure: `make e2e`
(host processes, deterministic extraction, ~15s) and `make e2e-k8s` (Kubernetes Deployments, real
LLM, entirely ArgoCD-driven, with KEDA observed scaling a Deployment 1→3 under a real backlog).
86 tests pass against real infra.

The interesting part of the history is that most of it was debugging: a liveness probe killing
healthy-but-slow pods, Ollama silently degrading to CPU, and a quality gate that passed on a
*missing* value and thereby disabled the model-tier escalation it was meant to guard. Those
write-ups are in [HISTORY.md](HISTORY.md).

## Running it

Needs Docker with Compose v2, `psql`, and [`uv`](https://docs.astral.sh/uv/) (no manual venv — it
resolves the interpreter from `.python-version`). `make e2e-k8s` additionally needs `minikube`,
`kubectl`, `ansible`, `terraform`, the `argocd` CLI, and a checkout of `mlops-llm-repo` as a
sibling directory.

```bash
make install                 # uv sync
cp .env.example .env         # check for a port conflict with any local postgres first
make e2e                     # ~15s: infra, fixtures, consumers, drain, test
```

- **`make e2e`** — fast loop, host processes, deterministic extraction, docker-compose infra.
- **`make e2e-k8s`** — full loop, entirely in-cluster and **destructive**: `minikube delete`,
  rebuilds the sibling repo's stack via its own `terraform apply`, deploys everything through one
  ArgoCD Application, then drains real-LLM fixtures and runs the canary. ~15–20 min.
- **`make summary`** — a real pass/fail gate, not a printout: `✅ RUN COMPLETE` and exit 0 only if
  every document reached a terminal state.

Every Makefile target carries a comment in place; every one that runs Python does so via `uv run`.

## Repo layout

```
docpipeline/                  the pipeline itself — see docpipeline/README.md
  config.py                     cross-cutting settings, read everywhere
  core/                         ledger, outbox, gates, models, artifact
  infra/                        gcs.py, kafka_utils.py
  text/                         pdf_utils.py, ocr_engine.py
  stages/                       the pipeline stages proper
  reconciliation/               sweeper, orphan detector, canary, operator
fixtures/                     generate_fixtures.py, content.py — fixtures/README.md
migrations/                   001_init.sql, 002_operator_lanes.sql
local_scripts/                run_local.py, wait_for_drain.py, replay_docs.py
tests/                        pytest suite — tests/README.md
k8s/                          the Helm chart make e2e-k8s deploys — k8s/README.md
argocd/                       the two Application objects
ansible/                      the sole orchestration layer — ansible/README.md
presentations/                a scrollytelling architecture walkthrough (open the .html directly)
AGENT.md                      implementation gotchas and mechanics (current-state)
HISTORY.md                    how it was built and debugged, including the closed bug register
```

## Known limitations / deliberately deferred

- **EU locale amounts/dates aren't parsed** by the deterministic extractor's regex backend — a
  real model tier doesn't have this problem; it's a backend limitation, not a mechanics gap.
- **No ensemble/consensus tier.** The role-swap gap (grounding can't verify seller/buyer
  assignment) is proven to exist in `test_extraction_funnel.py` but not remediated.
- **No central token governor** — nothing rate-limits concurrent LLM calls; local volume never
  makes one necessary.
- **No CI** enforcing the quality gates or the read-only/break-glass role split — both are proven
  by the test suite and by running live, not gated in a pipeline. A deliberate gap.

See the presentation's "What's demo-only" appendix for the full production-readiness gap list
(secrets, data durability, network policy, cluster lifecycle), and [AGENT.md](AGENT.md) for the
implementation-level gotchas that aren't obvious from reading the code once.
