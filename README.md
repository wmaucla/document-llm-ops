# document-llm-ops

A local, runnable document-extraction pipeline built to production shape rather than demo shape,
sized for a ~20k docs/day workload.

This is **not** the toy invoice-extraction eval harness in `mlops-llm-repo`. This is the
production-shaped pipeline: a document ledger with a real state machine, a transactional outbox, a
scatter-gather join for sharded OCR, a cascading extraction funnel gated by deterministic quality
gates, a reconciliation tier that recovers lost work, a real LLM tier behind the sibling repo's
LiteLLM gateway (with Langfuse trace + score visibility), KEDA-driven autoscaling, and
operator/break-glass lanes whose guardrails are structural rather than policy — all exercised
against real Postgres, real Kafka (Redpanda), a real GCS-compatible object store, and a real small
model, not mocks.

![Architecture: Ansible/ArgoCD provisioning a minikube cluster containing four bands — ingest and
ledger, text production with OCR fan-out and scatter-gather join, extraction with five quality
gates and the outbox, and an async reconciliation strip](docs/architecture.png)

Documents move through four in-flight states and settle in one of three. Only `complete` is truly
final; `review` and `failed` are parking states that are legal to leave, and the question is
whether anything ever does:

![Document end states: complete is absorbing and posts downstream; review is entered by triage
rejects, exhausted gates, duplicate detection or the kill switch, and nothing automatic ever leaves
it; failed is entered by the attempt cap and left only when the build or prompt version
changes](docs/document-states.png)

**Status: live-verified end to end**, twice over via two genuinely separate paths that share no
infrastructure — `make e2e` (host processes, mock LLM, ~15s) and `make e2e-k8s` (Kubernetes
Deployments, real LLM, entirely ArgoCD-driven, KEDA actually observed scaling a Deployment from 1
to 3 replicas under a real backlog). 82 tests pass against real infra, not mocked. See
[tests/README.md](tests/README.md) and the walkthrough in
[`presentations/`](presentations/llmops-document-pipeline-workflow.html).

For implementation-level gotchas, mechanics, and open bugs, see [AGENT.md](AGENT.md); for the
session-by-session story of how each was found and fixed, see [HISTORY.md](HISTORY.md) — this file
stays high-level.

## Repo layout

```
docpipeline/                  the pipeline itself — see docpipeline/README.md
  config.py                     cross-cutting settings, read everywhere
  fixtures/content.py            shared invoice text blocks (fixtures + tests)
  core/                         ledger, outbox, gates, models, artifact — docpipeline/core/README.md
  infra/                        gcs.py, kafka_utils.py — docpipeline/infra/README.md
  text/                         pdf_utils.py, ocr_engine.py — docpipeline/text/README.md
  stages/                       the pipeline stages proper — docpipeline/stages/README.md
  reconciliation/               sweeper, orphan detector, canary, operator — docpipeline/reconciliation/README.md
fixtures/                     generate_fixtures.py — fixtures/README.md
migrations/                   001_init.sql, 002_operator_lanes.sql — migrations/README.md
local_scripts/                 run_local.py, wait_for_drain.py, etc. — local_scripts/README.md
tests/                        pytest suite — tests/README.md
k8s/                          the Helm chart make e2e-k8s deploys — k8s/README.md
argocd/                       the two Application objects — argocd/README.md
ansible/                      the sole orchestration layer — ansible/README.md
presentations/                a scrollytelling architecture walkthrough (open the .html directly)
Dockerfile                    builds the image make image loads into minikube's docker daemon
docker-compose.yml             Postgres/Redpanda/fake-gcs-server/Redis — make e2e (host mode) only
Makefile                      the single entrypoint — make help for the full command list
AGENT.md                      implementation gotchas, mechanics, open bugs (current-state)
HISTORY.md                    session-by-session log of past debugging and build work
```

## Running it

Needs: Docker with Compose v2 (`docker compose version` should work), `psql`,
[`uv`](https://docs.astral.sh/uv/) (no separately-installed Python or manual venv needed — `uv`
resolves the interpreter from `.python-version` and manages the environment itself),
`poppler-utils` + `tesseract-ocr` on the host if you want the real-OCR opt-in path, and — only for
`make e2e-k8s` — `minikube`, `kubectl`, `ansible`, `terraform`, and the `argocd` CLI, plus a
checkout of `mlops-llm-repo` as a sibling directory.

```bash
make install                 # uv sync — no venv to create or activate yourself
cp .env.example .env         # check for port conflicts with any local postgres/redis first
make e2e                     # up, init-db, topics, fixtures, run consumers, drain, test
```

Every Makefile target that runs Python does so via `uv run` — prefix anything you run by hand the
same way, e.g. `uv run pytest tests/ -v` or `uv run python3 -m docpipeline.stages.triage_1`.

`make help` lists every target. The two end-to-end paths:

- **`make e2e`** — fast loop, host processes, mock LLM, docker-compose infra. Steps 0–7's
  correctness core. ~15s.
- **`make e2e-k8s`** — full loop, entirely in-cluster (no docker-compose), and **destructive**:
  `minikube delete` + `minikube start`, rebuilds the sibling `mlops-llm-repo`'s entire stack via
  *its own* `terraform apply`, then builds the image, deploys everything through one ArgoCD
  Application (app tier, KEDA, Postgres/Redis/Redpanda/fake-gcs-server, Prometheus/Grafana — see
  [k8s/README.md](k8s/README.md)), drains a small real-LLM fixture subset, and runs the synthetic
  canary against the live deployment. ~15-20 min typical (cluster rebuild dominates).

Other useful targets:

```bash
make reset             # truncate ledger, clear GCS, wipe Redpanda, flush Redis (make e2e only)
make canary             # inject + track one synthetic document end to end
make dlq-replay         # re-drive failed docs whose build_sha/prompt_version changed
make deadmans-switch    # check for total silence (exits 1 if unhealthy — cron/alerting friendly)
make k8s-status         # docpipeline pods + ScaledObjects
make undeploy           # remove everything make deploy/e2e-k8s created
make summary            # print the final per-document state report
```

Inspect ledger state any time:

```bash
psql -c "SELECT doc_id, state, vendor, invoice_no FROM documents ORDER BY created_at"
```

If ports 5432/6379 are already taken by something else on your machine (the defaults here are
remapped to 55432/6380 for exactly that reason), edit `docker-compose.yml` and `.env` together.

## How to tell a run actually finished — and what "finished" means

**`make summary`** is the command — run it any time you want to know if the last run actually
finished (host mode; for a live in-cluster check use
`kubectl exec deploy/docpipeline-triage -- python local_scripts/summarize.py`). It's a real
pass/fail gate, not just a printout: it prints `✅ RUN COMPLETE` and exits 0 if every document
reached one of the three terminal states below, or `❌ RUN INCOMPLETE` (listing exactly which
documents, and their current state) and exits 1 if anything is still in-flight. It's also the last
step of both `make e2e` and `make e2e-k8s` themselves — both poll
(`local_scripts/wait_for_drain.py`) until the ledger actually settles before it runs, so it never
gates on a lucky snapshot.

`make summary` reflects the *entire* shared ledger, not just "the last thing you ran" — if you've
been running `make test`/pytest against the same local Postgres, `tests/conftest.py` now truncates
the ledger both before *and* after the suite, so it won't leave debris behind. But any host
consumer process that didn't get shut down cleanly (see AGENT.md's PID-capture gotcha) will keep
silently processing documents in the background and show up here too — if `make summary` reports
something you don't recognize, `ps aux | grep docpipeline` before assuming it's a pipeline bug.

Every document ends up in exactly one of three states (`documents.state` in the ledger):

| State | Means | Reachable from |
|---|---|---|
| `complete` | Extracted, passed every blocking quality gate (grounding, arithmetic, business_dedupe), auto-posted. Fully automated, no human involved. | The extraction funnel, on a clean gate pass |
| `review` | Processing happened but the system deliberately didn't trust the result enough to auto-post. | A triage-time reject needing human judgment (encrypted/corrupt PDF, oversized) **or** the funnel exhausting every tier/repair attempt without a clean gate pass **or** the auto-post kill switch **or** a duplicate caught at commit time |
| `failed` | A harder, earlier rejection — the upload itself was never valid (zero-byte, unsupported MIME) — or an unrecoverable error. | Triage, mostly before any real processing starts |

**`review` and `failed` are not failures of this repo's code** — they're the pipeline correctly
declining to auto-trust something, which is the whole point of the quality-gate design (see
"The problem" in the presentation). A run where every document lands in `complete` would actually
be suspicious for real-model-mode fixtures that are *designed* to exercise the reject/review paths.
What `summarize.py` actually checks for is **in-flight** states —
`text_pending`/`text_running`/`extract_pending`/`extract_running` — which mean processing hasn't
settled yet. All three of the states above are fine outcomes; a document still in one of the
in-flight states after the drain-and-summary steps means something is actually stuck.

`review` and `failed` aren't dead ends either — the sweeper and the operator's break-glass lane can
redrive both of them back to `extract_pending` (see the architecture diagram's dashed "redriven"
loop). See [AGENT.md](AGENT.md)'s "The state machine and the scatter-gather join" for the full
transition table.

## Known limitations / deliberately deferred

- **EU locale amounts/dates aren't parsed** by the mock LLM's regex-based extractor — a real LLM
  tier wouldn't have this problem; it's a mock-LLM limitation, not a pipeline-mechanics gap.
- **No ensemble/consensus tier.** The role-swap gap (grounding can't verify seller/buyer
  assignment) is proven to exist (`test_extraction_funnel.py`) but not remediated.
- **No central Redis token governor** — Redis is stood up as part of the infra tier, but nothing
  reads or writes to it yet; no concurrent LLM call volume locally would exercise one.
- **No Argo Workflow wrapper for the operator lanes** — Argo Workflows already runs in this same
  minikube cluster for the sibling repo, so this would be additive, not a redesign; not done here
  since the interesting part is the guardrails inside `operator.py`, not the YAML that invokes it.
- **No CI enforcing the quality gates or the read-only/break-glass role split** — proven only by
  the test suite and by running live, not gated in CI. A known, deliberate gap.

See `presentations/llmops-document-pipeline-workflow.html`'s "What's demo-only" appendix for the
full production-readiness gap list (secrets, data durability, network policy, cluster lifecycle).
