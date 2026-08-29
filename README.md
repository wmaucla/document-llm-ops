# document-llm-ops

A local, runnable implementation of [the 100k docs/day document pipeline design](../mlops-llm-repo)
— Part I (production design) and Part II ("Local replication on minikube") of that design doc,
now covering the **entire local build order, steps 0–10**.

This is **not** the toy invoice-extraction eval harness in `mlops-llm-repo` (that's the design
doc's own "toy sibling," [[llmops-invoice-extraction-design]]). This is the production-shaped
pipeline: a document ledger with a real state machine, a transactional outbox, a scatter-gather
join for sharded OCR, a cascading extraction funnel gated by deterministic quality gates, a
reconciliation tier that recovers lost work, a real LLM tier behind the sibling repo's LiteLLM
gateway, KEDA-driven autoscaling in minikube, and the operator/break-glass lanes the design's
"Enforcement is structural, not policy" section describes — all exercised against real Postgres,
real Kafka (Redpanda), a real GCS-compatible object store, and (for step 8) a real small model,
not mocks.

**Status: live-verified end to end**, twice over — once as host processes (`make e2e`, mock LLM,
~15s) and once as Kubernetes Deployments with KEDA actually observed scaling a Deployment from 1
to 3 replicas under a real backlog (`make e2e-k8s`). 56 tests pass against real infra (not
mocked). See *What's verified*.

## Steps 0–7 vs 8–10 — what's genuinely proven vs what's wired but lighter-weight

Steps 0–7 (the ledger, outbox, join, gates, sweeper, orphan detector) are the correctness core
and are the most heavily exercised — dozens of live runs, 56 automated tests, every fixture
landing on its predicted terminal state. Steps 8–10 are real, working code, live-verified at
least once each, but by nature of what they are (autoscaling policy, operational tooling) they
don't have the same density of automated proof:

- **Step 8 (real LLM)** works end-to-end against the sibling repo's `litellm`/`ollama`/`langfuse`
  stack — confirmed live (`tests/test_real_llm_integration.py`) — and is the **default in the K8s
  deployment** (litellm is reachable in-cluster over DNS). `make e2e` (host-only, no minikube)
  still defaults to mock for its fast ~15s loop, despite a CPU-only 1B model taking ~170s/call.
- **Step 9 (KEDA)** was proven with a real burst (150 documents) that pushed `ocr-shard` from 1
  to 3 replicas and back down — genuinely observed, not just "the YAML applies." DLQ replay, the
  dead man's switch, and the canary are each covered by both a live manual run and automated
  tests.
- **Step 9b (operator lanes)** — `force_redrive`/`bulk_redrive`/`set_kill_switch` are fully
  tested (`tests/test_operator.py`), including the blast-radius cap and the reason requirement.
  There's no Argo Workflow wrapper around them (Argo Workflows is already running in this
  minikube cluster for the sibling repo, so wiring a `WorkflowTemplate` that just shells out to
  `python -m docpipeline.operator` would be additive, not a redesign — not done here since the
  interesting part is the guardrails inside `operator.py`, not the YAML that invokes it).

## Tool substitutions from the design doc

| Design doc says | This repo uses | Why |
|---|---|---|
| minikube + Tilt for the whole stack | `docker compose` for stateful infra (Postgres/Redpanda/fake-gcs-server/Redis) + **K8s Deployments in minikube for the app tier** (`make e2e-k8s`) | Full infra migration to minikube would have meant re-deriving Postgres/Redpanda/GCS-emulator Helm charts for zero additional correctness insight. Pods reach the host's docker-compose services via `host.minikube.internal` (verified reachable — see `k8s/configmap.yaml`) and reach the sibling repo's `litellm`/`langfuse` via normal in-cluster DNS. |
| Pub/Sub notification → orphan detector as fallback | Orphan detector *is* the ingest path (10s loop) | This one's the design doc's own recommendation, not a deviation — fake-gcs-server has no bucket-notification wiring at all. |
| Real Anthropic API behind LiteLLM | `docpipeline/llm_client.py` calling the sibling repo's already-running, already-Langfuse-wired `litellm` Deployment (Ollama-backed, free) — the K8s default; `docpipeline/mock_llm.py` stays the default for host-only `make e2e` | Step 8. Real end-to-end LLM calls work; mock stays the host-loop default purely for speed, not because the wiring is incomplete. |
| Real Tesseract OCR | `docpipeline/ocr_engine.py`'s `MockOcrEngine` (default); `TesseractOcrEngine` opt-in via `OCR_ENGINE=tesseract` | Matches the design doc's own "Tier A mock is the default for ~90% of tests" recommendation. Keyed by `(doc_id, page_no)` rather than the rendered image's checksum — a PDF-embedded placeholder page re-rasterises to different bytes than whatever was rendered at fixture-generation time, so image-checksum keying isn't robust locally. |
| Argo CronWorkflow reconcilers | Plain Python loops (`sweeper.py`, `orphan_detector.py`, `dlq_replay.py` as a cron-style script) | Same SQL, same batch-cap/SKIP LOCKED logic; Argo's scheduling adds nothing to what's being proven here. |
| KEDA | Real KEDA, installed via its own ArgoCD Application (`argocd/keda-application.yaml`, pointed straight at kedacore's upstream Helm chart — not a raw `helm install`), real `ScaledObject`s on Kafka consumer lag | `k8s/keda.yaml`. `lagThreshold: "1"` is set low on purpose — a production threshold would never trip at local fixture volume, and the whole point of building this locally is exercising the mechanism, not leaving it unexercised. |
| Argo Workflow operator/break-glass lanes | `docpipeline/operator.py`, invoked as a CLI or a library — no Argo wrapper | See *Steps 0–7 vs 8–10* above. |

## What's verified

### The fixture set (steps 0–7 core)

Run `make e2e` (or the manual sequence in *Running it*) and every one of the 14 generated
fixtures reaches exactly the terminal state the design doc predicts for it, live, against real
Postgres/Redpanda/fake-gcs-server:

| Fixture | Reaches | Proves |
|---|---|---|
| `digital_clean` | `complete` (tier-0, zero OCR) | The $0 text-layer fast path |
| `digital_garbage_text_layer` | `complete` (via OCR fallback) | text-sanity gate → fall-through → 5-shard split/join → still completes |
| `one_page_scan` | `complete` | Single-shard fast path (`shards_total=1`) |
| `three_page_scan` | `complete` | Real split + 3 shards + the scatter-gather join — text reassembled from 3 fragments |
| `twenty_five_page_scan` | `review` (page_ceiling_exceeded) | Hard page ceiling, no OCR/LLM spent |
| `encrypted_pdf` | `review` (encrypted_pdf) | Triage doesn't crash on an unreadable page tree |
| `corrupt_pdf` | `review` (corrupt_pdf) | Same, for a truncated file |
| `zero_byte` | `failed` (terminal, no retry) | |
| `injected_footer` | `complete` (control case — no malicious model behaviour registered) | The injection defense itself is proven in `tests/test_extraction_funnel.py`, where a registered `injected_total` mock-LLM behaviour shows grounding *passing* and arithmetic *failing* on the exact same text |
| `no_line_items` | `review` (`gates_exhausted`) | `arithmetic` → `inconclusive`, never treated as a pass |
| `credit_memo` | `complete` | Negative-total handling, `arithmetic` correctly `not_applicable` for non-invoices |
| `rescanned_duplicate` | `review` (`business_dedupe` fail) | Checksum dedupe can't catch this — different bytes, same `(vendor, invoice_no)` |
| `eu_locale` | `review` | **Known limitation**, not a bug — see below |
| `role_swap_candidate` | `complete` (control case) | The role-assignment gap itself is proven in `tests/test_extraction_funnel.py` via a registered `swapped_roles` behaviour: grounding passes on the swap |

This same run is also verified running as **K8s Deployments** in minikube (`make e2e-k8s`) —
identical mechanics, identical terminal states, reached via `python -m docpipeline.X` running in
pods instead of host processes.

### `tests/` — 56 tests, `pytest tests/ -v` (mock mode; ~10s)

Against real Postgres, with real threads for the concurrency cases:

- Illegal state transitions are rejected (`text_pending -> extract_pending`, any `*_running`
  not entered from its own `*_pending`, `complete` has no outbound transitions)
- The scatter-gather join fires exactly once under **concurrent** final shards (the specific
  lost-join hazard a bare `SELECT count(*)` has and `UPDATE ... RETURNING` doesn't)
- Duplicate shard delivery doesn't double-increment
- The outbox closes the dual-write window (kill-after-commit-before-relay is unrecoverable
  without it) and two relay replicas racing for the same row publish it exactly once (real
  threads, not a sequential call — a sequential pair can't exercise the actual lock contention)
- The stuck-state sweeper re-drives lost work, republishes *only missing shards* (not
  completed ones), and DLQs a document only once its attempt cap is exceeded — and only from
  a `*_running` state, never a `*_pending` one (a lost publish isn't a failed attempt)
- First-writer-wins holds under divergent redelivery
- A missing shard-output object routes to `review` via the completeness gate, not an infinite
  retry loop
- The `pipeline_ro` role's inability to `INSERT` into `outbox` is a real, tested grant, not an
  assumption about code paths
- **(step 9b)** Break-glass re-drive requires a reason, writes an audit row, and reuses the
  sweeper's own `redrive_document` function; bulk re-drive enforces (and can be told to override)
  a blast-radius cap; the auto-post kill switch is a live-toggleable DB flag, not an env var
- **(step 9b)** Force-redrive correctly refuses a document that failed *at triage*, before it was
  ever classified (e.g. a zero-byte upload) — there's nothing to route it to, it needs a fresh
  upload, not a retry (a real bug found and fixed during this build — see git history)
- **(step 9)** DLQ replay re-drives a `failed` document only when `build_sha`/`prompt_version`
  changed since the attempt that failed it, and is a no-op on a second failure at the same
  version
- **(step 9)** The dead man's switch reports unhealthy the moment something is ingested or
  in-flight with zero completions, and healthy when either nothing is happening or things are
  completing
- **(step 8, opt-in, not in the default suite)** `tests/test_real_llm_integration.py` — talks to
  the real `litellm` gateway; skipped unless `RUN_REAL_LLM_TESTS=1`, since one call took ~170s
  under load in verification

### KEDA — actually observed scaling, not just "the ScaledObject applies"

```
$ kubectl get hpa keda-hpa-docpipeline-ocr-shard
NAME                             REFERENCE                          TARGETS     MINPODS  MAXPODS  REPLICAS
keda-hpa-docpipeline-ocr-shard   Deployment/docpipeline-ocr-shard   3/1 (avg)   1        5        3
```
Pushed there by uploading 150 blank single-page documents at once — `ocr-shard` went 1 → 3
replicas under real Kafka consumer lag, then back down after the backlog drained (HPA's default
5-minute downscale-stabilization window applies, same as any HPA).

## Known limitations / deliberately deferred

- **EU locale amounts/dates aren't parsed.** `mock_llm.default_extract`'s regex-based
  ground-truth extractor is intentionally simple (it stands in for what a real LLM would parse
  correctly); `4.297,00 EUR` / `15.04.2026` fall outside it. A real LLM tier (step 8, opt-in)
  wouldn't have this problem — this is a mock-LLM limitation, not a pipeline-mechanics gap.
- **Tier-0 layout cache is a permanent stub** (`gate_results["layout_cache"]` is always
  `not_applicable`, "v1_stub_always_miss"). No vendor-layout-fingerprint cache is implemented.
- **No ensemble/consensus tier.** The role-swap gap (grounding can't verify seller/buyer
  assignment) is proven to exist but not remediated — the design doc scopes that as a
  gate-unverifiable-fields-only mechanism, deferred here.
- **No central Redis token governor.** Redis is stood up (per the design doc's infra list) but
  nothing reads or writes to it yet — there's no concurrent LLM call volume locally that would
  exercise a governor.
- **No PgBouncer / reserved reconciler connection pool.** Not needed at this connection count;
  `FM1` in the design doc is a real-scale concern.
- **No Argo Workflow wrapper for the operator lanes** — see *Steps 0–7 vs 8–10*.
- **Non-PDF multi-page TIFF isn't supported** — only single images (JPEG/PNG/TIFF) on the
  single-shard fast path, per the design doc's own "Non-PDF inputs" scope.
- **Fixture generation must happen before the K8s image is built**, not after: the mock-OCR
  ground-truth registry is baked into the image at build time. `make e2e-k8s` gets this ordering
  right; if you regenerate fixtures after `make image`, rebuild the image again before
  redeploying, or the OCR-path fixtures will see stale/unregistered ground truth.

## Repo layout

```
docpipeline/           the pipeline itself — see the file list below
  config.py              cross-cutting settings, read everywhere
  fixture_content.py     shared invoice text blocks (fixtures + tests)
  core/                  ledger.py, outbox.py, models.py, gates.py, artifact.py
  infra/                 gcs.py, kafka_utils.py — thin wrappers over the two external systems
  stages/                triage, pdf_utils, pdf_worker, ocr_engine, ocr_shard, extraction,
                         sink_stub, mock_llm, llm_client — the pipeline stages proper
  reconciliation/        sweeper, orphan_detector, dlq_replay, deadmans_switch, canary,
                         operator — everything that keeps the system healthy or fixes it by hand
fixtures/              generate_fixtures.py builds and uploads every fixture in the design
                       doc's fixture table; fixtures/generated/ (gitignored) holds the
                       manifest + the mock-OCR ground-truth registry
migrations/            001_init.sql (ledger schema + roles), 002_operator_lanes.sql
                       (break_glass_audit + feature_flags)
scripts/               reset.sh, init_db.sh, create_topics.py, run_local.py,
                       wait_for_drain.py, summarize.py
tests/                 pytest suite, run against real infra (see above)
k8s/                   configmap.yaml, deployments.yaml (8 Deployments), keda.yaml
Dockerfile             builds the image `make image` loads into minikube's docker daemon
docker-compose.yml     Redpanda, Postgres, fake-gcs-server, Redis
Makefile               the single entrypoint — `make help` for the full command list
```

`docpipeline/` maps directly onto the design doc's sections. The four subpackages mirror how
the design doc itself is organised: `core` is the ledger machinery every stage depends on,
`infra` is the two external systems, `stages` is the pipeline proper, and `reconciliation` is
everything under "Reconciliation and operations" plus the break-glass lane it depends on.

| File | Design doc section |
|---|---|
| `core/ledger.py` | The document ledger, the scatter-gather join, first-writer-wins, feature flags |
| `core/outbox.py` | The transactional outbox + the relay |
| `core/gates.py` | Quality gates (grounding, arithmetic, iban_mod97, completeness, plausibility, business_dedupe) |
| `core/models.py` | Tier 1 of the extraction funnel — the schema gate |
| `core/artifact.py` | Deterministic GCS paths + the canonical text-production artifact |
| `infra/gcs.py` | The storage wrapper — fake-gcs-server locally, no code branch for local vs prod |
| `infra/kafka_utils.py` | Producer/consumer/topic-admin helpers |
| `stages/triage.py` | Stage 0 · Triage |
| `stages/pdf_utils.py` | Text production (tier-0 pypdf + physical split mechanics) |
| `stages/pdf_worker.py` | The `pdf-worker` deployment (`text.embedded` + `ocr.split`) |
| `stages/ocr_shard.py` | The `ocr-shard` deployment + the join's "winner publishes, doesn't assemble" rule |
| `stages/ocr_engine.py` | "The OCR engine locally — two tiers, and mostly a mock" |
| `stages/extraction.py` | Stage 2 · Extraction funnel, lazy assembly, the funnel/tier/gate loop, the kill switch check |
| `stages/mock_llm.py` | "The mock LLM is a real component, not a stub" |
| `stages/llm_client.py` | Step 8 — the real LLM tier, behind the sibling repo's LiteLLM gateway |
| `stages/sink_stub.py` | The downstream contract's local stand-in |
| `reconciliation/sweeper.py` | Reconciler ① — the stuck-state sweeper; `redrive_document` is shared with `operator.py` |
| `reconciliation/orphan_detector.py` | Reconciler ② — the orphan detector (and the local ingest path) |
| `reconciliation/dlq_replay.py` | Reconciler ③ — "DLQ replay — daily, gated, never fully automatic" |
| `reconciliation/deadmans_switch.py` | "Dead man's switch — alerting on absence" |
| `reconciliation/canary.py` | "Hourly synthetic canary" |
| `reconciliation/operator.py` | "Operator and R&D entry points" — the read-only replay lane and the break-glass lane |

## Running it

Needs: Docker with Compose v2 (`docker compose version` should work — if you only have the
ancient `docker-compose` v1 binary, drop the v2 plugin into `~/.docker/cli-plugins/docker-compose`
from https://github.com/docker/compose/releases, no root needed), `psql`, [`uv`](https://docs.astral.sh/uv/)
(no separately-installed Python or manual venv needed — `uv` resolves the interpreter from
`.python-version` and manages the environment itself), `poppler-utils` + `tesseract-ocr` on the
host if you want the real-OCR opt-in path, and — only for `make e2e-k8s` — `minikube`, `kubectl`,
`ansible`, `terraform`, and the `argocd` CLI, plus a checkout of the sibling
[`mlops-llm-repo`](../mlops-llm-repo) as a sibling directory (`../mlops-llm-repo` relative to this
repo — see "Full cluster rebuild" below for why).

```bash
make install                 # uv sync — no venv to create or activate yourself
cp .env.example .env         # check for port conflicts with any local postgres/redis first
make e2e                     # up, init-db, topics, fixtures, run consumers, drain, test
```

Every Makefile target that runs Python does so via `uv run`, which transparently syncs the
environment against `pyproject.toml`/`uv.lock` first — there's nothing to `source activate` and
no `.venv/bin/...` path to reference directly. Running something by hand outside `make`? Prefix
it with `uv run`, e.g. `uv run pytest tests/ -v` or `uv run python3 -m docpipeline.stages.triage`.

`make help` lists every target. The two end-to-end paths:

- **`make e2e`** — fast loop, host processes, mock LLM. Steps 0–7's correctness core. ~15s.
- **`make e2e-k8s`** — full loop, and **destructive**: `minikube delete` + `minikube start`, then
  rebuilds the sibling `mlops-llm-repo`'s entire stack (ArgoCD, Argo Workflows, Ollama, LiteLLM,
  Langfuse) via *its own* `terraform apply` — genuinely clean-room every run, not a long-lived
  cluster reused across sessions. Only then does it build the image into minikube, install KEDA,
  deploy 8 Deployments + 2 `ScaledObject`s, drain a small real-LLM fixture subset, and run the
  synthetic canary against the live deployment. Deliberately drains 3 fixtures here, not the full
  14 — confirmed live that all 14 flowing through the real LLM path (one Ollama pod, effectively
  serial inference no matter how many extraction replicas exist) pushed a single canary well past
  20 minutes. The full 14 are already proven correct, fast and free, by `make e2e`'s mock-mode host
  loop; `make e2e-k8s`'s job is proving the *deployment* mechanics (ArgoCD, KEDA, real LLM
  connectivity), not re-proving per-fixture correctness under real-model latency. ~15-20 min typical
  (cluster rebuild dominates), but the canary's own SLO is a generous 900s on top of that — real
  Ollama inference latency is genuinely host-load-dependent, confirmed anywhere from ~150s to 7.5
  minutes for a *single* call depending on what else the host was doing. See "Full cluster rebuild"
  below, and `make cluster-rebuild` to run just that step on its own.

### Full cluster rebuild — why `make e2e-k8s` is destructive by design

Earlier in this repo's life, `make e2e-k8s` reused whatever minikube cluster and ArgoCD/KEDA
install already existed, on the theory that reuse is faster. In practice this let real bugs hide:
a `make reset` recreating Redpanda (new cluster ID) left already-running consumer pods with stale
Kafka clients that took an *observed* ~14 minutes to self-reconnect via librdkafka's own retry
logic — during which a canary or real ingest would silently fail with "never even triaged," which
reads exactly like a broken pipeline and isn't one. A hand-installed ArgoCD or a raw-`helm install`
KEDA release from an earlier debugging session could also linger indefinitely, diverging from what
this repo now declares (KEDA moved to its own ArgoCD Application — see the tool-substitutions
table below — specifically so nothing gets installed outside GitOps and silently drifts).

`make e2e-k8s` now pays the ~10-15 min cost of a real `minikube delete` + `minikube start`, then
re-provisions the sibling `mlops-llm-repo`'s entire stack via *that repo's own*
`terraform -chdir=tf apply -auto-approve` — the same command its own `scripts/deploy.sh` and
`scripts/end_to_end_pipeline.sh` use, so ArgoCD/Ollama/LiteLLM/Langfuse come up exactly the way
that repo already proves out, not a second reimplementation of its bring-up logic here. This repo
does *not* invoke that sibling's full `end_to_end_pipeline.sh` (which also builds its own app
image and runs its own eval workflow) — just the `terraform apply`, since that alone provisions
every shared dependency this repo actually reaches over cluster DNS.

Inspect results any time with `make summary`, or directly:

```bash
psql -c "SELECT doc_id, state, vendor, invoice_no FROM documents ORDER BY created_at"
psql -c "SELECT * FROM posted_documents"
```

Other useful targets:

```bash
make reset            # truncate ledger, clear GCS, wipe Redpanda, flush Redis
make canary            # inject + track one synthetic document end to end
make dlq-replay        # re-drive failed docs whose build_sha/prompt_version changed
make deadmans-switch   # check for total silence (exits 1 if unhealthy — cron/alerting friendly)
make k8s-status        # docpipeline pods + ScaledObjects
make undeploy          # remove everything make deploy/e2e-k8s created
make test-real-llm     # opt-in step-8 integration test — start `kubectl port-forward svc/litellm 4000:4000` first
```

**`make reset`** (== `ansible-playbook ansible/site.yml --tags reset`) truncates the ledger, clears every GCS prefix, recreates
Redpanda (wiping all topics/offsets/consumer groups — it has no persistent volume by design), and
flushes Redis. Meant to be pressed constantly, per the design doc's own "reset script — required
for repeatable runs." Note: recreating Redpanda gives it a new cluster ID, which any **live** K8s
consumer pods correctly treat as fatal and crash-restart on (Kubernetes' restart-on-crash is the
correct recovery here, not a bug) — expect a few pod restarts if you run `make reset` while
`make e2e-k8s`'s Deployments are up.

**Don't run `make e2e` (or bare `pytest`) concurrently with another `make e2e`/`make reset`
invocation against the same infra.** Both truncate the ledger and (for `reset`) recreate
Redpanda; overlapping runs will see each other's in-flight writes and either time out waiting to
drain or hit spurious lock contention. Real bugs were found this way during development (see git
history) but by the time you're running this, it's just noise, not signal.

If ports 5432/6379 are already taken by something else on your machine (the defaults here are
remapped to 55432/6380 for exactly that reason), edit `docker-compose.yml` and `.env` together.
