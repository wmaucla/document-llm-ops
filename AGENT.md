# AGENT.md

Notes for an AI agent (or new contributor) picking up this repo cold. For the "why" behind the
overall design, see [README.md](README.md) — this file is the implementation-level gotchas and
mechanics that aren't obvious from reading the code once, several of them confirmed only by
running the thing live and watching it misbehave.

## Handoff: where this stands right now

**Confirmed solid, no open questions:**
- `make e2e` (host, mock mode) — 56/56 tests pass, all 14 fixtures reach correct terminal states,
  ~15s, stable across many repeated runs this session.
- The K8s *deployment mechanics*: ArgoCD sync via `--core` (no port-forward), KEDA fully
  ArgoCD-managed, all 8 Deployments + 2 `ScaledObject`s deploy and become available, Kafka clients
  correctly reconnect after a Redpanda recreate. Each of these was a real, reproduced, root-caused
  bug this session — see the sections below for each one's fix.

**Not yet cleanly verified — pick this up first:**
- **GPU passthrough was just added and has never been run.** The host has an NVIDIA RTX 2080 Ti,
  confirmed working with `docker run --gpus all` and the NVIDIA Container Toolkit already installed
  — this is the actual fix for the latency problem below, not more SLO tuning. `site.yml`'s minikube
  start now passes `--gpus=all`, and the sibling `mlops-llm-repo`'s `k8s/ollama.yaml` now requests
  `nvidia.com/gpu: 1` (stock `ollama/ollama` image auto-detects CUDA, no image change needed).
  **Next step: run `make e2e-k8s` and confirm the Ollama pod actually schedules with the GPU
  (`kubectl exec -n default deploy/ollama -- nvidia-smi` should show the GPU) and that real
  inference drops from 150s–7.5min down to low single-digit seconds.** If the pod fails to schedule,
  check `kubectl describe pod` for GPU-related scheduling errors first — minikube's device plugin
  setup for `--gpus=all` was confirmed to exist (the flag is supported and documented for this exact
  driver/runtime combo) but was never actually exercised end to end this session.
- **No green real-mode canary run has actually been observed end to end in K8s** — every attempt
  this session either hit an infrastructure bug (now fixed) or ran into host-load-dependent CPU
  inference latency (150s–7.5min for a single call). Individual real LLM calls *did* succeed (other
  documents reached `complete`), so the mechanism works. GPU passthrough above should make this
  moot, but confirm the canary genuinely passes rather than just assuming GPU fixes it.
- **The 20s consumer-group settle pause after rollout-restart is unproven.** Added after one
  observed stranded-message incident, but every later failure traced back to CPU-inference latency
  instead, so this specific fix was never isolated and re-confirmed on its own.
- Consider adding a lightweight direct-Ollama smoke test (call `docpipeline.stages.llm_client.extract()`
  standalone, no Kafka/K8s involved) as a *second*, faster health check alongside the canary —
  proves Ollama connectivity in isolation without depending on consumer-group mechanics. Discussed,
  not implemented.

**Environment note:** the minikube cluster and docker images were torn down and pruned at the end
of this session (see git/shell history if you need exact commands) — `make e2e-k8s` starts fully
fresh either way, so this doesn't block anything, just means the next run pays the full ~15-20 min
cold-start cost (including re-pulling images that were flushed).

## What this repo is

A local, runnable implementation of [the 100k docs/day document pipeline design](../mlops-llm-repo)
(see [[document-llm-ops-production-design]]) — steps 0–10 of that doc's own build order,
covering the correctness core (ledger, outbox, scatter-gather join, quality gates, sweeper, orphan
detector) through the operational tier (real LLM, KEDA autoscaling, DLQ replay, dead man's switch,
canary, operator/break-glass lanes). Not the toy invoice-extraction eval harness in
`mlops-llm-repo` — this is the production-shaped pipeline, exercised against real Postgres, real
Kafka (Redpanda), a real GCS-compatible store, and (opt-in) the sibling repo's real local LLM
stack, not mocks.

1. **State machine, not a queue of independent jobs.** `documents.state` only moves along
   `ALLOWED_TRANSITIONS` in `docpipeline/core/ledger.py` — the load-bearing invariant is that a
   `_running` state is only enterable from its own `_pending` state, so every re-drive (sweeper,
   DLQ replay, break-glass) targets `_pending`, never `_running`, and can never race a live worker.
2. **Everything that matters is a guarded SQL statement, not application logic.** `transition()`
   is `UPDATE ... WHERE state = ANY(allowed) RETURNING state` — the row lock and the `RETURNING`
   clause *are* the concurrency control. `record_shard_and_maybe_join()` (the scatter-gather join)
   never does `SELECT count(*)`, because that read can observe a stale count while a concurrent
   final shard is mid-commit; only `UPDATE ... RETURNING shards_done, shards_total` is safe.
3. **Two roles, enforced by grants, not by convention.** `pipeline_ro` has no `INSERT` on `outbox`
   and no Kafka producer creds — `tests/test_read_only_role.py` asserts the grant is actually
   absent. This is what makes the operator's read-only replay lane *structurally* incapable of
   touching production, not a lane that merely happens not to call `enqueue()`.
4. **Deploys go through ArgoCD, KEDA included.** See "ArgoCD: both apps, no exceptions but two"
   below — this was a mid-build migration away from raw `helm install`/`kubectl apply`, done after
   the correctness core was already proven, so don't be surprised the git history (if this were
   committed incrementally) would show Ansible/ArgoCD arriving late.

## Repo layout

- `docpipeline/core/` — `ledger.py` (state machine + outbox + scatter-gather join, all raw SQL),
  `outbox.py` (polling relay: `SELECT ... FOR UPDATE SKIP LOCKED` batch, publish, mark posted),
  `gates.py` (five deterministic quality gates — see below), `models.py`, `artifact.py` (GCS
  read/write helpers for OCR page text and shard output).
- `docpipeline/infra/` — `gcs.py`, `kafka_utils.py` (thin wrappers, no business logic).
- `docpipeline/stages/` — one module per pipeline stage: `triage.py` (ingest + classify +
  dispatch), `pdf_worker.py`/`ocr_shard.py` (text production), `extraction.py` (the funnel:
  mock/cheap/strong tiers gated at each step), `llm_client.py` (real LLM tier, calls the sibling
  repo's `litellm` Deployment), `mock_llm.py` (default), `ocr_engine.py` (`MockOcrEngine` default,
  `TesseractOcrEngine` opt-in), `sink_stub.py`, `pdf_utils.py`.
- `docpipeline/reconciliation/` — `sweeper.py` (stuck-state recovery, batch-capped,
  `SKIP LOCKED`), `orphan_detector.py` (the actual ingest loop — GCS has no bucket-notification
  wiring locally, so this polls `inbox/` every 10s and *is* the design doc's own recommended
  fallback, not a deviation), `dlq_replay.py`, `deadmans_switch.py`, `canary.py`, `operator.py`
  (read-only + break-glass lanes — see "The two operator lanes" below).
- `config.py`, `fixture_content.py` — stay top-level, not subpackaged: `config.py` is imported by
  every other module (subpackaging it would just add an import hop with no grouping benefit), and
  `fixture_content.py` is shared fixture text, not pipeline logic.
- `ansible/site.yml` — the only orchestration layer (see "Ansible task ordering" below). `Makefile`
  targets are thin `ansible-playbook --tags <name>` aliases; there is no bash-script orchestration
  left (`scripts/reset.sh`/`init_db.sh` were deleted once Ansible fully covered them).
- `argocd/application.yaml`, `argocd/keda-application.yaml` — the two ArgoCD Applications. See
  "ArgoCD" below for why there are two and why both are still applied via raw `kubectl`.
- `k8s/` — `configmap.yaml`, `deployments.yaml` (8 Deployments), `keda.yaml` (2 `ScaledObject`s on
  Kafka consumer lag) — this whole directory is what `docpipeline`'s Application syncs.
- `scripts/` — `run_local.py` (host-process orchestrator for `make e2e`, has its own SIGTERM
  handler — see "PID-capture gotcha" below), `wait_for_drain.py`, `create_topics.py`,
  `summarize.py`.

## The state machine and the scatter-gather join

`ALLOWED_TRANSITIONS` in `ledger.py` is the entire legal-move table:
`text_pending→text_running→{extract_pending | text_pending | review | failed}`,
`extract_pending→extract_running→{complete | review | extract_pending | failed}`,
`review→extract_pending`, `failed→{text_pending | extract_pending}`. `transition()` raises
`IllegalTransition` rather than silently no-op'ing on an illegal move — if you add a new caller,
it *will* crash loudly the first time it races a state it doesn't expect, which is the intended
failure mode (better than a document quietly getting stuck).

`record_shard_and_maybe_join()` is the one piece of code in this repo where getting the SQL wrong
is a genuine correctness bug, not just a style nit: it inserts into `document_shards` with
`ON CONFLICT DO NOTHING RETURNING shard_idx` (so duplicate shard deliveries are free no-ops), then
`UPDATE documents SET shards_done = shards_done + 1 ... RETURNING shards_done, shards_total` and
compares those two numbers *from the same row lock* to decide who "wins" the join. Only the winner
transitions the document and enqueues `ocr.completed`. Don't refactor this into two separate
queries — the whole point is that the increment and the comparison happen under one lock.

## The five quality gates (`docpipeline/core/gates.py`)

`grounding`, `arithmetic`, `iban_mod97`, `plausibility`, `business_dedupe` — each returns one of
`pass | fail | inconclusive | not_applicable`. `BLOCKING_GATES = {grounding, arithmetic,
business_dedupe}`; `ON_INCONCLUSIVE` decides per-gate whether `inconclusive` blocks or passes
through (`iban_mod97` allows, everything blocking-capable blocks). The gate that actually defeats
prompt injection is `arithmetic`, not `grounding` — a model can inject text that grounds cleanly
but still lie about the total; `arithmetic` recomputes the subtotal from `line_items` independently
and only trusts arithmetic, not the model's own claimed total. `arithmetic.applies_to` is
`doc.doc_type == "invoice"`, and `doc_type` is set by `classify_doc_type()` at *triage* time from a
keyword heuristic — before extraction ever runs — specifically so a model can't evade the gate by
omitting `line_items` (an `applies_to` predicate must never depend on the thing being checked).
`business_dedupe` catches what the content-checksum primary key can't: a rescanned or re-emailed
duplicate has different bytes (different `doc_id`) but the same `(vendor, invoice_no)`.

## The two operator lanes (`docpipeline/reconciliation/operator.py`)

Read-only lane (`replay_documents`) connects as `pipeline_ro` — structurally incapable of writing,
per the grants point above. Break-glass lane (`force_redrive`, `bulk_redrive`, `set_kill_switch`)
connects as `pipeline_rw`, requires a non-empty `reason` string (`_require_reason` raises
`BreakGlassError` otherwise), writes a `break_glass_audit` row, and enforces a blast-radius cap on
bulk actions (`BlastRadiusExceeded` unless `approved=True`). `force_redrive` calls
`sweeper.redrive_document` — the *same* function the sweeper itself calls, not a second write path,
so a write bug can't diverge between the automatic and manual paths.

**Confirmed-live bug, now guarded:** `_redrive_target_state(doc_id, row)` needs `row["page_count"]`
to route correctly — but a zero-byte upload fails at triage *before* `page_count` is ever set
(`page_count IS NULL`), so DLQ-replaying it used to crash with a bare `TypeError` deep inside
`route_text_production`'s `page_count <= config.SHARD_SIZE_PAGES` comparison. Fixed by checking
`row["page_count"] is None` up front and raising a clear `BreakGlassError` instead. If you touch
this function, keep that guard — it's the difference between "this doc can't be re-driven, here's
why" and an unhandled crash three call frames deep.

## Ansible task ordering — file order wins, not tag order

`ansible-playbook site.yml --tags a,b,c` executes tasks in **file order**, filtered by tag — the
order you list tags in `--tags` has no effect on execution order. This bit twice during the
Ansible migration:

1. `test`/`summary` were originally positioned *before* the "start host consumers" section in the
   file. `--tags reset,e2e` ran pytest's session-scoped `TRUNCATE` fixture before fixtures were
   even generated. Fixed by moving `test`/`summary` to the physical end of the file (see the
   comment block above them in `site.yml`).
2. The `reset` block was positioned *after* `topics`/`fixtures`. `--tags reset,e2e` generated
   fixtures, then immediately wiped them via reset's truncate. Fixed by moving `reset` to directly
   follow `init-db`.

If you add a new tagged block, check its physical position against every tag combination that
might select it, not just the one you're testing.

## PID-capture gotcha: `uv run` does not `exec` into its target

`ansible.builtin.shell`'s pattern for backgrounding `run_local.py` was originally
`( {{ dotenv_cmd }} exec uv run python3 scripts/run_local.py ) > log 2>&1 & echo $! > pidfile`. This
looks right — `exec` should replace the subshell with the target process so `$!` captures the real
PID — but `uv run` itself **forks a child and waits** rather than exec'ing into it, so `exec uv run
...` only replaces the subshell with `uv`'s own short-lived wrapper. `$!` ends up holding a PID that
exits almost immediately, ~10 PIDs before the real `run_local.py` process spawns.

**Confirmed live, and it was silent for a while:** "Stop the host consumers" (`kill
$(cat pidfile)`) was killing an already-exited `uv` wrapper on every single `make e2e` run, so every
run's entire consumer stack (`run_local.py` + all 9 child processes) leaked as zombies. They kept
polling GCS and reprocessing objects into the ledger *after* the next run's reset truncated it,
producing ledger states that looked like a pipeline correctness bug (documents stuck in
`text_pending`/`extract_pending`) but were actually four overlapping generations of zombie
consumers fighting over the same tables. Fixed by exec'ing `{{ repo_root }}/.venv/bin/python3`
directly (bypassing `uv run`'s indirection) so `$!` is the real, killable PID — plus a follow-up
Ansible task that explicitly fails the play if the pidfile process doesn't actually exit within
30s, so this class of bug can't go silent again. If you ever background another long-running
process via Ansible, exec the real interpreter, not a wrapper CLI — and don't trust "the stop task
returned ok" without checking `ps` at least once.

## ArgoCD: both apps, no exceptions but two

Every K8s-manifest deploy in this repo — the 8 app Deployments/ConfigMap/ScaledObjects in `k8s/`,
*and* the KEDA operator itself — goes through ArgoCD (`argocd app sync <name>`), not raw
`kubectl apply`/`helm install`. There are exactly two `kubectl`-via-Ansible calls left in the whole
deploy path, and both are irreducible: applying `argocd/application.yaml` and
`argocd/keda-application.yaml` themselves. An `Application` object is what *tells* ArgoCD what to
sync — it can't be synced into existence by ArgoCD. ArgoCD itself is not bootstrapped by this repo
at all — see "Full cluster rebuild" below; it comes from the sibling repo's own terraform.

- `docpipeline`'s Application syncs `--local ./k8s` (renders from the working tree directly, no git
  push needed — same convention as the sibling repo).
- `keda`'s Application points at kedacore's real upstream Helm chart (`repoURL:
  https://kedacore.github.io/charts`, no `--local`) — it's a third-party operator, not "our"
  manifests, so pulling from the real chart source is correct GitOps, not a workaround.
- **Confirmed-live gotcha:** KEDA's `scaledjobs.keda.sh` CRD is large enough that client-side
  `kubectl apply`'s `last-applied-configuration` annotation blows past Kubernetes' 262144-byte
  annotation limit, failing the sync. Fixed with `syncOptions: [ServerSideApply=true]` on the
  `keda` Application — server-side apply tracks field ownership instead of stuffing the whole prior
  manifest into an annotation. If a future chart upgrade reintroduces a similarly oversized CRD,
  this is the fix, not `Replace=true` (which is needlessly destructive).
- `undeploy` tears down via `argocd app delete docpipeline --cascade`, not a hand-maintained list of
  `kubectl delete -f`. ArgoCD tracks every resource it synced (including `keda.yaml`'s
  `ScaledObject`s), so cascade delete removes exactly what `deploy` created — it's the GitOps mirror
  of the sync, not a separate manual teardown path that can drift from what `deploy` actually made.

## Step 8 (real LLM) — local, not a hosted API; real by default in K8s only

`config.py`'s `LITELLM_TIER_MODELS = {"cheap": "cheap-fast", "strong": "cheap-balanced"}` and
`llm_client.py` call the sibling `mlops-llm-repo`'s already-running, already-Langfuse-wired
`litellm` Deployment, which itself proxies to **Ollama** (`llama3.2:1b` / `qwen2.5:1.5b`, CPU-only,
$0 marginal cost) — there is no hosted-API call anywhere in this codebase.

Two different defaults, deliberately: `config.py`'s own fallback is `EXTRACTION_MODE=mock` (so
host-only `make e2e` stays a ~15s loop with no minikube needed), but `k8s/configmap.yaml` sets
`EXTRACTION_MODE=real` for the in-cluster Deployments — litellm is already reachable there over
cluster DNS, so there's no reason to fake it once you're actually inside minikube.
`tests/conftest.py`'s session-scoped `_force_mock_extraction_mode` fixture keeps the pytest suite
hermetic regardless of which value is ambient in the shell environment —
`test_real_llm_integration.py` still overrides it per-test via its own `monkeypatch`, which wins
over the session default. If you ever need to point at a different model, change
`LITELLM_TIER_MODELS`' aliases to match whatever `model_list` the sibling repo's `k8s/litellm.yaml`
currently defines — the two files describe the same aliases independently, no single source of
truth enforced in code.

**Confirmed-live gotcha: Ollama is one pod, not N.** Sending all 14 fixtures through the real LLM
path (the naive reading of "real is the K8s default") means all 14 compete for the *same* single
Ollama pod regardless of how many extraction replicas KEDA scales up — inference is effectively
serial there no matter how much you parallelize the consumer side. Confirmed live: 10 documents
queued at `extract_pending` simultaneously, a canary launched mid-batch failed even a 1200s (20
min) SLO. Adding more extraction replicas or raising the SLO further both treat the symptom.
The actual fix is `fixtures/generate_fixtures.py`'s `FIXTURE_LIMIT` env var — `site.yml`'s
`e2e-k8s`-tagged fixture task sets `FIXTURE_LIMIT=3` (tier-0, OCR-fallback, single-shard OCR),
while the plain `fixtures`/`e2e` tags still generate the full 14 for `make e2e`'s mock-mode host
loop, which already proves every fixture's correctness — `e2e-k8s` only needs to prove the
*deployment* (ArgoCD/KEDA/real-LLM-connectivity), not re-prove all 14 fixtures under real-model
latency. The canary's own `--slo-seconds` (900, in `site.yml`) is separate margin on top of that fix, not
the fix itself — if this class of failure resurfaces, check queue depth first
(`SELECT state, count(*) FROM documents GROUP BY state`) before assuming a bigger number will help;
a growing queue means contention (fix: `FIXTURE_LIMIT`), a single stuck `extract_pending` document
with a low queue count means something else broke.

**Confirmed live, separately: real Ollama inference time is host-load-dependent, not fixed.** Even
with contention resolved (`FIXTURE_LIMIT=3`), a single real call was observed anywhere from ~150s
to 7.5 minutes depending on what else the host was doing (load average ~7 after hours of continuous
minikube rebuilds in one session). 900s is margin for that variance, not a claim about typical
latency — don't read it as "real extraction normally takes 15 minutes."

## Confirmed-live gotcha: stale Kafka clients survive a Redpanda recreate

`reset` kills and recreates the Redpanda container (new cluster ID, by design — see "Recreate
Redpanda" in `site.yml`). Host processes started fresh by the next `run_local.py` invocation don't
care. **Already-running K8s pods do** — their rdkafka client stays connected to the old (now dead)
broker, and `argocd app sync` is a no-op on an unchanged Deployment spec, so nothing forces a
reconnect. librdkafka does eventually notice the cluster-ID mismatch and reconnect on its own, but
the observed recovery window was **~14 minutes** — long enough that a canary or real ingest
launched during that window sees `did not reach complete ... (never even triaged — ingest path
itself may be down)`, which reads exactly like a broken ingest path and isn't one; it's a stale
client. `ansible/site.yml`'s `deploy`/`e2e-k8s` tags now force a
`kubectl rollout restart deployment -l <docpipeline_apps>` right after the ArgoCD sync, specifically
so every pod reconnects immediately against whatever Redpanda cluster exists *right now* rather
than waiting out librdkafka's own backoff. If you ever see "never even triaged" against a live K8s
deployment, check `kubectl logs -l app=docpipeline-outbox-relay | grep ClusterId` before assuming
the pipeline itself is broken.

The rollout restart trades one race for a smaller one: `kubectl rollout status` confirms pods are
Ready, not that the Kafka consumer group's rebalance has settled. Observed once — a canary launched
immediately after the restart got its `ocr.completed` message stranded mid-rebalance, unprocessed
until the sweeper redrove it minutes later. `site.yml` adds a 20s pause between the rollout restart
and anything depending on the group actually consuming, as cheap insurance. This turned out to be
a minor contributor, not the dominant cause of the canary failures chased across this section and
the one above — see the Ollama-is-one-pod gotcha above for the actual dominant cause and its fix.

## Full cluster rebuild — `make e2e-k8s` is destructive on purpose

`make e2e-k8s` runs `minikube delete` + `minikube start --cpus=6 --memory=20480`, then
re-provisions the sibling `mlops-llm-repo`'s entire stack via *that repo's own*
`terraform apply` before doing anything docpipeline-specific — the same command its
`scripts/deploy.sh` uses, not a reimplementation of its bring-up. A long-lived reused cluster is
exactly what let the stale-Kafka-client bug above hide for as long as it did, and how a
hand-installed ArgoCD or stray KEDA release can silently diverge from what this repo declares.
`sibling_repo_root` assumes `mlops-llm-repo` is a literal sibling directory
(`{{ repo_root }}/../mlops-llm-repo`) — update it if you relocate either repo. Two gotchas found
getting this working:

- **asdf resolves terraform's version from the OS working directory, not `-chdir=`.** Terraform's
  own `-chdir=` flag only tells terraform where to find `*.tf` files; it never changes the actual
  process cwd asdf's shim inspects to find the nearest `.tool-versions`. Without Ansible's own
  `chdir:` on that task, the shim fell through past the sibling repo's own `.tool-versions` (pins
  the `1.16.0` its `base.tf` requires) to `~/.tool-versions` (pins an unrelated `1.7.4`) and failed
  outright. Fixed by adding `chdir: "{{ sibling_repo_root }}/tf"` to the Ansible task itself.
- **`kubectl port-forward` backgrounded from an Ansible shell task is fundamentally unreliable —
  don't fight it, avoid it.** It reliably died after handling exactly one connection when launched
  via `nohup`/`setsid`/retries from an Ansible-spawned shell, while the identical command worked
  perfectly run interactively. Rather than keep hardening the port-forward, the fix is to not need
  one: `argocd app sync --core` talks to the Kubernetes API directly (no argocd-server login or
  port-forward at all). It resolves `argocd-cm` from the *current kubectl context's namespace*, so
  `site.yml` switches the context to `argocd` before `--core` calls and back to `default`
  immediately after — nothing else in the play may assume `argocd` is the ambient namespace.

## Connection-leak discipline

`ledger.connect()` returns a plain `psycopg.Connection` — nothing closes it for you. Every
long-running consumer keeps one open for its process lifetime (fine); every **script** or **test**
that opens one must close it explicitly. This bit for real during development: `dlq_replay.py`'s
`run()` didn't close its connection (production code, not just a test), and three test files
(`test_business_dedupe.py`, `test_outbox_relay.py`, `test_scatter_gather_join.py`) had raw
`ledger.connect()` calls that were never closed either. The accumulated idle-in-transaction
connections held `AccessShareLock` on `documents`, which silently blocked a later test's `TRUNCATE`
and caused multi-minute hangs that took a `pg_stat_activity` inspection to actually diagnose. If you
open a connection outside the pytest `conn` fixture, wrap it in `try/finally: conn.close()` — no
exceptions.

## Dependency management

`uv` only — no manual venv, ever. Add deps to `pyproject.toml` and run `uv lock`; don't hand-edit
`uv.lock`. Every Makefile target that runs Python does so via `uv run` (or, for the one place it
matters — backgrounding a long-lived process — the `.venv/bin/python3` interpreter directly, see
the PID-capture gotcha above). `.python-version` pins `3.13`.

## Things intentionally left alone (not in scope here)

- No Argo Workflow wrapper around the operator lanes — Argo Workflows already runs in this same
  minikube cluster for the sibling repo, so a `WorkflowTemplate` that shells out to
  `python -m docpipeline.reconciliation.operator` would be additive, not a redesign. Not done here
  because the interesting part is the guardrails inside `operator.py`, not the YAML that invokes it.
- No CI enforcing the quality gates or the read-only/break-glass role split — both are currently
  proven only by the test suite and by running the thing live, not by a pipeline gate. A known,
  deliberate gap, not an oversight.
- Tesseract OCR and the real-LLM tier both stay opt-in behind env vars
  (`OCR_ENGINE=tesseract`, `EXTRACTION_MODE=real`) rather than becoming the default — matches the
  design doc's own "mock is the default for ~90% of tests" recommendation.
