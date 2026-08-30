# HISTORY.md

A chronological, session-by-session log of the debugging and build work behind this repo's
current shape — every bug found live, every hypothesis chased (including the wrong ones), and
why each fix looks the way it does. Entries are **newest first**.

For a lean, current-state reference (architecture, gotchas that still matter, and *open* bugs),
see [AGENT.md](AGENT.md) instead — this file is the "how we got here," not the "how it works now."

---

**Same-session, after the ArgoCD/Helm migration below: Langfuse Score integration, a custom
Prometheus metric, a lightweight Prometheus+Grafana stack, and `docpipeline/text/` split out of
`stages/` — confirmed working live, modulo the already-tracked wedged-extraction-consumer bug:**
- **Langfuse Score integration** (`docpipeline/stages/llm_client.py`). `extract()` now passes
  `metadata: {"trace_id": doc_id}` on every LiteLLM call, so every tier/repair attempt for one
  document lands on the same Langfuse trace instead of a new trace per call. `push_gate_scores()`
  then POSTs one Score per gate outcome (`pass`→1.0, `fail`→0.0, `inconclusive`→0.5,
  `not_applicable` skipped) to that trace via Langfuse's plain REST API (no `langfuse` SDK
  dependency added — a single `httpx.post` was enough), called from `extraction_4.py` at every
  point a document's `gate_results` become final (funnel-exhausted, kill-switch, duplicate-at-commit,
  and the actual auto-post winner — but *not* the discarded not-first-writer loser, since the
  winner already scores the authoritative result). Real mode only; mock mode never calls
  litellm/Langfuse, so `push_gate_scores` no-ops on `config.EXTRACTION_MODE != "real"`. New config:
  `LANGFUSE_HOST`/`LANGFUSE_PUBLIC_KEY`/`LANGFUSE_SECRET_KEY`, same demo keys
  `mlops-llm-repo/k8s/litellm.yaml` already uses server-side (same Langfuse project). **Confirmed
  live**: `kubectl logs` showed 6× `POST .../api/public/scores` all `200 OK` right after a real
  document landed in `review:gates_exhausted`. Later re-confirmed live via `curl` against
  Prometheus's own `/api/v1/query`: both scrape targets (`redpanda`, `docpipeline-triage`) reported
  `health: up`.
- **Triage now exports a custom Prometheus counter** (`docpipeline/stages/triage_1.py`):
  `triage_results_total{result=...}`, one label value per `handle_gcs_path()` classification
  (`dispatched`, `duplicate`, `zero_byte`, `unsupported_mime`, `encrypted`, `corrupt`,
  `page_ceiling`, `not_found`) — one counter, not one metric per failure mode. Served via
  `prometheus_client.start_http_server(9100)` at the top of `run_forever()`. `k8s/templates/
  deployment.yaml` gained an optional per-service `metricsPort` (only `triage` sets one, via
  `k8s/values.yaml`), which conditionally adds a container port *and* a dedicated
  `docpipeline-triage-metrics` Service (Deployments alone only give per-pod IPs, not something
  Prometheus can target by stable DNS).
- **New lightweight, standalone Prometheus + Grafana** (`k8s/templates/monitoring.yaml`) — plain
  Deployments, deliberately *not* the `kube-prometheus-stack` operator (Prometheus Operator + CRDs
  + Alertmanager + node-exporter + kube-state-metrics). The sibling `mlops-llm-repo`'s own
  `tf/langfuse.tf` explicitly documents dropping that exact stack for being "the single heaviest
  addition to the local stack" (offset against Langfuse's own ClickHouse weight) — this sidesteps
  that same resource-contention concern by scoping down to exactly two things: Redpanda's native
  `/metrics` on its already-exposed admin port (9644) and the new triage counter above. Both
  Grafana's datasource and Prometheus's scrape config are provisioned via ConfigMap, no manual UI
  setup. **No dashboard JSON shipped yet** — this is the first time this stack has run against a
  live cluster, so exact Redpanda metric names (topic/consumer-group lag specifically) aren't
  confirmed against real `/metrics` output; better to let Grafana's own Explore view surface what's
  actually there than ship panels that silently render "no data" against a guessed name. Everything
  here is part of the same `docpipeline` ArgoCD Application (wave `-1`, alongside the rest of
  infra) — no raw-kubectl exception. **Confirmed live**: both pods `Running`, `0` restarts,
  targets `up`.
- **`docpipeline/text/` split out of `docpipeline/stages/`**: `pdf_utils.py` (shared by
  `triage_1.py` and `pdf_worker_2.py`) and `ocr_engine.py` (shared by `ocr_shard_3.py` *and*
  `fixtures/generate_fixtures.py` — not even stage-only) were helpers living awkwardly next to the
  5 numbered sequential-step files, making them look like "just another stage." Straight `git mv` +
  import updates, no behavior change. **Found a real pre-existing bug in the process** (not caused
  by the move — the depth-relative-to-repo-root computation was already wrong at the old location
  too): `ocr_engine.py`'s `DEFAULT_REGISTRY_PATH` resolves to `docpipeline/fixtures/generated/...`
  instead of the intended top-level `fixtures/generated/...`, so `make reset`'s registry-clear step
  is a no-op on the file actually in use. Self-consistent (write and read always agree with each
  other), so it hasn't broken anything observable yet. Delegated to a background investigation
  rather than fixed inline.
- `scripts/` renamed to `local_scripts/` — makes explicit that these are host-local-execution
  utilities (even though two of them, `create_topics.py` and `summarize.py`, also get copied into
  the Docker image and reused in-cluster via Jobs/`kubectl exec` — that's incidental code reuse,
  not the reason the directory exists).
- **The wedged-extraction-consumer bug (below) recurred a second time, independently, confirming
  it's real and unrelated to any of the above** — see that entry for the live evidence. The
  `make e2e-k8s` canary failed on this, not on anything from this batch of changes: ArgoCD sync,
  GPU check, and the new Langfuse/Prometheus/Grafana/text-package changes all confirmed healthy in
  the same run. Also reproduced a **third** time later the same day (manually triggered a canary
  against a several-hours-old cluster) — same signature each time: exactly one document processed
  successfully, then silence, no crash/restart.

**Even later same-session work: `k8s/` is now Helm, `make e2e-k8s` is fully in-cluster and
fully ArgoCD-driven, and a new stuck-extraction-doc bug was found (not yet root-caused):**
- **`k8s/` converted from 3 flat manifests to a real Helm chart** (`Chart.yaml`, `values.yaml`,
  `templates/`). `templates/deployment.yaml` is one template `range`d over `values.yaml`'s
  `services:` list — adding a 9th consumer is a values entry now, not a copy-pasted Deployment
  block. Verified byte-identical to the old flat manifests via an object-level diff before the old
  files were deleted. `argocd/application.yaml`'s `path: k8s` and `--local ./k8s` sync are
  unchanged — ArgoCD detects Helm purely from `Chart.yaml`'s presence.
- **`make e2e-k8s` no longer touches docker-compose at all — Postgres/Redis/Redpanda/
  fake-gcs-server are now real pods in the cluster**, and **everything is synced through the same
  `docpipeline` ArgoCD Application**, no raw-kubectl exception (there was briefly a `k8s-infra/`
  directory applied via plain `kubectl apply` outside ArgoCD, mirroring the sibling repo's
  convention for its own foundational infra — the user explicitly asked for "purely ArgoCD driven"
  instead, so that got folded into `k8s/templates/infra.yaml` and removed). Ordering (infra up →
  schema/topics/fixtures exist → app tier starts → KEDA ScaledObjects reference existing
  Deployments) is enforced with **ArgoCD sync-waves**, not ansible: `templates/infra.yaml` +
  `templates/configmap.yaml` are wave `-1`, `templates/jobs.yaml` (migrate/topics/fixtures, one-off
  Jobs using the freshly-built image) are wave `0`, `templates/deployment.yaml` (the 8 app
  Deployments) is wave `1`, `templates/keda.yaml` is wave `2`. **Confirmed-live gotcha, now fixed:**
  KEDA's `ScaledObject`s originally had no wave annotation (defaulting to `0`, alongside the setup
  Jobs) and their `scaleTargetRef` failed admission validation because the target Deployments
  (wave `1`) didn't exist yet — that failure degraded the whole sync and silently blocked every
  later wave, including the app tier itself. Moving `ScaledObject`s to wave `2` fixed it. `argocd
  app sync docpipeline` now blocks through the full wave sequence, so it needs `--timeout 480`
  (added) rather than the CLI's indefinite default — a stuck wave would otherwise hang the whole
  ansible play forever with no failure signal.
  `host-mode make e2e is untouched` — it still uses `docker-compose.yml` directly (simplified to
  drop the now-unused `PODS` Kafka listener, since e2e-k8s no longer needs `host.minikube.internal`
  reachability at all) — the two paths share no infrastructure or ledger anymore. Migrations inside
  the Job run as the bootstrap admin (`PGUSER=postgres`, literal env on that one Job, not added to
  `docpipeline-config` — regular app pods have no business holding admin credentials) via `psql`
  (added `postgresql-client` + `COPY local_scripts local_scripts/` to the Dockerfile — the image
  had `libpq5`, the client *library*, but not the `psql` binary). `canary`/`dlq-replay`/
  `deadmans-switch`/`summarize` now run via `kubectl exec deploy/docpipeline-triage` instead of as
  host processes (Postgres/Kafka/GCS aren't host-reachable anymore); pytest was dropped from
  `make e2e-k8s` entirely (mock-mode correctness is already fully proven by `make e2e`'s host loop
  — e2e-k8s only needs to prove deployment mechanics + the real-LLM canary, and bundling
  pytest+tests/ into the image for a full test session's worth of DB access wasn't worth it for
  zero additional signal).
  **Two real bugs hit and fixed while validating this live:** (1) the GPU inference-warm-check
  (below) used `curl`, which isn't in the stock `ollama/ollama` image — switched to `ollama run
  llama3.2:1b hi`, which is always present and exercises the identical code path. (2) the
  KEDA-wave bug described above. **Confirmed live end to end after both fixes**: full clean
  `make e2e-k8s` run, `ok=21 failed=0`, GPU healthy on the first check (no self-heal needed),
  `argocd app get docpipeline --core` reports `Health Status: Healthy`, all 8 app pods + 4 infra
  pods Running with 0 restarts, `docker ps` confirmed the host's docker-compose containers were
  never touched, and the canary passed (`review`, via the real-mode review-as-pass logic).
- **New bug found while checking final state post-validation, NOT yet root-caused: a single
  `docpipeline-extraction` replica can silently stop consuming after its first message, with no
  crash, no restart, no error logged — just goes quiet while still `Running`/`Ready`.** Confirmed
  live: `kubectl exec deploy/redpanda -- rpk group describe extraction` showed partition 2 of
  `ocr.completed` at **51 messages of lag** while partitions 0 and 1 were fully caught up (lag 0);
  the pod assigned to partition 2 logged `extraction consumer started`, one successful
  `httpx ... POST http://litellm... 200 OK`, then nothing else ever again (no exception, no
  `extraction skipping stale redelivery`, no `extraction <doc> -> <result>` — just silence). Because
  Kafka partition assignment is keyed and sticky, every sweeper redrive of the one document whose
  key hashes to partition 2 lands on the same wedged consumer every time, which is exactly the
  "sweeper republishes, nothing ever picks it up" symptom from the very first stuck-doc bug earlier
  in this file — except this time in a completely fresh cluster with zero possibility of the
  zombie-consumer explanation that closed out that one. **Not yet root-caused** — no crash-loop
  signal for Kubernetes to act on (no liveness probe on these Deployments at all currently), so this
  can go unnoticed indefinitely in production shape today. Next steps: reproduce again and, before
  killing the wedged pod, get a thread/stack dump (`py-spy dump` if available in the image, or at
  minimum full `kubectl logs` plus `kubectl exec ... -- python -c "import faulthandler,
  signal; faulthandler.register(signal.SIGUSR1)"`-style instrumentation) to see what it's actually
  blocked on — a hung `httpx` call to litellm with no timeout is the most likely suspect given the
  one successful call right before it went quiet, but that's a guess, not yet confirmed.
  **Recurred a second time, independently, later the same day** — a `make e2e-k8s` run validating
  the Helm/Langfuse/Prometheus/file-rename work below failed the canary (900s timeout, no `review`
  fallback either) with the identical signature: `kubectl logs` on both `docpipeline-extraction`
  replicas showed each processing exactly one document successfully (one of them even completed a
  full Langfuse score push — 6× `POST .../api/public/scores` all `200 OK` — proving that new
  integration works correctly) and then going silent forever, never touching the remaining queued
  documents. Confirms this is a real, reproducible bug independent of any of this session's other
  changes, not a one-off. Delegated to a background investigation
  (`mcp__ccd_session__spawn_task`, title "Root-cause wedged docpipeline-extraction consumer") —
  check its status before re-investigating from scratch.

**Latest session's findings (picking up from the "pick this up first tomorrow" note below):**
- **The stuck-doc bug did NOT reproduce.** Ran a clean `make e2e` **four times in a row** (docker
  infra already up from the prior session, no host consumer processes running beforehand —
  confirmed via `ps aux`): every run drained all 14 fixtures to a terminal state inside the 180s
  SLO (`wait_for_drain.py` exit 0 each time, `sweeper.log` never logged a single
  `reconciler_stuck_docs_found` line in any of the four runs), `pdf_worker.log` showed normal
  per-document activity every run (not the "only its own startup line" silence from the session
  that found the bug), the post-run zombie check (`ps aux` for all 8 service modules) came back
  empty every time, and the full pytest suite passed (58/58) every run. **Working theory, not
  fully proven:** the stuck docs and the silent `pdf_worker.log` were both symptoms of *stale
  zombie consumer processes left over from that session's own marathon of repeated manual
  runs/interrupts* (before, or coexisting with, the `setpgrp()` fix below actually taking effect
  for every generation) — a zombie `pdf-worker` still holding a Kafka consumer-group partition
  assignment, but wedged, would exactly produce "sweeper redrives, nothing ever picks the work
  back up" plus a healthy-looking startup log line with silence after (it received a message,
  presumably failed silently on something session-specific like a dead DB connection, and never
  logged or committed). **Not re-chased further this session** since it wouldn't reproduce in a
  verified-clean environment — if it recurs, check `ps aux` for extra/orphaned service processes
  *before* assuming it's a pipeline correctness bug, and don't trust "the stop task returned ok"
  alone (same lesson as the PID-capture gotcha below).
- **Made `review` a valid pass condition for the canary under `EXTRACTION_MODE=real`** — the item
  explicitly left "not yet done" below. `canary.py`'s `run_canary()` now treats a doc landing in
  `review` as `ok: True` (with a `reason` noting it) only when `config.EXTRACTION_MODE == "real"`;
  under mock mode `review` still fails the canary, since mock extraction is deterministic and
  landing in `review` there means something is actually broken, not model variance. Two new tests
  in `test_canary.py` lock in both branches. Full suite still 58/58 (canary tests add 2, so 56→58).
  **Now exercised against a real `make e2e-k8s` run — see the two entries directly below.**
- **`make e2e-k8s`'s canary hit the 900s SLO and failed — root cause was the GPU-registration race
  recurring, not the pipeline.** `ollama` pod's `nvidia-smi` returned `Failed to initialize NVML`,
  `ggml_cuda_init` fell back to CPU (`slot print_timing: tg ≈ 0.48 t/s`, matching the documented
  1.2-2.1 t/s CPU-fallback ballpark). Applied the already-documented fix (`kubectl delete pod -l
  app=ollama`) — the replacement pod's `nvidia-smi` worked immediately. **This is not a one-off: it
  recurred on a second, independent cluster rebuild days after it was first found, so treat it as a
  standing risk on every `make e2e-k8s`, not a closed issue** — watch `kubectl logs deploy/ollama`
  for `slot print_timing` rates under ~5 t/s, or any canary/extraction call taking >30s, as the
  tell, and re-run the pod delete if seen.
- **Once GPU was healthy, re-ran the canary manually — it revealed the delimiter fix was never the
  actual cause of the `arithmetic` gate failure.** Finished in 110s (vs. heading toward the 900s
  timeout), but still landed in `review` on the same gate (`computed=100, declared=-100`) *with the
  `" | "` delimiter already in place* — disproving the "bare hyphen misread as a sign" hypothesis
  from the prior session. The real cause is more likely `llm_client.py`'s prompt instruction
  (`total_cents (integer, negative for credit memos)`) being over-applied to a non-credit-memo doc
  — **not fixed this session**. The delimiter migration is still worth keeping (real ambiguity fix,
  just not this bug's fix). The canary reported `ok: True` this run via the new review-as-pass
  logic — validates that change was the right call, not just a belt-and-suspenders nicety.

**Confirmed solid, no open questions:**
- `make e2e` (host, mock mode) — 59/59 tests pass (was 56; grew by 3 with the canary
  review-as-pass-in-real-mode work later in this file), all 14 fixtures reach correct terminal
  states, ~15s, stable across many repeated runs.
- The K8s *deployment mechanics*: ArgoCD sync via `--core` (no port-forward), KEDA fully
  ArgoCD-managed, all 8 Deployments + 2 `ScaledObject`s deploy and become available, Kafka clients
  correctly reconnect after a Redpanda recreate. Each of these was a real, reproduced, root-caused
  bug found live — see the sections below for each one's fix.
- Repo migrated from the old `llmops-document-pipeline` working directory into this one
  (`document-llm-ops`), pushed to `github.com/wmaucla/document-llm-ops`. This is now the canonical
  location; the old directory is untouched but no longer the source of truth.
- **GPU passthrough is now confirmed working end-to-end in Kubernetes, not just raw Docker.**
  `document-llm-ops/ansible/site.yml`'s minikube start passes `--gpus=all`, and the sibling
  `mlops-llm-repo`'s `k8s/ollama.yaml` requests `nvidia.com/gpu: 1` (stock `ollama/ollama` image, no
  special build). Verified live on the cluster left running from the interrupted `make e2e-k8s`
  attempt: `kubectl describe nodes minikube` shows `nvidia.com/gpu: 1` in both Capacity and
  Allocatable (the device plugin — `nvidia-device-plugin-daemonset` in `kube-system` — registered
  correctly); the ollama pod initially hit `FailedScheduling: Insufficient nvidia.com/gpu` for the
  ~8s before the device plugin finished registering, then scheduled fine on retry (transient, not a
  bug); `kubectl exec -n default deploy/ollama -- nvidia-smi` shows the RTX 2080 Ti; ollama's own
  logs show `msg="inference compute" library=CUDA compute=7.5 name="NVIDIA GeForce RTX 2080 Ti"`;
  `ollama ps` shows `100% GPU` (full offload, no CPU fallback layers). A real `/api/generate` call
  via `kubectl port-forward` (warm, model already resident) returned in **0.079s total /
  0.055s eval** for a 3-token response — confirms this is genuinely the fix for the 150s–7.5min
  CPU-latency problem below, not just plumbing that happens to schedule.
  **Note: the sibling repo's `k8s/ollama.yaml` GPU edit is still uncommitted there** (`git status`
  shows `M k8s/ollama.yaml` in `mlops-llm-repo`) — it's picked up fine since the ansible flow
  `kubectl apply`s the file directly, but commit it there if you want it to survive independently
  of this repo's working tree.

**GPU: confirmed working, with one real transient failure mode found and one fixed:**
- A full `make e2e-k8s` run completed through image build, ArgoCD sync, KEDA, and rollout-restart,
  but its *first* Ollama pod instance never became healthy: `kubectl exec ... nvidia-smi` returned
  `Failed to initialize NVML: Unknown Error`, no CUDA/GPU lines anywhere in its logs, and live
  `slot print_timing` showed **1.2-2.1 tokens/sec** — pure CPU speed despite the GPU resource
  request being honored by the scheduler. This is a real, observed race: the GPU device plugin can
  register with the node before it's actually able to hand out a working device to the *first*
  container that claims it. **Fix confirmed live:** `kubectl delete pod` on the broken instance
  forced a fresh scheduling attempt, and the replacement pod's `nvidia-smi` worked immediately
  (`RTX 2080 Ti`, 2028MiB used, no errors). If a GPU-requesting pod is stuck `0/1 Ready` for more
  than a couple minutes past its model-pull window, don't assume it'll self-heal — delete it and
  let it reschedule.
- **Confirmed-live gotcha, already fixed in `mlops-llm-repo/k8s/ollama.yaml`:** the readiness
  probe's default 1s `timeoutSeconds` was too tight — two sequential `ollama list` invocations take
  ~2-3s combined, so the probe flapped on an otherwise-healthy server, dropping the Service's only
  endpoint (`kubectl get endpoints ollama` showed none) and making every downstream call fail with
  "connection refused," which is exactly what made 6 documents sit at `extract_pending` with **zero**
  extraction attempts logged even after the full 900s canary SLO elapsed — not a Kafka/consumer-group
  bug, a starved-endpoint bug. Fixed with `timeoutSeconds: 5` on that probe.
- **Confirmed-live gotcha, separately fixed in the same file: single-GPU nodes deadlock the default
  rolling update.** Any spec change to the `ollama` Deployment (the `timeoutSeconds` fix above
  included) triggers Kubernetes' default `RollingUpdate` strategy, which surges a new pod *before*
  killing the old one — but the node has exactly one `nvidia.com/gpu`, so the new pod sits in
  `FailedScheduling: Insufficient nvidia.com/gpu` forever while the old pod, still holding the only
  GPU, never gets torn down because the rollout is waiting for the new pod to become Ready first. A
  genuine deadlock, not a slow rollout — observed live, new pod stuck >5 minutes with zero progress.
  Fixed by adding `strategy: {type: Recreate}` to the Deployment spec, which kills the old pod first
  and frees the GPU before scheduling the new one. Any single-GPU-node Deployment needs this; it's
  not specific to Ollama.
- **Real, measured GPU speedup once past the above:** a warm `/api/generate` call returned in
  **6.03s total / 0.065s eval** for the tokenizer's `eval_duration` specifically (vs. the
  documented 150s-7.5min CPU baseline) — genuinely GPU-accelerated, not just scheduled-with-a-GPU.
  `ollama` Deployment in the sibling repo requests `nvidia.com/gpu: 1`; `document-llm-ops/ansible/
  site.yml`'s minikube start passes `--gpus=all`. **The sibling repo's `k8s/ollama.yaml` edits are
  still uncommitted there** (`git status` shows `M k8s/ollama.yaml`) — pick up fine via `kubectl
  apply` regardless, but commit them there if you want them to survive independently of this
  working tree.

**Bug found in an earlier session — did NOT reproduce on retest, see "Latest session's findings" above:**
- **Two documents per `make e2e` run reproducibly get stuck forever in `text_pending`/`extract_pending`,
  resisting the sweeper's redrive.** Confirmed live after fixing the process-group bug below (so this
  isn't a symptom of that): `sweeper.log` finds the same 1 `text_pending` + 1 `extract_pending` doc on
  every 30s cycle from `20:47:03` through at least `20:48:03` without ever clearing — the sweeper
  re-drives them (transitions back to `_pending`, presumably re-publishes work) but nothing ever picks
  the work back up. This run's two stuck docs: `crc32c-b9e3f5a2` (`credit_memo.pdf`, `extract_pending`,
  never appears anywhere in `logs/extraction.log`) and `crc32c-7c373c50`
  (`digital_garbage_text_layer.pdf`, `text_pending`, `has_text_layer=False`, 5 pages). **Suspicious
  detail: `logs/pdf_worker.log` shows only its own startup line and zero processing activity all run**,
  yet `logs/ocr_shard.log` shows shards for two *other* docs being processed successfully — meaning
  pdf_worker did *something* (shards don't appear without it) but isn't logging it, or it's dropping
  specific messages silently. `logs/outbox.log`'s last "relayed N messages" line is at `20:46:34`; no
  relay activity after that suggests either everything caught up cleanly, or these two docs' outbox
  rows never got inserted in the first place — check `SELECT * FROM outbox WHERE doc_id IN
  ('crc32c-b9e3f5a2','crc32c-7c373c50')` first thing. Not yet checked: whether this is
  poison-message behavior specific to these two fixtures (`credit_memo` and the OCR-fallback path),
  a Kafka partition/consumer-group assignment gap, or something about the outbox relay itself.
  `make e2e`'s own drain check (`wait_for_drain.py 180`) now correctly detects and fails loudly on this
  (that path itself is fine) — the actual pipeline bug is upstream of it.

**Two infra bugs found and fixed this session, confirmed live:**
- **`local_scripts/run_local.py`'s process-group kill never worked.** `ansible/site.yml`'s "Stop the host
  consumers" task runs `kill -9 -- "-$(cat pidfile)"` (kills the whole process *group*) specifically to
  bypass `run_local.py`'s own SIGTERM handler and guarantee children die too. But `run_local.py` is
  backgrounded via plain `&` inside a **non-interactive** bash (`ansible.builtin.shell`, no job
  control), so it never becomes its own process-group leader — it inherits the group of the
  already-exited ansible-launched shell. Confirmed live: `ps -o pid,pgid` showed `run_local.py`'s own
  PID (348456) with **pgid 343828** — a group `kill -9 -- "-348456"` can never touch, since no process
  in that group has pgid 348456. Every consumer survived silently (`|| true` swallowed the kill's
  "no such process" error), leaking a full generation of 7 host processes that keep polling/reprocessing
  after the *next* run's reset truncates the ledger — the exact zombie-consumer failure mode already
  described above, just from a different root cause than the `uv run`-doesn't-exec bug that was fixed
  earlier. **Fixed:** `run_local.py`'s `main()` now calls `os.setpgrp()` before spawning any children,
  making itself the leader of a new group (pgid == pid), so the existing kill command actually works.
  Confirmed live: re-ran `make e2e`, the "did host consumers actually stop" check now *skips*
  (condition not met — they really did stop) instead of failing. Doesn't affect the documented
  interactive `Ctrl-C` use case: bash's job control already makes a foreground command its own
  process-group leader at fork time, so `setpgrp()` there is a no-op.
- **`mlops-llm-repo/k8s/ollama.yaml` had two real bugs, both fixed:** (1) the readiness probe's
  default 1s `timeoutSeconds` was too tight for two sequential `ollama list` calls (~2-3s combined),
  causing the probe to flap on an otherwise-healthy server and drop the Service's only endpoint —
  fixed with `timeoutSeconds: 5`. (2) the Deployment's default `RollingUpdate` strategy deadlocks
  forever on this single-GPU node (new pod can't schedule — `Insufficient nvidia.com/gpu` — while the
  old pod holds the only GPU and never gets torn down waiting for the new one to be Ready) — fixed with
  `strategy: {type: Recreate}`. Both confirmed live; **still uncommitted in `mlops-llm-repo`**
  (`git status` shows `M k8s/ollama.yaml` there) — commit if you want them to survive independently of
  this working tree.
- **Canary's synthetic doc reproducibly failed the `arithmetic` gate under the real model** — fixed at
  the root, not worked around. `docpipeline/reconciliation/canary.py`'s line item was
  `"Line Item: Canary probe - 1.00"`; the real-LLM prompt's own `total_cents (integer, negative for
  credit memos)` instruction was making the small model misread the bare hyphen as a sign and report a
  negative total, which the gate correctly caught and routed to `review`. **Fix:** migrated the
  description/amount delimiter from a bare `" - "` to an unambiguous `" | "` everywhere it's used —
  `docpipeline/stages/mock_llm.py`'s parsing regex, `docpipeline/fixture_content.py` (all 7 line
  items, including the credit-memo one with a genuinely negative amount:
  `"Returned Steel Brackets | -4297.00"` still parses correctly since the delimiter itself carries no
  sign anymore), `fixtures/generate_fixtures.py` (3 occurrences), `canary.py`, and the three test files
  that hardcode the same fixture text (`test_real_llm_integration.py`, `test_business_dedupe.py`,
  `test_extraction_funnel.py`). `llm_client.py`'s prompt needed no change — it just forwards the raw
  document text verbatim. **Now re-verified against the real model — the delimiter was never the
  cause.** `make e2e-k8s`'s own canary timed out (900s) on an unrelated GPU-registration race (see
  "Latest session's findings" above and the GPU section below); after fixing that, a manual rerun
  directly against the live cluster (`kubectl exec deploy/docpipeline-triage -- python -m
  docpipeline.reconciliation.canary --slo-seconds 120`) finished in 110s but still landed in
  `review` on the exact same `arithmetic` gate (`computed=100, declared=-100`) — **with the `" | "`
  delimiter already in place**. So the "bare hyphen misread as a sign" hypothesis is disproven; the
  surviving hypothesis is `llm_client.py`'s prompt instruction (`total_cents (integer, negative for
  credit memos)`) being over-applied by the small model to a non-credit-memo document. **Not fixed
  this session** — the delimiter migration is still correct to keep (real ambiguity fix, just not
  this bug's fix), and the new review-as-pass-in-real-mode logic is exactly what absorbs this: the
  canary reported `ok: True` on this run, correctly reading "gate caught a bad value" as the
  pipeline working, not the pipeline being broken. The user separately asked to also make `review`
  a valid pass condition for the canary under real mode (belt-and-suspenders once the delimiter fix
  is in) — **done this session**, see "Latest session's findings" above.

**Real bug found once GPU speed unblocked everything downstream — not an infra issue:**
- **The canary's own synthetic document reproducibly lands in `review`, not `complete`, twice in a
  row with an identical result** (`arithmetic` gate: `computed=100, declared=-100`) — the real model
  extracts the total as **negative** for a document that never states a negative anywhere. Likely
  cause: `canary.py`'s line item is phrased `"Line Item: Canary probe - 1.00"`, and
  `llm_client.py`'s own prompt says `total_cents (integer, negative for credit memos)` — the small
  model may be reading the bare hyphen-as-separator as a sign, or over-applying the
  negative-for-credit-memos instruction to a document that isn't one. **This is the gates working
  correctly** (`arithmetic` and `plausibility` both correctly rejected the bad value rather than
  auto-posting it), not a pipeline defect — but it means the canary's strict "must reach `complete`"
  check may not be the right success criterion for real-mode, since a real small model's occasional
  gate-caught mistake is expected behavior, not pipeline failure.
  **Do not "fix" this by just changing the delimiter in `canary.py`** — I tried
  (`"Canary probe: 1.00"`) and reverted it: `mock_llm.py`'s line-item regex
  (`r"Line Item:\s*(.+?)\s*-\s*(\$?-?[\d,]+\.\d{2})"`) *requires* the literal `" - "` separator, so
  changing it breaks mock-mode canary parsing (arithmetic gate would see zero line items and go
  `inconclusive` instead of `pass`). Same `" - "` format is used by every other fixture's line items
  in `fixture_content.py`/`generate_fixtures.py` too, so they likely have the same latent risk under
  real mode. **Real fix, not yet done:** either (a) make the mock regex accept a second, unambiguous
  delimiter and migrate the canary (and ideally all fixtures) to it, or (b) decide the canary should
  accept `review` as a pass condition when `EXTRACTION_MODE=real` (distinguishing "processed and
  correctly gated" from "never processed at all," which is the failure mode that actually matters).
- **Next step: run a full clean `make e2e-k8s` end to end** with all of the above already fixed
  (readiness probe timeout, PID-capture, poison-message handling, max-poll-interval, `--core` mode,
  `FIXTURE_LIMIT=3`) and confirm the *pipeline* (not necessarily the canary's `complete` assertion)
  behaves correctly start to finish — expect the canary to still report `review` unless the
  delimiter issue above is fixed first, and don't mistake that for an infra regression if it happens.
- Consider adding a lightweight direct-Ollama smoke test (call `docpipeline.stages.llm_client.extract()`
  standalone, no Kafka/K8s involved) as a *second*, faster health check alongside the canary —
  proves Ollama connectivity in isolation without depending on consumer-group mechanics. Discussed,
  not implemented.

**Environment note:** docker daemon was found stopped at the start of this session (needed
`sudo systemctl start docker` — the agent cannot do this itself, no interactive sudo). Check it's
running before assuming any infra command failure is a code/config bug.
