# AGENT.md

Notes for an AI agent (or new contributor) picking up this repo cold. For the "why" behind the
overall design, see [README.md](README.md) — this file is the implementation-level gotchas,
mechanics, and open bugs that aren't obvious from reading the code once. For the session-by-session
story of how each of these was found and fixed (or not), see [HISTORY.md](HISTORY.md) — this file
stays current-state only.

## Known open bugs / standing risks

1. **"Wedged" extraction consumer — it was probably never wedged; the liveness probe was killing
   pods that were merely slow. Fixed, and the fix is now supported by a deliberate live
   reproduction (2026-08-31) — but see "What is still not established" below.** Symptom as
   originally observed: a `docpipeline-extraction` pod goes quiet after `"extraction consumer
   started"`, its
   partitions' lag grows, and it takes a liveness-probe restart (`Exit Code: 137`,
   `Reason: Killing — Container extraction failed liveness probe`) roughly every 5-6 minutes.
   Reproduced on fresh clusters; documents eventually land in `failed` with `extract_attempts=5`
   and an empty `last_error`. Five sessions of investigation chased this as a Kafka
   consumer-group problem (simultaneous-join race, then coordinator reassignment failure) and
   shipped four mitigations, none of which closed it — full narrative in
   [HISTORY.md](HISTORY.md).

   **Why the Kafka reading is probably backwards** — *this paragraph describes the code as it was
   before the fix; `run_funnel` now also touches the heartbeat before each call, which is precisely
   what closed it.* `heartbeat.touch()` (`docpipeline/infra/heartbeat.py`) was called in exactly two
   places in `extraction_4.py`: on message pickup in `run_forever()`, and *after `_call_model`
   returned successfully* in `run_funnel()`. Every failure branch of the repair loop — including the
   transient/timeout `continue` — looped back without touching it. The funnel permits **6 model
   calls per message**
   (2 tiers × `MAX_REPAIR_ATTEMPTS + 1`) at `LITELLM_TIMEOUT_SECONDS=200` each, i.e. up to ~1200s
   of *legitimate* silence, against a probe that kills at **300s** staleness with
   `failureThreshold: 1`. Two slow calls trips it; so does one call at the ~450s CPU-inference
   latency recorded in "real Ollama inference time is host-load-dependent" below. Under that
   reading every recorded artifact is explained with no Kafka bug at all:
   - logs containing **only** `"extraction consumer started"` are an in-flight `httpx.post` that
     hasn't returned yet — not, as previously read, a hang *before* `run_funnel` is reached;
   - the ~5-6 minute restart cadence **is** the 300s threshold;
   - `MEMBERS 7` (expected 3) and partition assignments pointing at dead pod IPs are ghosts left
     by SIGKILL sending no graceful `LeaveGroup` — a *consequence* of the restarts;
   - `extract_attempts=5` is consistent with repeated kills, since `increment_attempts` commits
     *before* the funnel runs. (The empty `last_error` is **not** independent evidence, contrary to
     an earlier version of this entry: `sweeper._claim_batch` never writes `last_error` on its
     give-up path, so *every* attempt-capped document has an empty one regardless of cause — see
     bug #3.)
   - the GCS-timeout and join-jitter fixes both failed because neither addresses call *latency*.

   `extraction` is also the only service with a `livenessProbe` at all, which explains the
   apparent "only KEDA-scaled consumers are affected" correlation better than a join race does —
   `ocr-shard` scales 1→5 and has never shown this. (An earlier version of this entry flagged a
   self-contradiction here — `config.py` hardcoding `KAFKA_MAX_POLL_INTERVAL_MS = 900_000` while
   claiming to cover a 1200s budget. That was real, and bug #3 fixed it: the value is now derived
   to 1500s. The probe's 300s is the deliberate exception, justified below, not the leftover half
   of a contradiction.)

   **Probably coupled to bug #2 below,** which this file previously treated as unrelated: with
   working GPU passthrough a warm call is 0.079s and the probe never trips; when the GPU
   registration race fires and inference silently falls back to CPU, calls run 150s-450s and it
   trips reliably. That is a clean explanation for intermittent reproduction across supposedly
   identical fresh clusters.

   **Mechanism closed 2026-08-30; hypothesis supported by direct evidence 2026-08-31.** All three
   fixes are in: (a) `run_funnel` touches the heartbeat *before* each model call as well as after,
   so staleness tracks one bounded `LITELLM_TIMEOUT_SECONDS` call rather than the whole message
   budget — every failure branch (timeout, refusal, unparseable) previously looped back without
   touching, which is what let a couple of slow calls kill a healthy pod; (b) the timeouts derive
   from `EXTRACTION_BUDGET_SECONDS` (bug #3), so the probe can honestly stay at 300s;
   (c) `failureThreshold: 3` instead of 1, so a single throttled exec on a 500m-CPU container
   isn't fatal. Bug #3's fix removes a second, independent source of the same symptoms.

   **The test AGENT.md kept asking for has now been run.** `maxReplicaCount` is back to 3, and the
   symptom was deliberately reproduced: watchdog scaled to 0, `CUDA_VISIBLE_DEVICES=""` on ollama
   (GPU to 322 MiB, host load 12.3, calls at ~114s), and a 10-document backlog. 29 minutes, three
   replicas processing concurrently, tier escalation and a cold model load — **zero restarts, zero
   liveness warnings**, well past the ~390s window (300s staleness + `failureThreshold: 3` ×
   `periodSeconds: 30`) where an untouched heartbeat would have killed a pod. The pods were
   demonstrably busy, not idle. Full numbers in [HISTORY.md](HISTORY.md).

   **What is still not established** is whether the probe was ever the *original* cause. The run
   above proves the *current* code survives the conditions that would have triggered it; nobody
   re-ran the pre-fix code under forced CPU for a direct comparison, which would mean deliberately
   reverting the heartbeat fix. Treat this as "mechanism closed, hypothesis supported," not
   "proven." `KAFKA_CONSUMER_DEBUG=1` (in `config.py`, off by default) remains the right tool if
   protocol-level evidence is ever needed.

   **How to re-run the reproduction** (the order matters — two things in the repo will otherwise
   heal it out from under you): the deploy-time residency gate *fails the play* on a CPU-resident
   ollama, and `gpu-watchdog` deletes the pod on `size_vram == 0`. So: a healthy `e2e-k8s` first,
   then `kubectl scale deploy/docpipeline-gpu-watchdog --replicas=0`,
   `kubectl set env deploy/ollama CUDA_VISIBLE_DEVICES=""`, then `make replay-docs COUNT=10`.
   Restore with `kubectl set env deploy/ollama CUDA_VISIBLE_DEVICES-` and scaling the watchdog
   back to 1. **`restartCount` is the ground truth** — do not use
   `kubectl get events --field-selector reason=Killing | grep extraction`, which matches routine
   `Normal`/"Stopping container" rollout terminations and has already produced one false alarm.

   **Mitigations currently in the tree** (all shipped while chasing the Kafka reading above; each
   is independently worth keeping, none closed the gap — see [HISTORY.md](HISTORY.md) for what
   each was tested against and why it was ruled out):
   - `docpipeline/infra/heartbeat.py` + the `extraction` `livenessProbe` in `k8s/values.yaml` /
     `k8s/templates/deployment.yaml`. **This is now the prime suspect, not a mitigation** — see
     above.
   - `ledger.connect()` sets a session-level `statement_timeout` (`PG_STATEMENT_TIMEOUT_MS`,
     default 30s) on every consumer's connection, so a DB-lock hang raises and redelivers instead
     of hanging silently. Ruled out as this bug's cause (no blocked queries ever observed in
     `pg_stat_activity`), but correct on its own merits.
   - Every call in `docpipeline/infra/gcs.py` passes `timeout=config.GCS_TIMEOUT_SECONDS` (30s)
     rather than relying on `google-cloud-storage`'s undocumented per-call default. Tested and
     ruled out as this bug's cause; also correct on its own merits.
   - `kafka_utils.make_consumer()` sleeps `random.uniform(0, KAFKA_JOIN_JITTER_SECONDS)` before
     creating the `Consumer` (default `0`/off; `k8s/values.yaml` sets `8` in-cluster). Confirmed
     structurally working (pod starts do stagger) and confirmed *not* sufficient on a fresh
     cluster.
   - `extraction`'s KEDA `maxReplicaCount` was capped at 1 (from 3) as a workaround.
     **Reverted to 3 on 2026-08-31** once the reproduction above ran clean; KEDA has been observed
     scaling extraction to 3 since, so the presentation's autoscaling claim is true again.

   **Diagnostic lesson from a false alarm on this bug** (full story in [HISTORY.md](HISTORY.md)):
   a document needing multiple schema-repair rounds issues one full LLM call per round, and every
   extraction replica shares the *same single* Ollama pod (see "Ollama is one pod, not N" below),
   so one document can legitimately take 10+ minutes with long silent gaps between its
   `httpx ... 200 OK` lines. **Don't conclude "wedged" without either (a) an actual blocked query
   in `pg_stat_activity`/`pg_locks`, or (b) waiting past the canary's own 900s SLO.** Note this
   lesson is also the strongest independent support for the liveness-probe hypothesis above: the
   latency it describes is exactly what a 300s probe cannot survive.
2. **Ollama silently falls back to CPU — root-caused as *two* problems, both fixed from this
   repo's side. Live-validated 2026-08-30 on a clean `make e2e-k8s`: `failed=0`, GPU gate passed,
   `✅ RUN COMPLETE 4/4`, extraction `RESTARTS 0`.** The original framing
   ("GPU-registration race on cluster rebuild") was only half of it, and guarded only the half
   that happens at bring-up.

   **(a) The GPU gets *released*, repeatedly — this is why it kept coming back.** Ollama's
   `OLLAMA_KEEP_ALIVE` defaults to **5 minutes**; an idle model unloads and its `llama-server`
   subprocess exits. The next request re-spawns it and **re-runs `ggml_cuda_init`** — so every
   idle gap is a fresh chance to land on CPU, and this pipeline is mostly idle gaps
   (`FIXTURE_LIMIT=4` plus a canary). Confirmed live: `kubectl describe pod -l app=ollama` showed
   `Environment: <none>`, i.e. the default was never overridden. This is what explains a pod that
   passed the deploy-time warm check being on CPU twenty minutes later *without ever restarting* —
   previously read as "the bring-up race recurring," which it isn't. Tier escalation compounds it:
   the funnel uses two models (`cheap-fast`→`llama3.2:1b`, `cheap-balanced`→`qwen2.5:1.5b`) and
   only the cheap one was ever warmed, so a strong-tier escalation cold-loaded its model for the
   first time *mid-pipeline*, unwarmed and unverified.

   **(b) The container was memory-starved.** Ollama's own loader logged `disabling mmap for
   llama-server load due to host memory pressure` with `system_free="768.2 MiB"
   system_total="4.0 GiB"` — the 4Gi cgroup limit, not VRAM. The `CUDA_Host` pinned buffers count
   against the cgroup even though the tensors live in VRAM. Not a VRAM shortage at all: the
   2080 Ti has 10.6 GiB and both models need ~2.8 GiB of it.

   **What makes this hard to see:** the failure mode is *degradation, not failure*. Ollama stays
   `Running`/`Ready` and keeps answering, just at 150-450s/call instead of 0.079s. Its own
   liveness probe is an HTTP GET that passes fine on CPU. Downstream this looks exactly like
   extraction hanging (**this is the suspected trigger for bug #1** — CPU-fallback latency against
   a 300s liveness probe), not like a GPU problem.

   **Fixes, all in this repo — `mlops-llm-repo`'s `ollama.yaml` is deliberately not edited:**
   - `ansible/site.yml` patches the live Deployment after the sibling's terraform applies it:
     `kubectl set env OLLAMA_KEEP_ALIVE=-1 OLLAMA_MAX_LOADED_MODELS=2` and
     `kubectl set resources --limits=memory=8Gi`. That alone collapses N `ggml_cuda_init` rolls
     into one per pod lifetime. It then warms **both** tiers' models, not just the cheap one.
   - `docpipeline/reconciliation/gpu_watchdog.py` + its `gpu-watchdog` Deployment
     (`k8s/values.yaml`, RBAC in `k8s/templates/rbac.yaml`) enforce both invariants *continuously*
     — polling `/api/ps` every 30s, re-pinning a model that unloaded, and deleting the ollama pod
     when a model comes back CPU-resident (`size_vram == 0`), rate-limited by a 300s cooldown so a
     genuinely GPU-less host degrades to periodic restarts rather than a thrash loop. This is the
     "otherwise let the pod churn" half, automated.
   - `site.yml`'s deploy-time gate now execs `gpu_watchdog --check-once` in that same pod, so the
     gate and the continuous enforcement run identical code and cannot disagree.

   **`/api/ps`'s `size_vram` is the signal to use** — not `nvidia-smi` (a pod can pass it and
   still fail at inference, confirmed live) and not log-grepping. Two practical notes for anyone
   touching this: the ollama image ships **no `curl` or `wget`** (confirmed live:
   `exec: "curl": executable file not found in $PATH`), so anything HTTP against it must run from
   another pod; and the previous deploy-time check was a **no-op that never once fired** — it ran
   `kubectl logs --tail=50 | grep -qv "ggml_cuda_init: failed" || exit 1`, but `grep -qv PAT`
   succeeds whenever *any* line fails to match, which across 50 lines is always true. Treat any
   past run that "passed" that check as unverified. Asserting absence needs `! grep -q PAT`.

   Manual fix if something still slips through: `kubectl delete pod -l app=ollama`.

   **Standing constraint — never enable auto-sync on `mlops-llm-serving`.** Because `site.yml`
   patches the live ollama Deployment while deliberately leaving the sibling repo's manifest
   alone, that Application sits permanently `OutOfSync` (`Healthy`). The drift *is* the fix.
   Neither Application currently sets `syncPolicy.automated`, so nothing reverts it — but turning
   on auto-sync or selfHeal there would silently roll back `OLLAMA_KEEP_ALIVE=-1`,
   `OLLAMA_MAX_LOADED_MODELS=2`, and the 8Gi limit, reintroducing this bug whole. The
   `gpu-watchdog` would catch the resulting CPU fallback; nothing continuously checks the memory
   limit, so the 4Gi starvation half would come back unobserved. If you ever want that
   Application auto-synced, the patches have to move into the sibling repo's manifest first.

   **Validation run, 2026-08-30 (full teardown/rebuild, `ok=25 changed=11 failed=0
   unreachable=0`):** the `Recreate` rollout after `kubectl set env`/`set resources` settled
   without the single-GPU deadlock; both models warmed; the residency gate passed on its first
   attempt (no heal needed); canary passed; `✅ RUN COMPLETE — 4/4 documents settled`
   (`complete=2 review=2`, `posted_documents: 2`). The two `review` outcomes are bug #6, not this.
   The new chart objects (ServiceAccount/Role/RoleBinding + a 9th Deployment) synced through
   ArgoCD cleanly, and the watchdog has been polling `/api/ps` every 30s with `200 OK` since.

   **Read this result carefully — it does not clear bugs #1 and #3.** A warm GPU call is 0.079s,
   so extraction finishes far inside both the sweeper's 30s stuck threshold and the probe's 300s
   staleness window; neither bug *can* fire while the GPU is healthy. Confirmed in this run:
   extraction stayed at `RESTARTS 0` and the sweeper logged no `reconciler_stuck_docs_found` at
   all. That is the GPU fix working and **masking** the other two, not evidence they're fixed —
   both return the moment inference goes slow again.

   **Confirmed live, 2026-08-30: the race isn't only a startup-time thing — GPU access can also
   break *mid-session*, after the self-heal check already passed.** During a long `make e2e-k8s` run
   (extraction working through a real backlog for 15+ minutes), `ollama ps` started reporting both
   loaded models at `100% CPU` and `nvidia-smi` inside the pod started failing with the same `NVML`
   error — despite Ollama's own startup logs showing a clean GPU load minutes earlier
   (`offloaded 17/17 layers to GPU`). Per-call latency matched documented CPU-fallback speed
   (60-187s/call) instead of the documented warm-GPU baseline (~0.079s). The existing self-heal
   check only runs once, right after cluster bring-up, before real traffic starts — it has no
   coverage for degradation later in a session. `kubectl delete pod -l app=ollama` fixed it again
   live. **This is exactly what the `gpu-watchdog` Deployment now covers** — it polls `/api/ps`
   every 30s for the pod's whole lifetime and deletes it on `size_vram == 0`, so mid-session
   degradation is detected and healed without anyone watching latency. It also independently
   confirms the mechanism above: a mid-session drop back to CPU is what you would expect from the
   5-minute idle unload re-running `ggml_cuda_init`, which `OLLAMA_KEEP_ALIVE=-1` now prevents from
   happening at all. Between the two, the one-shot startup check is no longer the only line of
   defence.
3. **Fixed 2026-08-30 — the four timeouts sized for the same operation now derive from one
   constant.** `config.EXTRACTION_BUDGET_SECONDS` = `EXTRACTION_TIER_COUNT * (MAX_REPAIR_ATTEMPTS
   + 1) * LITELLM_TIMEOUT_SECONDS` = **1200s** at the defaults: what one `ocr.completed` message
   may legitimately take, worst case. Two of the four numbers were simply wrong:
   - `STUCK_THRESHOLD_SECONDS` was **30s** while `sweeper._claim_batch` selected on all of
     `IN_FLIGHT_STATES` including `extract_running` — so the sweeper claimed documents that were
     *being processed successfully*, burned their `extract_attempts`, kicked them back to
     `extract_pending`, and re-enqueued `ocr.completed`. A slow document exhausted all 5 attempts
     in ~2.5 minutes and landed in `failed` while the original worker went on to succeed; lag grew
     while work was being done (reading exactly like a stalled consumer); and the duplicate work
     piled onto the single Ollama pod, making the next redrive more likely — a positive feedback
     loop. Correctness held throughout (first-writer-wins + `IllegalTransition`); throughput and
     the attempt budget did not. Now split per stage: `STUCK_THRESHOLD_SECONDS` (30s) still covers
     text production, which is fast in every mode, while `EXTRACT_STUCK_THRESHOLD_SECONDS` covers
     `extract_*` and is budget-derived (1500s) under `EXTRACTION_MODE=real`, falling back to 30s in
     mock so the host loop and the test suite are unchanged.
   - `KAFKA_MAX_POLL_INTERVAL_MS` was **900s**, with a comment claiming it "covers two tiers plus
     repair retries at 200 each" — that arithmetic is 1200, so the value was *below* the budget it
     claimed to cover and a worst-case document would have been kicked from the group mid-process.
     Now derived: 1500s.

   The extraction `livenessProbe` is the deliberate exception and stays tight at 300s, because
   `extraction_4.run_funnel` now touches the heartbeat *before* each model call as well as after
   (see bug #1) — staleness tracks one bounded call, not the whole message budget.

   **Do not split `EXTRACT_STUCK_THRESHOLD_SECONDS` by state — measured and settled 2026-08-31.**
   `_claim_batch` applies one threshold to both `extract_pending` and `extract_running`. It is
   tempting to shorten it for `_pending` on the reasoning that nobody holds such a document, since
   during the forced-CPU run 5 of them waited the full 1500s for redrive. **That reasoning is
   wrong**, and the change would reintroduce this very bug on the `_pending` side.

   Measured directly under a 10-document forced-CPU backlog — sampling `extract_pending` counts
   and `rpk group describe extraction`'s TOTAL-LAG together — three consecutive stable samples:

       extract_pending=6  extract_running=3  ocr.completed TOTAL-LAG=9

   `lag == pending + running`, exactly. The pending documents' messages are **sitting in the topic,
   unconsumed** — they are queued behind slow inference, waiting for a free worker, not stranded.
   (The `_running` three still count toward lag because extraction commits the offset only after
   processing.) Nothing is lost, so there is nothing for a faster redrive to rescue: shortening the
   threshold would just republish duplicates of healthy queued work every sweeper cycle — the same
   theft-of-live-work this bug's fix removed, relocated from `_running` to `_pending`.

   The 1500s wait is therefore the system correctly declining to interfere with a deep queue, not a
   regression in time-to-recovery. **If you ever revisit this, the trigger is `lag ≈ 0` while
   documents sit in `extract_pending`** — that would mean messages genuinely went missing, and only
   then does a per-state split make sense. To re-check, sample both together under bug #1's
   forced-CPU recipe; discard any sample taken while the group is rebalancing, because `rpk`
   reports lag 0 there and that is indistinguishable from a drained topic.
4. **Fixed 2026-08-30 — the outbox relay no longer marks undelivered messages as published.**
   `relay_once` discarded `producer.flush()`'s return value; `flush()` reports how many messages
   are *still queued*, so on a slow or partitioned broker the `UPDATE outbox SET published_at`
   ran anyway and those messages were lost — silently, in the one component whose entire job is
   not losing them. `kafka_utils.publish()` now takes an `on_delivery` callback, and `relay_once`
   raises `outbox.DeliveryFailed` and rolls back if `flush()` reports anything unflushed or any
   delivery errored. Rows stay pending and the next tick retries; already-delivered messages get
   redelivered, which is correct — every consumer here is idempotent by construction, so
   at-least-once is the contract and at-most-once never was. Covered by
   `tests/test_relay_delivery.py`.
5. **Fixed 2026-08-30 — a `failed` document now says why.** `sweeper._claim_batch` transitioned
   attempt-capped documents straight to `failed` without setting `last_error`, so the column was
   empty for every document the sweeper gave up on — which is why "empty `last_error`" recurs in
   this file's evidence trails explaining nothing, and was once mistaken for a signal about *how* a
   document died. Now writes the state and attempt count via the new `ledger.set_last_error`.
6. **Fixed 2026-08-30 — `make summary`/`make summary-k8s` no longer fail on a healthy live
   pipeline.** `summarize.py` exits 1 on anything in-flight, which is right at the end of an e2e
   run and wrong as a standalone progress report — and the drain-wait that makes it safe is tagged
   `[e2e-k8s, replay-wait]`, so a bare `summary-k8s` never ran it. Confirmed live: a standalone
   `summary-k8s` failed the play on two documents that reached `complete` 4 and 8 seconds later,
   with extraction at `RESTARTS 0`, the sweeper idle, and LLM calls completing in 2.7-3.3s. The
   hard gate is now opt-in behind `--require-settled`, which `site.yml` passes only when
   `e2e`/`e2e-k8s` is in `ansible_run_tags`; standalone runs print `⏳ IN PROGRESS` and exit 0.
   **If you see `RUN INCOMPLETE`, check timestamps against the extraction log before concluding
   anything is stuck** — this class of false alarm has now cost two separate investigations.
7. **Fixed 2026-08-31 — the `arithmetic`-gate "false positive" was never a gate bug. The root cause
   was a hole in that same gate that disabled the funnel's tier escalation.** Long carried as an
   intermittent canary quirk and absorbed rather than fixed.

   The chase, because the order matters: bug #9's fix let OCR documents carry real text, which
   turned an occasional canary anomaly into three simultaneous reproductions of a sign inversion
   (`computed=800, declared=-800`). An A/B fixed that (prompt v1 → v3) — and revealed the model
   instead *omitting* `total_cents`. Chasing that revealed the real defect: `arithmetic` guarded its
   comparison with `total is not None`, so an extraction with no total returned **pass**. Three
   documents reached `complete` and were posted carrying no total at all, on text that plainly reads
   `Total: 800.00`.

   That hole is why everything else looked confusing. It was short-circuiting the two-tier funnel:
   documents "completed" on the cheap tier with unusable output instead of escalating. Measured
   across 3 prompt wordings × 16 extractions, the cheap tier (`llama3.2:1b`) produces **zero**
   verifiable extractions; the strong tier (`qwen2.5:1.5b`) passes **15/15**. With the gate honest,
   every document now escalates and completes on `tier=strong` with a correct total.

   Fixes: `arithmetic` returns `inconclusive` (which blocks — it is a blocking gate with
   `ON_INCONCLUSIVE=block`) when the total is missing, so **nothing the model omits can produce a
   pass**; and prompt v3 marks `total_cents` required. `config.PROMPT_VERSION` is bumped to
   `invoice-extract@v3` — `dlq_replay` re-drives on `prompt_version`, so a silent prompt edit makes
   old and new extractions indistinguishable.

   Resolved by the same root cause, having looked like a separate undetectable defect: the model
   read `800.00` as `800` cents rather than `80000`, consistently across line items, subtotal and
   total, so no gate could catch it. That was also cheap-tier output. Strong tier converts correctly
   (`4297.00 → 429700`).

   `canary.py`'s `run_canary()` still treats `review` as a pass under `EXTRACTION_MODE=real`. That
   absorption existed for this bug and is now removable, but only after a few clean runs confirm the
   canary reaches `complete` unaided.

   **Two lessons worth keeping.** A deterministic check disagreeing with a model is evidence about
   the model first — this sat open for sessions as "the gate is wrong" while the gate was right. And
   an `applies_to` predicate is not the only evasion surface: this gate was carefully hardened so a
   model could not switch it off by omitting `line_items`, and then let one through by omitting the
   very field being checked.

   **`review` rate is environment-dependent, not a property of the document — established by a
   failed prediction, 2026-08-31.** `local_scripts/replay_docs.py` builds every document with
   `canary.synthetic_invoice_pdf_bytes()`, so it is tempting to conclude replayed documents
   inherently trip this gate. They don't. Under forced-CPU degradation a 13-document replay landed
   13/13 in `review` (all `review:gates_exhausted`); on a healthy GPU, a 3-document replay landed
   3/3 in `complete`. Same code, same document generator, opposite outcomes. The review outcomes
   under CPU were the funnel exhausting tiers/repairs against slow-and-timing-out calls, not the
   `arithmetic` gate misfiring. **Practical consequence:** a high `review` count after
   `make replay-docs` means *inference is degraded*, and is worth investigating as a GPU problem
   (bug #2) rather than dismissed as expected gate noise.
8. **Fixed 2026-08-30 — `summarize.py` and `wait_for_drain.py` now close their connections.**
   Both opened a `ledger.connect()` and never closed it, exactly what "Connection-leak discipline"
   below forbids, and both now run via `kubectl exec` inside long-lived pods where an exception
   before exit strands an idle-in-transaction connection holding `AccessShareLock` on `documents` —
   the same class that caused multi-minute `TRUNCATE` hangs once already. Both wrapped in
   `try/finally`.
9. **Fixed 2026-08-31 — the mock-OCR registry was unreachable in k8s, so every OCR document
   extracted from the string `"unregistered page"`.** This was filed as a cosmetic path bug
   ("`DEFAULT_REGISTRY_PATH` resolves one directory too high; self-consistent, so nothing
   observably breaks"). The self-consistency argument holds only on the host, where the fixture
   generator and the OCR workers share a filesystem. **In k8s they are different pods**: a one-shot
   Job writes the registry into its own container and dies, and `ocr-shard` reads whatever is at
   that path in *its* container — which is the stale copy baked into the image at build time, whose
   doc_ids never match the freshly generated fixtures.

   Found by adding `three_page_scan` to the in-cluster fixture set (`FIXTURE_LIMIT=4`) and noticing
   the outcomes split cleanly by code path: `digital_clean`, the only fixture that never touches
   OCR, was the only one reaching `complete`; every OCR-dependent document landed in `review` with
   `gates_exhausted`. Confirmed directly — the assembled text was
   `'[mock-ocr:crc32c-6fc96:0] unregistered page'`, `ocr_engine.py`'s miss fallback. The gates were
   doing their job; there was genuinely nothing to extract.

   **Why it stayed invisible:** `review` is a tolerated outcome, `e2e-k8s` asserts only that
   documents *settled* rather than that they were extracted correctly, and the mechanical half of
   the OCR path (split, shard, scatter-gather join) worked perfectly the whole time — it was
   faithfully carrying placeholder text.

   **Fix:** the registry location is now `config.MOCK_OCR_REGISTRY_URI`, which accepts a `gs://` URI
   or a local path; `k8s/values.yaml` points it at GCS, which every pod already reaches and which
   `artifact.py` already uses for page text and shard output. The local default is also corrected to
   top-level `fixtures/generated/`, so `make reset`'s registry clear stops being a no-op.
   `MockOcrEngine` caches the registry per instance — `get_engine()` runs once per shard message, so
   that is one fetch per message rather than per page, and deliberately not process-global, since
   fixtures can be regenerated under a long-lived consumer and a stale cache there is silently wrong
   text.

   **Lesson worth keeping:** "self-consistent, so nothing breaks" is a statement about one
   deployment topology. Any file written by one process and read by another is a shared-storage
   question the moment those processes stop sharing a filesystem.
10. **Fixed 2026-08-31 (not yet validated live) — `wait_for_drain` could declare victory before all
    documents were ingested.** It counts what is *in the ledger now*, so a document the orphan
    detector has not yet discovered is invisible to it. The manifest.json target guards this, but
    **there is never a manifest in k8s** — the fixtures Job writes it into its own container, the
    same cross-pod trap as bug #9 — so every in-cluster drain fell through to the "total unchanged
    across two consecutive polls" fallback. At `poll_seconds=2.0` that is a 4s window against a 10s
    `ORPHAN_DETECTOR_INTERVAL_SECONDS`, so a document could be discovered *after* the check passed.
    Confirmed live: a replay drain reported `7/7 settled` while an 8th document was still being
    ingested (it completed a moment later, so the run was fine — the *check* was not).

    The fallback is now a time-based quiet period of two full ingest cycles rather than a count of
    polls, derived from `ORPHAN_DETECTOR_INTERVAL_SECONDS` so it tracks that value. This matters
    beyond replay: proving replayed documents reach a terminal state is `verify-loop`'s whole
    purpose, and it could previously pass without having looked at all of them.

## What this repo is

A local, runnable document-extraction pipeline built to production shape, sized for a ~20k docs/day
workload — covering the correctness core (ledger, outbox, scatter-gather join, quality gates,
sweeper, orphan detector) through the operational tier (real LLM, KEDA autoscaling, DLQ replay,
dead man's switch, canary, operator/break-glass lanes). Not the toy invoice-extraction eval
harness in
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
- `docpipeline/text/` — `pdf_utils.py` (shared by `triage_1.py` and `pdf_worker_2.py`) and
  `ocr_engine.py` (`MockOcrEngine` default, `TesseractOcrEngine` opt-in; shared by `ocr_shard_3.py`
  and `fixtures/generate_fixtures.py`). Moved out of `stages/` deliberately — both are helpers used
  by *multiple* stages (one of them even by fixture-generation code, not a stage at all), not a
  single stage's own logic, so living next to the 5 numbered sequential-step files made them look
  like "just another stage" when they aren't one.
- `docpipeline/stages/` — one module per pipeline stage, five of them numbered by dataflow order
  as a trailing suffix (`_1` through `_5` — a leading digit isn't a valid Python identifier, so
  `triage_1.py` not `1_triage.py`; helper modules that aren't independent steps stay unnumbered):
  `triage_1.py` (ingest + classify + dispatch), `pdf_worker_2.py`/`ocr_shard_3.py` (text
  production), `extraction_4.py` (the funnel: mock/cheap/strong tiers gated at each step),
  `llm_client.py` (real LLM tier, calls the sibling repo's `litellm` Deployment; also pushes
  gate-outcome Scores to Langfuse — see "Langfuse Score integration" below), `mock_llm.py`
  (default), `sink_stub_5.py`. Every `python -m docpipeline.stages.<name>` invocation
  (`k8s/values.yaml`'s `services:` list, `local_scripts/run_local.py`'s `SERVICES`) uses the numbered
  name; every in-repo import uses `from docpipeline.stages import triage_1 as triage` (etc.) so
  callers' own code bodies never have to change, just the one import line.
- `docpipeline/reconciliation/` — `sweeper.py` (stuck-state recovery, batch-capped,
  `SKIP LOCKED`), `orphan_detector_0.py` (numbered as the true step 0 — the actual ingest loop;
  GCS has no bucket-notification wiring locally, so this polls `inbox/` every 10s — the standard
  fallback for that, not a deviation), `dlq_replay.py`, `deadmans_switch.py`,
  `canary.py`, `terminal_report.py` (scheduled summary of what is parked in `failed` and
  `review` — both grow silently, and nothing automatic re-drives `review` at all; run by the
  `docpipeline-terminal-report` CronJob, `k8s/templates/cronjobs.yaml`),
  `prune.py` (retention for `outbox`/`attempt_log`, the two tables nothing else deletes from),
  `operator.py` (read-only + break-glass lanes — see "The two operator lanes" below),
  `gpu_watchdog.py` (keeps ollama's models pinned in VRAM and restarts the ollama pod when it has
  silently fallen back to CPU — see "Known open bugs" #2; the only module in this repo that talks
  to the Kubernetes API rather than Postgres/Kafka/GCS, hence the only one with a
  `serviceAccountName`).
- `config.py`, `fixture_content.py` — stay top-level, not subpackaged: `config.py` is imported by
  every other module (subpackaging it would just add an import hop with no grouping benefit), and
  `fixture_content.py` is shared fixture text, not pipeline logic.
- `ansible/site.yml` — the only orchestration layer (see "Ansible task ordering" below). `Makefile`
  targets are thin `ansible-playbook --tags <name>` aliases; there is no bash-script orchestration
  left (`local_scripts/reset.sh`/`init_db.sh` were deleted once Ansible fully covered them).
- `argocd/application.yaml`, `argocd/keda-application.yaml` — the two ArgoCD Applications. See
  "ArgoCD" below for why there are two and why both are still applied via raw `kubectl`.
- `k8s/` — a Helm chart (`Chart.yaml`, `values.yaml`, `templates/`), converted this session from 3
  flat manifests (`configmap.yaml`, `deployments.yaml`'s 8 near-identical Deployments,
  `keda.yaml`). `templates/deployment.yaml` is one template `range`d over `values.yaml`'s
  `services:` list — adding a 10th consumer is a values entry, not a copy-pasted Deployment block
  (`gpu-watchdog` was added exactly that way, plus an optional `serviceAccountName`).
  `templates/configmap.yaml`/`templates/infra.yaml`/`templates/jobs.yaml`/`templates/keda.yaml` are
  similarly values-driven where it matters. ArgoCD detects this as a Helm source automatically
  (`Chart.yaml`'s presence is the only signal it needs) — no change to `argocd/application.yaml`'s
  `path: k8s` or to `--local ./k8s` sync required. Helm-rendered output was diffed object-for-object
  against the original flat manifests before the old files were deleted (byte-identical on all 11
  objects). This whole directory is what `docpipeline`'s Application syncs — `templates/infra.yaml`
  (Postgres/Redis/Redpanda/fake-gcs-server, sync-wave `-1`) and `templates/jobs.yaml`
  (migrate/topics/fixtures one-off Jobs, sync-wave `0`) are part of that same sync now too, not a
  separate raw-kubectl step — see [HISTORY.md](HISTORY.md)'s ArgoCD/Helm migration entry for the
  full story and the sync-wave gotcha that came with it.
- `local_scripts/` — `run_local.py` (host-process orchestrator for `make e2e`, has its own SIGTERM
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
`( {{ dotenv_cmd }} exec uv run python3 local_scripts/run_local.py ) > log 2>&1 & echo $! > pidfile`. This
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

**Reproduced live again, 2026-08-30 — this time outside Ansible entirely.** Ad-hoc bash testing
used `timeout 25 uv run python3 local_scripts/run_local.py & PID=$!; ...; kill $PID` to validate
`replay_docs.py` — the exact same trap: `$!` captured `uv`'s wrapper, not `run_local.py`, so `kill
$PID` silently killed nothing and the entire consumer stack (`run_local.py` + all 8 children) kept
running unsupervised in the background for 20+ minutes, processing whatever showed up in `inbox/`
the whole time. Symptom: an unrelated `make summary` run reported one document stuck in
`text_pending` that nobody could explain, because it wasn't from anything the person running
`make summary` had done. Fixed by killing the process group directly (`kill -9 -- "-$PID"`, the
same pattern the Ansible task already uses) once found via `ps aux | grep docpipeline`. **The
lesson generalizes beyond Ansible: never background `uv run <long-lived-script>` with plain `&` +
`$!` and expect `kill` to work, in a shell script or interactively — always exec the venv
interpreter directly, or accept that you'll need a process-group kill (`kill -9 -- "-$PID"`) instead
of a plain one.**

## ArgoCD: both apps, no exceptions but two

Every K8s-manifest deploy in this repo — the 9 app Deployments, the in-cluster infra
(Postgres/Redis/Redpanda/fake-gcs-server), Prometheus/Grafana, the setup Jobs, ConfigMap, and
ScaledObjects, all in `k8s/` — *and* the KEDA operator itself — goes through ArgoCD (`argocd app
sync <name>`), not raw
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
host-only `make e2e` stays a ~15s loop with no minikube needed), but `k8s/values.yaml`'s `config.EXTRACTION_MODE` sets
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
The actual fix is `fixtures/generate_fixtures.py`'s `FIXTURE_LIMIT` env var — the in-cluster
fixtures Job (`k8s/templates/jobs.yaml`) sets `FIXTURE_LIMIT=4`, while the plain `fixtures`/`e2e`
tags still generate the full 14 for `make e2e`'s mock-mode host loop. 4 is the smallest set
covering every distinct path: tier-0, OCR-fallback, single-shard OCR, and multi-shard split+join.
**The 4th matters and was missing until 2026-08-31** — at `FIXTURE_LIMIT=3` the scatter-gather
join never ran in the k8s path at all, only in mock host mode, which left the one piece of code
where bad SQL is a genuine correctness bug unexercised in the real deployment. The canary's own `--slo-seconds` (900, in `site.yml`) is separate margin on top of that fix, not
the fix itself — if this class of failure resurfaces, check queue depth first
(`SELECT state, count(*) FROM documents GROUP BY state`) before assuming a bigger number will help;
a growing queue means contention (fix: `FIXTURE_LIMIT`), a single stuck `extract_pending` document
with a low queue count means something else broke.

**`make e2e-k8s` didn't actually wait for the fixtures to finish, only the canary.** The canary's
900s SLO blocks on its own one synthetic document; the 3 real fixtures race it for the same Ollama
pod and often finish around the same time, but that's incidental serialization, not a guarantee —
`summarize.py` (final step) could catch a fixture mid-flight (e.g. still working through a
multi-round schema repair loop, confirmed live to take 10+ minutes once). Fixed by adding a
`local_scripts/wait_for_drain.py 900` step (via `kubectl exec`) before the summary, mirroring what
`make e2e` (host) already did — its manifest.json fallback (stability across two consecutive polls)
is what actually applies here, since the Job that generates `fixtures/generated/manifest.json` is a
different, short-lived pod from `docpipeline-triage`. This matters more since `summarize.py` now
exits 1 on anything still in-flight (below) — before that change a premature snapshot was just a
confusing printout; now it fails the whole play.

**Confirmed live, separately: real Ollama inference time is host-load-dependent, not fixed —
CPU-only.** Even with contention resolved (`FIXTURE_LIMIT=4`), a single real CPU-inference call was
observed anywhere from ~150s to 7.5 minutes depending on what else the host was doing (load average
~7 after hours of continuous minikube rebuilds in one session). 900s is margin for that variance,
not a claim about typical CPU latency — don't read it as "real extraction normally takes 15
minutes." **GPU passthrough makes this moot — but only on the runs where it actually works**: a
warm GPU call completed in 0.079s, three orders of magnitude faster, making host load essentially
irrelevant to canary timing. The caveat matters: known open bug #2 (GPU-registration race) makes
inference silently fall back to CPU on a large fraction of `make e2e-k8s` runs, and on those runs
these 150s-450s numbers are the norm, not history. That coupling is why bug #2 is a suspected
trigger for bug #1 — a 300s liveness probe survives a 0.079s call and cannot survive a 450s one.

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

## The reconciliation tier runs three different ways

Conceptually one tier, three execution models — worth knowing which is which
before adding to it:

| lane | how | why |
|---|---|---|
| `sweeper`, `orphan_detector_0`, `gpu_watchdog` | **Deployment**, own `while True` + sleep | need sub-minute cadence (10-30s), below what a CronJob can express |
| `terminal_report`, `prune`, `deadmans_switch` | **CronJob** (`k8s/templates/cronjobs.yaml`) | daily/periodic; a permanently-running pod would just be a pod that sleeps |
| `dlq_replay`, `canary` | **one-shot**, no workload object | invoked deliberately — `make dlq-replay` is "after a deploy, retry what the old version failed", and the canary is an e2e step |

**None of them consume a Kafka topic** — zero `make_consumer` calls across the
whole tier. The numbered `stages/` are message-driven; reconciliation is
poll-based against Postgres, GCS or the Kubernetes API. That is deliberate: the
bus is one of the things this tier exists to recover from, so a sweeper that
needed Kafka to run could not fix a Kafka-caused stall.

`deadmans_switch` was one-shot until 2026-08-31, reachable only by a human
typing `make deadmans-switch` — which defeats its purpose, since it exists to
catch the case where nobody is looking. It is now a CronJob at the same 15
minute cadence as `DEADMANS_SWITCH_WINDOW_SECONDS`, so consecutive checks tile
the window they inspect. It exits non-zero on unhealthy, so the Job goes
`Failed` and is visible to anything watching Job status.

## Retention and growth

Two tables grow without bound, and both are invisible until they aren't:
`outbox` and `attempt_log`. Nothing deleted from either until 2026-08-31.

**Why it stays hidden.** Both hot paths use partial indexes —
`outbox_pending_idx` covers only `published_at IS NULL`, and the sweeper's
`documents_inflight_idx` only the four in-flight states — so index size tracks
*concurrency*, not history, and query latency never degrades as the heap grows.
The symptom is disk exhaustion or autovacuum falling behind, not slow queries.
At ~20k docs/day that is roughly 7M `documents` rows a year (fine) against tens
of millions of `outbox` and `attempt_log` rows (not).

`docpipeline/reconciliation/prune.py` handles it, on a nightly CronJob. Two
properties in it are load-bearing:

- **The outbox predicate is on `published_at`, never `created_at`.** An
  unpublished row can be arbitrarily old — broker down, delivery failing — and
  deleting one destroys an undelivered message, exactly the loss the outbox
  exists to prevent, reintroduced by its own cleanup. `tests/test_prune.py`
  asserts a 999-day-old *pending* row survives a 1-day retention window.
- **Batched with a commit between batches.** One DELETE over tens of millions
  of rows is a single enormous transaction that holds a lock and blocks
  autovacuum for its duration — causing the bloat it was meant to prevent.

`attempt_log` is pruned by **id watermark**, not timestamp: `started_at`/
`ended_at` are both optional there, so a time predicate would skip undated rows
forever. `id` is bigserial and therefore monotonic in insert order, so a
watermark taken from the newest datable old row also sweeps the undated rows
interleaved among them.

**The better answer at real volume is partitioning, not deletion.** Partition
by time and `DROP TABLE` the old partition: O(1), no WAL churn, no dead tuples,
versus a DELETE treadmill you run forever. That is a schema migration rather
than a job and is deliberately not done here. If you add it, the one guard it
needs is: never drop a partition containing `published_at IS NULL` rows.

**Not worth archiving.** A published outbox row duplicates what is already
durable — `documents.extraction_result`, `posted_documents`, `attempt_log`, and
the GCS artifacts. The outbox is a queue, not a record. If a compliance
requirement ever appears, the thing worth retaining is the posted *events*, not
the queue mechanics that carried them.

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

## Open decision: dropping docker-compose, isolating e2e onto k8s

**Raised 2026-08-30, deliberately deferred — not a bug, a direction.** The intent is that
`e2e` should live entirely on k8s and `docker-compose.yml` should stop being a dependency of the
main path (a separate stack for the pytest suite is fine if it needs one). Nothing has been
changed for this yet; `make test` is explicitly staying as-is for now.

What makes it more than a delete, and what has to be decided first:

- `docker-compose` backs the *whole* host path, not just the test database: `up`, `down`,
  `init-db`, `reset`, `topics`, `fixtures`, `run-local`, `test`, and `make e2e`. Removing it
  removes host-mode `make e2e` entirely.
- **The coverage question is the real one.** Host `make e2e` runs **all 14 fixtures** in mock mode
  in ~15s and is what currently "already proves every fixture's correctness" (see the
  `FIXTURE_LIMIT` discussion under "Ollama is one pod, not N"). `e2e-k8s` runs only **4**, on
  purpose, because 14 documents contending for one Ollama pod is too slow. So deleting host mode
  silently drops a 10-fixture correctness proof unless it is replaced.
- The obvious replacement is an in-cluster *mock-mode* pass (`EXTRACTION_MODE=mock`, all 14
  fixtures) — it never calls Ollama, so the contention argument doesn't apply and it should stay
  fast. That keeps the coverage and still makes k8s the only stack. Not implemented; flagged as
  the leading option, not a decision.
- The pytest suite itself is already hermetic (`conftest.py`'s `_force_mock_extraction_mode`) and
  needs only a Postgres and a fake-GCS. The cluster has both, and `canary`/`summarize`/
  `wait_for_drain` already run via `kubectl exec deploy/docpipeline-triage`, so pointing pytest at
  the same place would be consistent — it would need `tests/` and `pytest` in the image, which
  they currently aren't.

## Things intentionally left alone (not in scope here)

- No Argo Workflow wrapper around the operator lanes — Argo Workflows already runs in this same
  minikube cluster for the sibling repo, so a `WorkflowTemplate` that shells out to
  `python -m docpipeline.reconciliation.operator` would be additive, not a redesign. Not done here
  because the interesting part is the guardrails inside `operator.py`, not the YAML that invokes it.
- No CI enforcing the quality gates or the read-only/break-glass role split — both are currently
  proven only by the test suite and by running the thing live, not by a pipeline gate. A known,
  deliberate gap, not an oversight.
- Tesseract OCR and the real-LLM tier both stay opt-in behind env vars
  (`OCR_ENGINE=tesseract`, `EXTRACTION_MODE=real`) rather than becoming the default — mock stays
  the default for the large majority of tests, which keeps the suite fast and hermetic.
