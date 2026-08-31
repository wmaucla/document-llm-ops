# HISTORY.md

A chronological, session-by-session log of the debugging and build work behind this repo's
current shape — every bug found live, every hypothesis chased (including the wrong ones), and
why each fix looks the way it does. Entries are **newest first**.

For a lean, current-state reference (architecture, gotchas that still matter, and *open* bugs),
see [AGENT.md](AGENT.md) instead — this file is the "how we got here," not the "how it works now."

---

**2026-08-31 (later) — `maxReplicaCount` restored to 3 and the forced-CPU reproduction finally
run: bug #1's fix has direct evidence for the first time, bug #3's fix validated in both
directions, and `replay-docs` turns out to reproduce bug #7 on every document.**

Every previous "clean" extraction run was on a healthy GPU, where a 2.5s call trips neither the
300s liveness probe nor any stuck threshold — so bugs #1 and #3 were *structurally unable* to
fire and every green run was masking, not evidence. AGENT.md said the missing test was multiple
replicas under slow inference. This session finally ran it.

*Setup.* `k8s/values.yaml`'s `extraction.maxReplicaCount` back to 3 (from the 1-cap that had stood
since 2026-08-30), synced via `make deploy` — no rebuild needed, it's a ScaledObject field. Then
the degradation, in an order that matters: the deploy-time residency gate *fails the play* on a
CPU-resident ollama and the `gpu-watchdog` deletes the pod on `size_vram == 0` every 300s, so both
had to be worked around rather than fought — a full healthy `e2e-k8s` first, then
`kubectl scale deploy/docpipeline-gpu-watchdog --replicas=0` and
`kubectl set env deploy/ollama CUDA_VISIBLE_DEVICES=""`, then a 10-document backlog via
`make replay-docs COUNT=10`. Confirmed degraded: GPU down to 322 MiB, host load to 12.3, calls at
~114s instead of 2.5s, and a cold `qwen2.5:1.5b` load mid-pipeline (the ollama restart discarded
`site.yml`'s warm-up — bug #2(a)'s scenario, reproduced incidentally).

*Result 1 — bug #1's fix works.* 29 minutes, 3 replicas processing concurrently, real CPU latency,
tier escalation, cold model load: **zero restarts, zero liveness warnings**. The window where an
untouched heartbeat would have killed a pod (300s staleness + `failureThreshold: 3` ×
`periodSeconds: 30` ≈ 390s) passed repeatedly with the pods demonstrably busy rather than idle.
The `touch()` before each model call does bound staleness to one call as claimed. **This still
does not prove the probe caused the *original* symptom** — the pre-fix code was never re-run under
these conditions for a direct comparison. Correct status: mechanism closed, hypothesis now
supported by direct evidence, not proven.

*Result 2 — bug #3's fix works. A possible cost was spotted, then downgraded to an open question.*
The sweeper correctly left slow-but-live `extract_running` documents alone for many minutes —
exactly the theft-of-live-work it was written to stop, and the first time that has been exercised
against genuinely slow extraction. Separately, 5 documents sat in `extract_pending` and were
redriven only after the full `EXTRACT_STUCK_THRESHOLD_SECONDS`: published 12:55:26, redriven
13:20:56, i.e. 1500s, where the pre-fix 30s threshold would have taken ~30 seconds.

This was first written up here as "5 stranded documents nobody was coming for," with a threshold
split (short for `extract_pending`, budget-derived for `extract_running`) as the obvious fix.
**That overstated the evidence.** What was actually observed is that 5 documents sat in
`extract_pending` for 1500s and were then redriven. Whether their `ocr.completed` messages were
*lost* — or merely *queued behind slow inference*, waiting their turn exactly like the 3 documents
that drained normally at 13:19 with no redrive at all — was never established. Consumer-group lag
was not captured at the time, and it is the one measurement that separates the two.

The distinction decides the fix, and inverts it. If the messages were lost, a shorter
`extract_pending` threshold is right. If they were queued, a shorter threshold recreates bug #3's
original failure — the sweeper redriving work that is progressing fine — merely relocated from
`extract_running` to `extract_pending`, and with a deep backlog it would republish every queued
document every sweeper cycle. `_claim_batch` cannot tell the two apart from the `documents` table
alone, which is *why* one threshold currently covers both states.

**Measured the same day, and the answer was QUEUED — the proposed fix would have been a
regression.** `local_scripts/measure_pending_lag.py` was written for exactly this question and run
under a fresh 10-document forced-CPU backlog. Three consecutive stable samples, identical:
`extract_pending=6  extract_running=3  ocr.completed TOTAL-LAG=9`. That is `lag == pending +
running` exactly: every pending document's message was still sitting in the topic unconsumed, and
the three running ones counted toward lag only because extraction commits the offset after
processing. Nothing had been lost, so there was nothing for a faster redrive to rescue — a short
`extract_pending` threshold would have republished duplicates of healthy queued work every sweeper
cycle, which is this bug's original failure mode moved from `_running` to `_pending`.

So the 1500s wait is the system correctly declining to interfere with a deep queue, and the
single threshold stays. Two process notes worth keeping: the measurement had to be *built* before
the fix could be judged, and it is the second time in one session that a confidently-written
finding was overturned by going and looking — the first being the `replay-docs`/bug #7 claim above.
Both errors had the same shape: generalising from data collected under a degraded environment
without a controlled comparison.

*Result 3 — a `review` spike tracks inference health, not document shape. This was written up
backwards first, and a prediction caught it within the hour.* Final state after restoring the GPU:
`20/20 documents settled, 0 stuck`, but `complete=5 review=15` — all 13 replayed documents landed
in `review`, every one `review:gates_exhausted`. The tempting inference, which this file briefly
recorded as fact, was that `local_scripts/replay_docs.py` builds each document with
`canary.synthetic_invoice_pdf_bytes()` — the exact synthetic invoice bug #7 says the small model
mis-extracts — and that replays therefore inherently trip the `arithmetic` gate.

**That was wrong.** The claim was restated as a falsifiable prediction against the next fresh
`make verify-loop` on a healthy GPU: 3 replayed documents should land 3/3 in `review`. They landed
3/3 in `complete` (`total: 7 complete=5 review=2`, the two reviews being the canary and one fixture
from phase 1, unchanged from before the replay). Same code, same generator, opposite outcome — so
the 13/13 review rate came from the *forced-CPU environment* those documents were processed in (the
funnel exhausting tiers and repair attempts against slow, timing-out calls), not from anything
about the documents themselves. The mistake was inferring a property of the document from data
gathered entirely under a degraded environment, with no controlled comparison.

The genuinely useful version of this finding is the inverse: **a `review` spike after
`make replay-docs` is a signal that inference has degraded** — worth chasing as a bug #2 GPU
problem — rather than expected gate noise to be waved through.

*Two false alarms worth recording, since this file's whole theme is that they are expensive.*
A monitor counting `kubectl get events --field-selector reason=Killing | grep extraction` reported
a probe kill that never happened — all three matches were `Normal`/"Stopping container extraction"
on old ReplicaSets from the deploy rollout. Bug #1's signature is `Warning` + "failed liveness
probe" + `Exit Code: 137`, and `restartCount` is the ground truth. Separately, watching
`extract_pending` drain from 8 to 5 was briefly read as "the stranding hypothesis is wrong" — it
wasn't; 3 documents drained normally while 5 genuinely were stranded, and both were true at once.
Partial drainage does not falsify a stranding claim.

---

**2026-08-31 — first start-to-finish `make verify-loop`: passed 7/7, confirmed bug #2's fix on a
clean rebuild, and exercised a sweeper recovery path the 2026-08-30 baseline never hit.**

Prior sessions had run every piece of `verify-loop` in order against a live cluster, but never as
one invocation — this was the first. Exit 0, all five phases: `e2e-k8s` (`ok=25 changed=11
failed=0`, the same recap as the 2026-08-30 validation), standalone `summary-k8s` (`ok=1
failed=0`), `replay-docs COUNT=3`, `replay-wait` (drained; the timeout guard `skipped`), and a
final `summary-k8s` reporting `✅ RUN COMPLETE — 7/7 documents settled, 0 stuck in-flight`
(`complete=5 review=2`, `posted_documents: 5`).

*Verified independently of the exit code*, since the Ansible recap doesn't surface `summarize.py`'s
stdout and "failed=0" only means the play didn't error: all 11 pods at `RESTARTS 0`; GPU at
3252 MiB resident (matching the ~2.8 GiB both models need); `gpu-watchdog` polling `/api/ps` every
30s with `200 OK` and **zero heals or pod deletions**; LLM calls at ~2.5s each
(12:36:39.707 → 12:36:42.232), three orders of magnitude off the 150-450s CPU-fallback range.
`kubectl get deploy ollama` confirmed both patches live: `OLLAMA_KEEP_ALIVE=-1`,
`OLLAMA_MAX_LOADED_MODELS=2`, and `limits.memory: 8Gi`.

*The standalone `summary-k8s` calls are the direct regression test for bug #6* — before that fix a
bare `summary-k8s` could fail the play on documents that were merely in flight. Both passed.

*What this does not prove.* Bugs #1 and #3 remain unconfirmed, exactly as AGENT.md's bug #2 entry
warns. A 2.5s call trips neither the 300s liveness probe nor either stuck threshold, so
`RESTARTS 0` here is the GPU fix **masking** them, not evidence they are fixed. This run is not
grounds for reverting `extraction.maxReplicaCount` from 1.

*New behaviour vs. the baseline: the sweeper fired once.*
`reconciler_stuck_docs_found{state=text_pending} 1`, where the 2026-08-30 run logged none at all.
Benign, and arguably the most useful new datum in the run: `text_pending` is covered by the 30s
`STUCK_THRESHOLD_SECONDS`, which is the correct threshold for text production in every mode, the
redrive succeeded, and everything still settled 7/7. It is specifically **not** bug #3's failure
mode — that was the sweeper claiming `extract_running` documents and burning their attempt budget,
and both `review` documents finished at `extract_attempts=1`, so nothing was burned. The recovery
path now has live coverage it previously lacked.

The two `review` outcomes are bug #7 (`extraction ... -> review:gates_exhausted` on the first
attempt), expected and absorbed.

*Latent risk found while checking ArgoCD state, not previously recorded anywhere.*
`mlops-llm-serving` sits permanently `OutOfSync` (`Healthy`), which is the unavoidable consequence
of `site.yml` patching the live ollama Deployment while deliberately not editing the sibling repo's
manifest — the drift *is* the fix. Neither Application has `syncPolicy.automated`, so nothing
reverts it today. But enabling auto-sync or selfHeal on that Application would silently revert both
patches and reintroduce bug #2 in full. The `gpu-watchdog` would catch the resulting CPU fallback;
it would **not** catch the return of the 4Gi memory starvation, which has no continuous check at
all. Recorded in AGENT.md's bug #2 as a standing constraint.

Also fixed this session, all documentation rather than code: AGENT.md's bug #1 still asserted as
current a `KAFKA_MAX_POLL_INTERVAL_MS = 900_000` self-contradiction that bug #3 had already fixed
(it derives to 1500s); the presentation's A4-adjacent claims still advertised extraction's 1→3 KEDA
autoscaling while it is capped at 1; and `agent-handoff.md` had gone actively misleading — its "do
this first" section described the extraction heartbeat fix as `NOT YET IMPLEMENTED` when it had
shipped, proposed a background-thread design where an inline touch-before-call is what actually
landed, and its bug numbering no longer mapped to AGENT.md's. That file was a punch list whose
items are now all resolved or tracked in AGENT.md, so it was deleted rather than corrected, as its
own header instructed.

---

**2026-08-30 — ollama's GPU "falters and gets released": root-caused to a 5-minute default nobody
overrode, plus a memory-starved container. Fixed entirely from this repo; the sibling
mlops-llm-repo's `ollama.yaml` is deliberately untouched.**

Framing going in was AGENT.md's existing bug #2: "GPU-registration race on cluster rebuild,"
guarded by a one-shot self-heal in `ansible/site.yml`. That framing turned out to cover only the
bring-up half, and the guard turned out not to work at all.

*Evidence, from the live pod rather than the docs.* `kubectl logs deploy/ollama` showed the pod
was on GPU and healthy at that moment — `NVIDIA GeForce RTX 2080 Ti (10818 MiB)`,
`offloaded 17/17 layers to GPU`, both models in `CUDA0` buffers (~2.2 GiB of tensors). So the GPU
is acquirable; the question was why it stops being held. Two answers, both visible in
`kubectl describe pod -l app=ollama`:

1. **`Environment: <none>`.** `OLLAMA_KEEP_ALIVE` was at its default of 5 minutes, so an idle model
   unloads and its `llama-server` subprocess exits; the next request re-spawns it and re-runs
   `ggml_cuda_init`. Every idle gap is a fresh chance to land on CPU, and with `FIXTURE_LIMIT=3`
   plus a canary this pipeline is mostly idle gaps. This is what explains the observation that
   never fit the bring-up-race theory: a pod that passed the deploy-time warm check being on CPU
   twenty minutes later *without ever restarting*. Compounding it, only the cheap tier's model was
   ever warmed, so a strong-tier escalation cold-loaded `qwen2.5:1.5b` for the first time
   mid-pipeline, unwarmed and unverified.
2. **`system_free="768.2 MiB" system_total="4.0 GiB"`**, with ollama's loader logging `disabling
   mmap for llama-server load due to host memory pressure`. That is the 4Gi cgroup limit, not VRAM
   — `CUDA_Host` pinned buffers count against it even though the tensors are in VRAM. The 2080 Ti
   has 10.6 GiB and both models need ~2.8 GiB, so VRAM was never the constraint.

*The old guard was a no-op.* `site.yml`'s post-self-heal verification ran
`kubectl logs --tail=50 | grep -qv "ggml_cuda_init: failed" || (echo ...; exit 1)`. `grep -qv PAT`
exits 0 when *any* line fails to match, which across 50 log lines is always true — so the
`|| exit 1` was unreachable and every run "passed" regardless of GPU state. Treat any historical
run that "passed" this check as unverified. Asserting absence needs `! grep -q PAT`.

*Two things found only by probing the live cluster, both of which would otherwise have shipped
broken.* The ollama image ships **no `curl` and no `wget`** (`exec: "curl": executable file not
found in $PATH`), which invalidated a first design that had ansible `kubectl exec`-ing HTTP calls
into the ollama pod. And `nvidia-smi` remains useless as a signal (a pod can pass it and still
fail at real inference, per the earlier session) — `/api/ps`'s per-model `size_vram` is the
structured signal that actually distinguishes GPU from CPU residency, `0` meaning CPU.

*Fixes.* All in `document-llm-ops`; the sibling repo owns ollama's manifest and this repo patches
the live object instead:
- `ansible/site.yml` runs `kubectl set env deployment/ollama OLLAMA_KEEP_ALIVE=-1
  OLLAMA_MAX_LOADED_MODELS=2` and `kubectl set resources --limits=memory=8Gi --requests=memory=2Gi`
  right after the sibling's terraform applies it, then warms *both* tiers' models via
  `ollama run` (the CLI, since there's no curl in that image). Pinning collapses N
  `ggml_cuda_init` rolls into one per pod lifetime, which is the single biggest change here.
- New `docpipeline/reconciliation/gpu_watchdog.py` + a `gpu-watchdog` Deployment
  (`k8s/values.yaml`) with a narrowly-scoped ServiceAccount/Role/RoleBinding
  (`k8s/templates/rbac.yaml`, pods list/delete in one namespace only). It polls `/api/ps` every
  30s, re-pins a model that merely unloaded, and deletes the ollama pod when a model comes back
  CPU-resident — rate-limited by a 300s cooldown so a genuinely GPU-less host degrades to periodic
  restarts rather than a thrash loop. This is the "otherwise let the pod churn" half, automated
  and continuous, which is what the one-shot deploy-time check never was.
- `site.yml`'s deploy-time gate now execs `gpu_watchdog --check-once` inside that same pod, so the
  gate and the continuous enforcement run identical code and cannot silently diverge the way the
  grep did. `k8s/templates/deployment.yaml` gained an optional `serviceAccountName`.
- `tests/test_gpu_watchdog.py` covers the decision table — the interesting part being which of
  three outcomes each ollama state maps to, since a missed CPU fallback costs 3 orders of
  magnitude of latency while a spurious restart kills in-flight inference for nothing.

*Deliberately not done:* a custom ollama image. The stock image already auto-detects CUDA
correctly (proven live above), and a rebuilt image can't fix device wiring or the mid-life reload,
which is the actual recurring failure. It would only buy faster restarts.

*Suspected coupling to bug #1,* recorded in AGENT.md: CPU-fallback inference runs 150-450s per
call against extraction's 300s liveness probe, so this is a plausible trigger for the
"wedged extraction consumer" restarts rather than an unrelated issue.

*Live-validated the same day, first attempt, full teardown/rebuild:* `ok=25 changed=11 failed=0
unreachable=0`. Every new step held — the `Recreate` rollout after `kubectl set env`/`set
resources` settled without the single-GPU deadlock, both models warmed, the residency gate passed
without needing its heal path, the canary passed, and `summarize.py` reported `✅ RUN COMPLETE —
4/4 documents settled` (`complete=2 review=2`, `posted_documents: 2`; the two `review`s are the
known arithmetic-gate false positive, unrelated). The new Helm objects (ServiceAccount, Role,
RoleBinding, and a 9th Deployment via the new optional `serviceAccountName`) synced through ArgoCD
without incident. Extraction stayed at `RESTARTS 0` and the sweeper logged no
`reconciler_stuck_docs_found` — which is the GPU fix *masking* bugs #1 and #3 (a 0.079s call
finishes far inside both the 30s sweeper threshold and the 300s probe window), not evidence either
is fixed. Recorded explicitly in AGENT.md so a future green run isn't misread as clearing them.

*Audit findings from the same session, documented but not fixed* (AGENT.md bugs #3, #4, #5, #7):
the sweeper's 30s `STUCK_THRESHOLD_SECONDS` is shorter than a single real LLM call so it redrives
actively-processing documents; `outbox.relay_once` discards `producer.flush()`'s return value and
can mark undelivered messages as published; the sweeper never writes `last_error` when it caps a
document; and `summarize.py`/`wait_for_drain.py` leak their ledger connections. The `last_error`
finding also corrected an overstatement made earlier in the same session — an empty `last_error`
had been cited as evidence for the bug #1 liveness-probe hypothesis when it is simply what the
sweeper's give-up path always produces.

---

**2026-08-30 — the "wedged extraction consumer" hunt, in full: five hypotheses, four mitigations,
and a late re-reading of the evidence that inverts the causality.** This is the narrative behind
AGENT.md's "Known open bugs" #1, moved here so that entry can stay current-state only. Ordered as
it actually happened.

*Original signature.* A `docpipeline-extraction` replica silently stops consuming after processing
some number of messages: no crash, no restart, no error logged, just goes quiet while still
`Running`/`Ready`. Reproduced independently 3 times across fresh clusters, always with the same
tell (`kubectl exec deploy/redpanda -- rpk group describe extraction` shows lag stuck on one
partition while others are caught up), each time confirmed by waiting well past the canary's 900s
SLO with zero recovery.

*Mitigation round 1 — make the symptom self-heal.* Reasoning at the time: every code path in
`extraction_4.py` that could raise is already caught and logged by `run_forever()`'s own
`except Exception`, so a silent hang with zero log output can only mean the process is genuinely
*blocked* on something with no timeout. Three changes to close that gap without the root cause:
- `docpipeline/infra/heartbeat.py` touches `/tmp/heartbeat` at the top of every poll-loop iteration
  and immediately after every model response inside `run_funnel`'s repair loop.
- `ledger.connect()` sets a session-level `statement_timeout` (`PG_STATEMENT_TIMEOUT_MS`, default
  30s) on every consumer's connection, ruling out a stuck-on-a-DB-lock hang.
- `k8s/values.yaml`'s `extraction` service gets a `livenessProbe` (300s staleness, sized as
  "200s `LITELLM_TIMEOUT_SECONDS` + 100s margin") execing a heartbeat-freshness check.

*Live test — probe works, pod re-wedges immediately, and the hang looks earlier than documented.*
A real `make e2e-k8s` canary run failed its 900s SLO. The extraction pod had restarted **4 times**
via the liveness probe (`Exit Code: 137`, `Reason: Killing — Container extraction failed liveness
probe`, recurring every ~5-6 minutes) — the mitigation genuinely fires. But every crashed
container's logs showed **only** `"extraction consumer started"` — no httpx call, no gate result.
Read at the time as: the hang happens *before* `run_funnel` is ever reached, earlier than the
"successful LLM call, then silence" signature of the 3 prior reproductions. Ruled out live: Kafka
group was `Stable`/`0 lag`, litellm answered a health check in 1.1s, no blocked queries in
`pg_stat_activity`. That left the GCS reads in `ensure_assembled` as the strongest candidate —
code inspection confirmed **none passed an explicit `timeout`**, relying on
`google-cloud-storage`'s undocumented per-call default. Fixed: every call in `gcs.py` now passes
`timeout=config.GCS_TIMEOUT_SECONDS` (30s).

*Re-test — GCS timeout did NOT fix it.* Fresh teardown/rebuild with the timeout in place. The run
succeeded end to end (`✅ RUN COMPLETE`, canary passed) only because drain-wait + sweeper give-up
now bound it — but one extraction replica still restarted twice, with **identical** logs: only
`"extraction consumer started"`, no exception, despite the process now having *two* independent
timeouts (DB, GCS) on every blocking call it should reach before the model call. Two documents
landed in `failed` (`extract_attempts=5`, empty `last_error`). Hypothesis revised to
`kafka_utils.poll_json()`'s underlying `Consumer.poll()` blocking past its nominal 1.0s timeout at
the librdkafka/socket level — a known confluent-kafka issue class around reconnects, and this
always happened shortly after `site.yml`'s post-deploy rollout-restart.

*Narrowed to a simultaneous-join race.* Caught live on a running cluster: `rpk group describe
extraction` matched per-partition lag to per-pod IPs and logs. Two of three replicas (`5mtqg`,
`pxslf`) showed zero activity past `"extraction consumer started"` with their partitions' lag
steadily *growing*, minutes after the group itself reported `STATE Stable` — so not "waiting for a
rebalance." The decisive-looking clue: **both wedged replicas had started at the exact same
timestamp** (the same rollout-restart, which restarts a KEDA-scaled Deployment's replicas
simultaneously), while a third replica that had separately restarted solo came up and worked
immediately. Read as a JoinGroup/SyncGroup thundering herd, structurally possible only for the two
KEDA-scaled consumers (`extraction`, `ocr-shard`).

*Mitigation round 2 — join jitter. Did not close the gap.* `make_consumer()` now sleeps
`random.uniform(0, KAFKA_JOIN_JITTER_SECONDS)` before creating the `Consumer` (default `0`/off;
`k8s/values.yaml` sets `8`). Added `KAFKA_CONSUMER_DEBUG=1` (off by default) for librdkafka's own
`consumer,cgrp` logging. Pod start timestamps did come back staggered — jitter structurally
working. **But lag still climbed (88 → 94), and `rpk group describe extraction`'s
partition-to-member table pointed at pod IPs that no longer existed** — none of the three running
replicas' IPs appeared in the assignment output. Group reported `STATE Stable` and `MEMBERS 7`
(should be 3). Read as the coordinator failing to reassign partitions away from departed members.
Caveat noted at the time: that cluster had been through heavy consumer-group churn from the same
session's testing against a never-recreated Redpanda.

*Caveat resolved — reproduced clean.* Full teardown/rebuild (new minikube, new Redpanda, no
leftover churn). Same pattern at the same point: right after the post-deploy rollout-restart, 2 of
3 extraction replicas took a liveness-probe restart within a couple minutes (the third stayed at
0), and the canary genuinely failed — `did not reach complete within 900s SLO`. Jitter confirmed
not sufficient on a clean cluster.

*Workaround — `maxReplicaCount` capped at 1 (was 3).* A single replica structurally can't race
siblings that don't exist. Costs little real throughput locally (every replica already serializes
on the one Ollama pod), but `extraction` no longer demonstrates KEDA autoscaling in the
presentation's A4 appendix (`ocr-shard`, 1-5, still does and has never shown this issue).

*False alarm along the way, worth keeping as a diagnostic lesson.* During a post-rebuild check,
one document sat at `extract_pending` for several minutes with no new log lines from its assigned
replica — identical-looking to the signature above — but on re-check it had genuinely finished
(`review:gates_exhausted`, `repair_attempts=3`, `extract_attempts=2`), and
`pg_stat_activity`/`pg_locks` showed no blocked queries at any point. Cause of the *false alarm*:
a document needing multiple schema-repair rounds issues one full LLM call per round, and every
extraction replica shares the *same single* Ollama pod — 3 repair rounds queued behind other
replicas' calls easily reach 10+ minutes for one document, with long silent gaps between its
`httpx ... 200 OK` lines that look exactly like a hang. **Lesson: don't conclude "wedged" without
either (a) an actual blocked query in `pg_stat_activity`/`pg_locks`, or (b) waiting past the
canary's own 900s SLO.**

*Audit re-reading of the accumulated evidence — causality is probably inverted.* Reviewing the
above as a whole rather than session by session: `heartbeat.touch()` is only called on message
pickup and *after a model call returns successfully* — every failure branch of `run_funnel`'s
repair loop (including the transient/timeout `continue`) loops without touching it. The funnel
permits 6 model calls per message (2 tiers × `MAX_REPAIR_ATTEMPTS + 1`) at
`LITELLM_TIMEOUT_SECONDS=200` each, so up to ~1200s of legitimate silence — against a 300s probe
with `failureThreshold: 1`. Two slow calls, or one at the ~450s CPU latency this same session
recorded, trips it. Under that reading every artifact above is explained without any Kafka bug:
the "only `extraction consumer started`" logs are an in-flight `httpx.post` that hasn't returned
yet (not a hang before `run_funnel`); the ~5-6 minute restart cadence *is* the 300s threshold; the
`MEMBERS 7` / stale-IP assignments are ghosts left by SIGKILL sending no graceful `LeaveGroup`;
`extract_attempts=5` with an empty `last_error` is what a killed process leaves behind, since
`increment_attempts` commits before the funnel runs; and both prior "fixes" failed because neither
GCS timeouts nor join jitter addresses call *latency*. Also notable: `extraction` is the only
service with a `livenessProbe` at all, which explains the apparent "only KEDA-scaled consumers"
correlation better than a join race does (`ocr-shard` scales 1→5 and has never shown this). The
repo also contradicts itself here — `config.py` sets `KAFKA_MAX_POLL_INTERVAL_MS = 900_000`
explicitly to cover "two tiers plus repair retries at 200s each," while the probe comment sizes
300s from a single call. Not yet tested live; see AGENT.md's bug #1 for the current state and the
falsification test.

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
