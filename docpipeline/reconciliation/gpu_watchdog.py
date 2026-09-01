"""Ollama GPU watchdog — keeps the model pinned in VRAM, and churns the pod
when it isn't.

See AGENT.md bug #2. Ollama's GPU failure mode is *silent
degradation*: `ggml_cuda_init` fails, ollama falls back to CPU, and the pod
stays `Running`/`Ready` answering every request 3 orders of magnitude slower
(0.079s -> 150-450s). Nothing in Kubernetes can see that — ollama's own
liveness probe is an HTTP GET that passes fine on CPU — so the degradation
persists until a human notices, and downstream it looks like extraction
hanging (bug #1), not like a GPU problem.

Two things make it *recur* rather than being a one-time bring-up race:

1. `OLLAMA_KEEP_ALIVE` defaults to 5 minutes, so an idle model unloads and its
   `llama-server` subprocess exits. The next request re-spawns it and re-runs
   `ggml_cuda_init` — every idle gap is another chance to land on CPU. This
   pipeline is mostly idle gaps.
2. The funnel escalates cheap -> strong across *two* models, so tier
   escalation alone can force a load/evict cycle.

This runs as its own single-replica Deployment rather than a sidecar because
the sibling mlops-llm-repo owns ollama's pod spec and this repo deliberately
doesn't edit it (see the k8s/values.yaml entry). Both halves of the fix live
here instead:

- *keep it pinned*: re-request every tier's model with `keep_alive: -1` when it
  isn't resident, so `ggml_cuda_init` runs once per pod lifetime instead of
  once per idle gap.
- *otherwise let it churn*: when a model is resident but on CPU
  (`size_vram == 0`), delete the ollama pod so it reschedules past the
  driver-init window — the same `kubectl delete pod` that has fixed this every
  time it has been observed, just applied continuously instead of once at
  deploy time.

Deliberately not a livenessProbe on ollama itself: that would require editing
the sibling repo's manifest, and a probe can only restart the pod, not re-pin a
model that merely unloaded (the common case, which needs no restart at all).
"""

from __future__ import annotations

import argparse
import logging
import time

import httpx

from docpipeline import config

log = logging.getLogger(__name__)

# The in-cluster Kubernetes API, reached with the pod's own mounted
# ServiceAccount token — no kubernetes client library needed for one
# deletecollection call. RBAC is in k8s/templates/rbac.yaml.
_SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
_K8S_API = "https://kubernetes.default.svc"


class OllamaUnreachable(Exception):
    pass


def loaded_models() -> list[dict]:
    """`/api/ps`'s view of what is resident right now. Each entry carries
    `size_vram` — 0 means the model loaded onto CPU, which is the whole signal
    this module exists to detect."""
    try:
        resp = httpx.get(f"{config.OLLAMA_BASE_URL}/api/ps", timeout=10)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise OllamaUnreachable(str(exc)) from exc
    return resp.json().get("models", [])


def classify(models: list[dict]) -> tuple[set[str], set[str]]:
    """Returns (resident_on_gpu, resident_on_cpu) model names.

    A model absent from both sets simply isn't loaded — that's the idle-unload
    case, which needs a re-pin, not a restart.
    """
    on_gpu, on_cpu = set(), set()
    for m in models:
        name = m.get("name") or m.get("model", "")
        (on_gpu if m.get("size_vram", 0) > 0 else on_cpu).add(name)
    return on_gpu, on_cpu


def pin(model: str) -> None:
    """Load `model` and hold it resident indefinitely.

    `keep_alive: -1` is the per-request form of `OLLAMA_KEEP_ALIVE=-1`, which
    is what makes this fixable from outside ollama's own pod spec. An empty
    prompt loads the model without generating anything.
    """
    resp = httpx.post(
        f"{config.OLLAMA_BASE_URL}/api/generate",
        json={"model": model, "prompt": "", "keep_alive": -1},
        # A cold load pulls ~1-1.5GB of tensors onto the GPU; generous, since
        # the alternative to waiting is a spurious restart.
        timeout=300,
    )
    resp.raise_for_status()


def restart_ollama() -> None:
    """Delete the ollama pod(s) so the Deployment reschedules past whatever
    driver-init window broke CUDA for this one."""
    with open(f"{_SA_DIR}/token") as f:
        token = f.read().strip()
    resp = httpx.request(
        "DELETE",
        f"{_K8S_API}/api/v1/namespaces/{config.K8S_NAMESPACE}/pods",
        params={"labelSelector": "app=ollama"},
        headers={"Authorization": f"Bearer {token}"},
        verify=f"{_SA_DIR}/ca.crt",
        timeout=30,
    )
    resp.raise_for_status()


def check_once(last_restart: float) -> float:
    """One poll. Returns the (possibly updated) last-restart timestamp."""
    try:
        models = loaded_models()
    except OllamaUnreachable as exc:
        # Ollama restarting, or not up yet. Not our problem to fix and not
        # evidence of a GPU fault — say so and wait.
        log.info("gpu_watchdog ollama unreachable (%s); will retry", exc)
        return last_restart

    on_gpu, on_cpu = classify(models)

    if on_cpu:
        # The actual fault. A CPU-resident model will never move to the GPU on
        # its own -- only a reschedule fixes it.
        since = time.time() - last_restart
        if config.GPU_WATCHDOG_DRY_RUN:
            log.error("gpu_watchdog DRY RUN: would restart ollama, cpu_resident=%s", sorted(on_cpu))
            return last_restart
        if since < config.GPU_WATCHDOG_RESTART_COOLDOWN_SECONDS:
            log.error(
                "gpu_watchdog ollama on CPU (%s) but in cooldown (%.0fs of %ds elapsed) — "
                "not restarting; if this repeats the GPU is likely unavailable on this host, "
                "not merely mis-initialised",
                sorted(on_cpu), since, config.GPU_WATCHDOG_RESTART_COOLDOWN_SECONDS,
            )
            return last_restart
        log.error("gpu_watchdog ollama fell back to CPU (%s) — deleting pod to reschedule", sorted(on_cpu))
        try:
            restart_ollama()
        except httpx.HTTPError as exc:
            # Almost always missing RBAC; worth naming explicitly since the
            # watchdog is otherwise silently useless.
            log.exception("gpu_watchdog could not delete ollama pod (RBAC? %s)", exc)
            return last_restart
        return time.time()

    missing = [m for m in config.OLLAMA_MODELS if m not in on_gpu]
    if missing:
        # Idle unload, or never loaded. Re-pin rather than restart: nothing is
        # broken yet, and this is exactly the moment that would otherwise
        # become a cold ggml_cuda_init on the pipeline's critical path.
        for model in missing:
            try:
                pin(model)
                log.info("gpu_watchdog pinned %s (keep_alive=-1)", model)
            except httpx.HTTPError as exc:
                log.warning("gpu_watchdog failed to pin %s: %s", model, exc)
    return last_restart


def wait_healthy(timeout_seconds: int) -> bool:
    """Poll until every configured model is GPU-resident, or give up.

    Used by `--check-once` (i.e. by ansible/site.yml at deploy time) rather
    than by the loop: after a heal there is a pod restart plus a cold model
    load to wait out, and "not resident yet" is not the same as "on CPU."
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            on_gpu, on_cpu = classify(loaded_models())
        except OllamaUnreachable:
            time.sleep(5)
            continue
        if not on_cpu and all(m in on_gpu for m in config.OLLAMA_MODELS):
            log.info("gpu_watchdog healthy: %s resident in VRAM", sorted(on_gpu))
            return True
        time.sleep(5)
    return False


def run_forever() -> None:
    if not config.GPU_WATCHDOG_ENABLED:
        # Host-mode `make e2e` runs deterministic extraction with no ollama at all.
        # Idle rather than crash-looping so the Deployment can stay in the
        # chart unconditionally.
        log.info("gpu_watchdog disabled (GPU_WATCHDOG_ENABLED != 1); idling")
        while True:
            time.sleep(3600)

    log.info(
        "gpu_watchdog started: ollama=%s models=%s interval=%ds cooldown=%ds dry_run=%s",
        config.OLLAMA_BASE_URL, config.OLLAMA_MODELS,
        config.GPU_WATCHDOG_INTERVAL_SECONDS, config.GPU_WATCHDOG_RESTART_COOLDOWN_SECONDS,
        config.GPU_WATCHDOG_DRY_RUN,
    )
    last_restart = 0.0
    while True:
        try:
            last_restart = check_once(last_restart)
        except Exception:
            # Same contract as every other consumer's run_forever: never die
            # on an unexpected error, log it and keep polling.
            log.exception("gpu_watchdog check failed")
        time.sleep(config.GPU_WATCHDOG_INTERVAL_SECONDS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    # ansible/site.yml runs this form at deploy time, via `kubectl exec` into
    # this same Deployment. Deliberately the *same* code the loop runs, so the
    # deploy-time gate and the continuous enforcement can never disagree about
    # what "on the GPU" means — the previous deploy-time check was a separate
    # `kubectl logs | grep` that silently diverged (and was inverted, so it
    # never fired at all).
    parser.add_argument("--check-once", action="store_true",
                        help="run one heal/pin cycle, wait for VRAM residency, exit non-zero if not healthy")
    parser.add_argument("--timeout", type=int, default=600,
                        help="seconds to wait for residency after healing (--check-once only)")
    args = parser.parse_args()

    if not args.check_once:
        run_forever()
        return

    check_once(0.0)  # last_restart=0 -> no cooldown, an explicit check may always heal
    if not wait_healthy(args.timeout):
        raise SystemExit(
            f"ollama is not GPU-resident after {args.timeout}s — models={config.OLLAMA_MODELS}. "
            "Refusing to continue on CPU inference: it is ~3 orders of magnitude slower and "
            "will trip extraction's liveness probe (see AGENT.md's bug register)."
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    main()
