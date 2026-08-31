"""Scaled-down local limits.

Deliberately tiny so local runs exercise the same code paths as production, not
the same capacity — a 3-page document should force a real split+shard+join, not
sail through a single shard. Never a shortcut past the mechanism.
"""

from __future__ import annotations

import os


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


# --- Postgres ---
PG_DSN_RW = os.environ.get(
    "PG_DSN_RW",
    "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
        user=os.environ.get("PIPELINE_RW_USER", "pipeline_rw"),
        pw=os.environ.get("PIPELINE_RW_PASSWORD", "pipeline_rw_pw"),
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "55432"),
        db=os.environ.get("PGDATABASE", "docpipeline"),
    ),
)
PG_DSN_RO = os.environ.get(
    "PG_DSN_RO",
    "postgresql://{user}:{pw}@{host}:{port}/{db}".format(
        user=os.environ.get("PIPELINE_RO_USER", "pipeline_ro"),
        pw=os.environ.get("PIPELINE_RO_PASSWORD", "pipeline_ro_pw"),
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "55432"),
        db=os.environ.get("PGDATABASE", "docpipeline"),
    ),
)
# Turns a lock-blocked query from a silent indefinite hang into a raised,
# redelivered exception. ledger.connect() applies it session-level, so it
# covers every consumer.
PG_STATEMENT_TIMEOUT_MS = _int("PG_STATEMENT_TIMEOUT_MS", 30_000)

# --- Kafka / Redpanda ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# KAFKA_MAX_POLL_INTERVAL_MS is derived from EXTRACTION_BUDGET_SECONDS at the
# bottom of this file -- see "The extraction time budget".
#
# make_consumer() sleeps a random [0, this] before creating the Consumer, to
# spread the simultaneous JoinGroup a KEDA rollout produces. Off by default:
# host mode never runs more than one replica per group. k8s/values.yaml sets
# it in-cluster. (Kept on its merits; it never fixed the bug it was added for
# -- AGENT.md "Known open bugs" #1.)
KAFKA_JOIN_JITTER_SECONDS = _float("KAFKA_JOIN_JITTER_SECONDS", 0.0)
# librdkafka group/rebalance logging, opt-in -- too noisy to leave on.
KAFKA_CONSUMER_DEBUG = os.environ.get("KAFKA_CONSUMER_DEBUG", "") == "1"

TOPICS = [
    "triage.requests",
    "text.embedded",
    "ocr.split",
    "ocr.shard",
    "ocr.completed",
    "extract.repair",
    "extract.escalate",
    "document.extracted",
    "triage.requests.dlq",
    "text.embedded.dlq",
    "ocr.split.dlq",
    "ocr.shard.dlq",
    "ocr.completed.dlq",
    "document.extracted.dlq",
]
TOPIC_PARTITIONS = _int("KAFKA_PARTITIONS", 3)

# --- GCS (fake-gcs-server) ---
STORAGE_EMULATOR_HOST = os.environ.get("STORAGE_EMULATOR_HOST", "http://localhost:4443")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "docpipeline-local")
GOOGLE_CLOUD_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "local-project")
# google-cloud-storage's per-call default is undocumented and version-dependent,
# so every call in infra/gcs.py passes this explicitly rather than inheriting it.
GCS_TIMEOUT_SECONDS = _float("GCS_TIMEOUT_SECONDS", 30.0)

# --- Redis ---
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380/0")

# --- OCR ---
OCR_ENGINE = os.environ.get("OCR_ENGINE", "mock")  # mock | tesseract
OCR_DPI = _int("OCR_DPI", 150)
# Mock-OCR registry: a gs:// URI, or empty for the local file. Must be shared
# storage wherever the fixture generator and OCR workers are different pods --
# in k8s they are, and a local path silently degrades every OCR document to
# "unregistered page" (AGENT.md bug #9).
MOCK_OCR_REGISTRY_URI = os.environ.get("MOCK_OCR_REGISTRY_URI", "")

# --- Scaled-down limits (prod counterparts in the trailing comments) ---
SHARD_SIZE_PAGES = _int("SHARD_SIZE_PAGES", 1)              # prod: 4
HARD_PAGE_CEILING = _int("HARD_PAGE_CEILING", 20)            # prod: 5000
SWEEPER_BATCH_CAP = _int("SWEEPER_BATCH_CAP", 5)             # prod: 500
SWEEPER_CADENCE_SECONDS = _int("SWEEPER_CADENCE_SECONDS", 30)  # prod: 600
STUCK_THRESHOLD_SECONDS = _int("STUCK_THRESHOLD_SECONDS", 30)  # prod: ~3x p99
RELAY_POLL_SECONDS = _float("RELAY_POLL_SECONDS", 2.0)        # prod: ~0.2
RELAY_BATCH_CAP = _int("RELAY_BATCH_CAP", 50)
# How long relay_once waits for broker acks before rolling back rather than
# marking posted. Bounds how long one tick blocks everything behind it.
RELAY_FLUSH_TIMEOUT_SECONDS = _float("RELAY_FLUSH_TIMEOUT_SECONDS", 10.0)
ORPHAN_DETECTOR_INTERVAL_SECONDS = _int("ORPHAN_DETECTOR_INTERVAL_SECONDS", 10)
# Retention for the two unbounded tables (docpipeline.reconciliation.prune).
# outbox is a queue, not a record -- a published row duplicates what is already
# in documents/posted_documents/attempt_log, so 7 days is generous headroom for
# debugging a delivery, not a data-retention decision. attempt_log is genuine
# diagnostic history, hence longer.
OUTBOX_RETENTION_DAYS = _int("OUTBOX_RETENTION_DAYS", 7)
ATTEMPT_LOG_RETENTION_DAYS = _int("ATTEMPT_LOG_RETENTION_DAYS", 30)
MAX_TEXT_ATTEMPTS = _int("MAX_TEXT_ATTEMPTS", 5)
MAX_EXTRACT_ATTEMPTS = _int("MAX_EXTRACT_ATTEMPTS", 5)
MAX_REPAIR_ATTEMPTS = _int("MAX_REPAIR_ATTEMPTS", 2)

FUNNEL_VERSION = _int("FUNNEL_VERSION", 1)
GATE_SET_VERSION = _int("GATE_SET_VERSION", 1)
BUILD_SHA = os.environ.get("BUILD_SHA", "local-dev")
# Bump on every prompt change: dlq_replay re-drives documents whose
# prompt_version moved, so a silent edit makes old and new extractions
# indistinguishable. v3 marks total_cents required -- earlier wordings produced
# sign-flipped totals, then omitted ones (AGENT.md bug #7).
PROMPT_VERSION = os.environ.get("PROMPT_VERSION", "invoice-extract@v3")

PLAUSIBLE_TOTAL_CEILING_CENTS = _int("PLAUSIBLE_TOTAL_CEILING_CENTS", 10_000_000_00)

# --- Real LLM tier, behind the sibling mlops-llm-repo's LiteLLM gateway ---
# That gateway is Langfuse-wired server-side, so every request is auto-traced.
# "mock" is the deterministic path; "real" swaps in llm_client. Defaults to
# mock so host-mode runs need no cluster; k8s/values.yaml sets real in-cluster.
EXTRACTION_MODE = os.environ.get("EXTRACTION_MODE", "mock")  # mock | real
LITELLM_BASE_URL = os.environ.get("LITELLM_BASE_URL", "http://localhost:4000")
LITELLM_MASTER_KEY = os.environ.get("LITELLM_MASTER_KEY", "sk-demo-key")
LITELLM_TIER_MODELS = {"cheap": "cheap-fast", "strong": "cheap-balanced"}
LITELLM_TIMEOUT_SECONDS = _float("LITELLM_TIMEOUT_SECONDS", 200.0)  # small CPU model under load is slow, not hung

# llm_client passes doc_id as metadata.trace_id, so every tier/repair attempt
# for a document lands on one trace; these let it push gate-outcome Scores onto
# that same trace. Same demo keys litellm uses -- same Langfuse project.
LANGFUSE_HOST = os.environ.get("LANGFUSE_HOST", "http://localhost:3001")
LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY", "pk-lf-demo-local-0000")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY", "sk-lf-demo-local-0000")

# --- The extraction time budget: one number, three consumers ---
# Worst-case time for one ocr.completed message: two tiers x
# (MAX_REPAIR_ATTEMPTS + 1) calls x LITELLM_TIMEOUT_SECONDS = 1200s default.
# Exists because four timeouts for this same operation were sized independently
# and disagreed by 30x, two of them wrong. Derive, don't guess.
#
# The extraction livenessProbe is the deliberate exception at 300s, because
# extraction_4.py touches the heartbeat around every model call -- staleness
# tracks one bounded call, not the whole budget. Remove those touches and the
# probe must grow to this budget instead.
EXTRACTION_TIER_COUNT = 2  # cheap -> strong, see LITELLM_TIER_MODELS
EXTRACTION_BUDGET_SECONDS = int(
    EXTRACTION_TIER_COUNT * (MAX_REPAIR_ATTEMPTS + 1) * LITELLM_TIMEOUT_SECONDS
)
# Margin for schema validation, gates and commits. A stuck threshold that
# exactly equals the work's own bound is a race by construction.
_BUDGET_MARGIN_SECONDS = 300

# librdkafka's 300s default is far shorter than a real extraction under tier
# escalation, which kicks the consumer from the group mid-processing.
KAFKA_MAX_POLL_INTERVAL_MS = _int(
    "KAFKA_MAX_POLL_INTERVAL_MS", (EXTRACTION_BUDGET_SECONDS + _BUDGET_MARGIN_SECONDS) * 1000
)

# Per-stage stuck threshold. Text production is fast in every mode, so
# STUCK_THRESHOLD_SECONDS stays small; real extraction is not, and a threshold
# shorter than one LLM call makes the sweeper redrive documents that are being
# processed successfully. Known cost: this also delays recovery of genuinely
# stranded extract_pending documents to 1500s -- see AGENT.md bug #3.
EXTRACT_STUCK_THRESHOLD_SECONDS = _int(
    "EXTRACT_STUCK_THRESHOLD_SECONDS",
    (EXTRACTION_BUDGET_SECONDS + _BUDGET_MARGIN_SECONDS) if EXTRACTION_MODE == "real" else STUCK_THRESHOLD_SECONDS,
)

# --- Ollama GPU watchdog (AGENT.md "Known open bugs" #2) ---
# Ollama falling back to CPU is degradation, not failure: it stays Ready and
# keeps answering, ~3 orders of magnitude slower (0.079s GPU vs 150-450s CPU).
# `/api/ps`'s per-model size_vram is the only structured signal that tells the
# two apart -- 0 means CPU-resident. nvidia-smi does not (a pod can pass it and
# still infer on CPU).
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
# Both tiers pinned, not just cheap: the funnel escalates cheap -> strong, and a
# strong-tier cold load is a fresh ggml_cuda_init, i.e. another chance at CPU.
OLLAMA_MODELS = [m for m in os.environ.get("OLLAMA_MODELS", "llama3.2:1b,qwen2.5:1.5b").split(",") if m]
GPU_WATCHDOG_INTERVAL_SECONDS = _int("GPU_WATCHDOG_INTERVAL_SECONDS", 30)
# Caps pod deletes to one per interval, so a genuinely GPU-less host degrades to
# periodic restarts rather than a thrash loop.
GPU_WATCHDOG_RESTART_COOLDOWN_SECONDS = _int("GPU_WATCHDOG_RESTART_COOLDOWN_SECONDS", 300)
# Off by default: host mode has no ollama, and this needs in-cluster RBAC.
GPU_WATCHDOG_ENABLED = os.environ.get("GPU_WATCHDOG_ENABLED", "") == "1"
GPU_WATCHDOG_DRY_RUN = os.environ.get("GPU_WATCHDOG_DRY_RUN", "") == "1"  # observe, never delete
K8S_NAMESPACE = os.environ.get("K8S_NAMESPACE", "default")

# --- Reconciliation extras ---
DEADMANS_SWITCH_WINDOW_SECONDS = _int("DEADMANS_SWITCH_WINDOW_SECONDS", 900)  # prod: 15 min
CANARY_SLO_SECONDS = _int("CANARY_SLO_SECONDS", 60)
BREAK_GLASS_BLAST_RADIUS_CAP = _int("BREAK_GLASS_BLAST_RADIUS_CAP", 25)

# The auto-post kill switch lives in the `feature_flags` table, not here -- it
# must be flippable without a redeploy, which an import-time constant can't do.
