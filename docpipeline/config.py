"""Scaled-down local limits.

Every value here has a production counterpart in the design doc's "Scaled-down
limits" table. The point of running locally is exercising the same code
paths, not the same capacity — so these are deliberately tiny (a 3-page
document should force a real split+shard+join, not sail through a single
shard), never a shortcut past the mechanism.
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

# --- Kafka / Redpanda ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# librdkafka's 300s default is shorter than a real LLM extraction call under
# tier escalation (confirmed live: MAXPOLL exceeded, consumer left the group
# mid-processing, in a loop that never let a document finish). 900s covers
# two tiers plus repair retries at LITELLM_TIMEOUT_SECONDS=200 each.
KAFKA_MAX_POLL_INTERVAL_MS = _int("KAFKA_MAX_POLL_INTERVAL_MS", 900_000)

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

# --- Redis ---
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6380/0")

# --- OCR ---
OCR_ENGINE = os.environ.get("OCR_ENGINE", "mock")  # mock | tesseract
OCR_DPI = _int("OCR_DPI", 150)

# --- Scaled-down limits (see doc's "Scaled-down limits" table) ---
SHARD_SIZE_PAGES = _int("SHARD_SIZE_PAGES", 1)              # prod: 4
HARD_PAGE_CEILING = _int("HARD_PAGE_CEILING", 20)            # prod: 5000
SWEEPER_BATCH_CAP = _int("SWEEPER_BATCH_CAP", 5)             # prod: 500
SWEEPER_CADENCE_SECONDS = _int("SWEEPER_CADENCE_SECONDS", 30)  # prod: 600
STUCK_THRESHOLD_SECONDS = _int("STUCK_THRESHOLD_SECONDS", 30)  # prod: ~3x p99
RELAY_POLL_SECONDS = _float("RELAY_POLL_SECONDS", 2.0)        # prod: ~0.2
RELAY_BATCH_CAP = _int("RELAY_BATCH_CAP", 50)
ORPHAN_DETECTOR_INTERVAL_SECONDS = _int("ORPHAN_DETECTOR_INTERVAL_SECONDS", 10)
MAX_TEXT_ATTEMPTS = _int("MAX_TEXT_ATTEMPTS", 5)
MAX_EXTRACT_ATTEMPTS = _int("MAX_EXTRACT_ATTEMPTS", 5)
MAX_REPAIR_ATTEMPTS = _int("MAX_REPAIR_ATTEMPTS", 2)

FUNNEL_VERSION = _int("FUNNEL_VERSION", 1)
GATE_SET_VERSION = _int("GATE_SET_VERSION", 1)
BUILD_SHA = os.environ.get("BUILD_SHA", "local-dev")
PROMPT_VERSION = os.environ.get("PROMPT_VERSION", "invoice-extract@v1")

PLAUSIBLE_TOTAL_CEILING_CENTS = _int("PLAUSIBLE_TOTAL_CEILING_CENTS", 10_000_000_00)

# --- Step 8: real LLM tier, behind the same LiteLLM gateway the sibling
# mlops-llm-repo already runs (already Langfuse-wired server-side — every
# request through it is auto-traced with no SDK calls needed for the trace
# itself). "mock" keeps the deterministic path from steps 0-7; "real" swaps
# in docpipeline.llm_client.
EXTRACTION_MODE = os.environ.get("EXTRACTION_MODE", "mock")  # mock | real
LITELLM_BASE_URL = os.environ.get("LITELLM_BASE_URL", "http://localhost:4000")
LITELLM_MASTER_KEY = os.environ.get("LITELLM_MASTER_KEY", "sk-demo-key")
LITELLM_TIER_MODELS = {"cheap": "cheap-fast", "strong": "cheap-balanced"}
LITELLM_TIMEOUT_SECONDS = _float("LITELLM_TIMEOUT_SECONDS", 200.0)  # small CPU model under load is slow, not hung

# llm_client.extract() passes doc_id as metadata.trace_id on every LiteLLM
# call, so litellm's own Langfuse callback lands every tier/repair attempt
# for one document on one trace. These three let docpipeline push its own
# gate-outcome Scores onto that same trace afterward (see
# llm_client.push_gate_scores) -- same demo keys mlops-llm-repo/k8s/litellm.yaml
# already uses server-side, since they're the same Langfuse project.
LANGFUSE_HOST = os.environ.get("LANGFUSE_HOST", "http://localhost:3001")
LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY", "pk-lf-demo-local-0000")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY", "sk-lf-demo-local-0000")

# --- Step 9: reconciliation extras ---
DEADMANS_SWITCH_WINDOW_SECONDS = _int("DEADMANS_SWITCH_WINDOW_SECONDS", 900)  # prod: 15 min
CANARY_SLO_SECONDS = _int("CANARY_SLO_SECONDS", 60)
BREAK_GLASS_BLAST_RADIUS_CAP = _int("BREAK_GLASS_BLAST_RADIUS_CAP", 25)

# The auto-post kill switch itself lives in the `feature_flags` table (see
# migrations/002_operator_lanes.sql and ledger.get_feature_flag) rather than
# here — it must be flippable without a redeploy, which an import-time
# constant can't do.
