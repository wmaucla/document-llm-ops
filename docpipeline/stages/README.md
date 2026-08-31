# docpipeline/stages/

The pipeline proper — one module per stage, run as a standalone Kafka consumer (`python -m
docpipeline.stages.<name>`, one Deployment each in `k8s/values.yaml`'s `services:` list).

The five sequential-dataflow files carry a trailing step number (a leading digit isn't a valid
Python identifier, so `triage_1.py`, not `1_triage.py`). Helper modules that aren't independent
steps — `deterministic_extractor.py`, `llm_client.py` — stay unnumbered. Shared cross-stage helpers (`pdf_utils`,
`ocr_engine`) live in [`../text/`](../text/README.md), not here.

| File | Stage | What it does |
|---|---|---|
| `triage_1.py` | 0 | The only writer of the initial ledger row. Two passes: reject zero-byte/unsupported-MIME/encrypted/corrupt/oversized uploads outright, then classify what's left (doc type, has-a-text-layer) and dispatch. Ledger row + outbox row, same transaction, always. Single replica. |
| `pdf_worker_2.py` | 1 | **A fork point, not a straight-through step.** Tier-0 pypdf text extraction if there's already a usable text layer (→ skips OCR entirely, straight to `extract_pending`); otherwise physically splits the PDF into page-range shards and hands off to `ocr_shard_3.py`. |
| `ocr_shard_3.py` | 2 | Rasterizes + OCRs one page-range shard at a time, independent and parallel across replicas (KEDA-scaled 1–5). The scatter-gather join lives in `core/ledger.py`; the winning shard publishes, it does not reassemble. |
| `extraction_4.py` | 3 | The extraction funnel: mock → cheap → strong tiers, each result checked by all five quality gates before anything auto-posts. First-writer-wins on commit. KEDA-scaled 1–3. |
| `sink_stub_5.py` | 4 | The downstream contract's local stand-in — consumes `document.extracted` and does the final write. |
| `deterministic_extractor.py` | — | The default extraction backend (steps 0–7's deterministic path) — a real component with programmable per-scenario behaviors (`injected_total`, `swapped_roles`, `refusal`, `context_overflow`), not a stub. |
| `queries.py` + `sql/` | The one multi-line statement these stages issue (`post_document`), loaded from `sql/sink.sql` by its `-- name:` marker. One-liners stay inline |
| `llm_client.py` | — | The real LLM tier (step 8) — calls the sibling repo's LiteLLM gateway. Passes `metadata.trace_id=doc_id` on every call so tier/repair attempts land on one Langfuse trace, and `push_gate_scores()` attaches this repo's own gate outcomes to that trace afterward. |

Every `python -m docpipeline.stages.<name>` invocation (`k8s/values.yaml`'s `services:` list,
`local_scripts/run_local.py`'s `SERVICES`) uses the numbered module name directly; every in-repo import
uses `from docpipeline.stages import triage_1 as triage` (etc.), so calling code's own body never
has to change, just the one import line.
