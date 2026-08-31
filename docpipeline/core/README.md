# docpipeline/core/

The ledger machinery every stage depends on — all raw SQL, no ORM. See [AGENT.md](../../AGENT.md)'s
"The state machine and the scatter-gather join" and "The five quality gates" sections for the why
behind the design; this file is just what's here.

| File | What it is |
|---|---|
| `ledger.py` | The document ledger — state machine (`transition()`, `ALLOWED_TRANSITIONS`), the scatter-gather join (`record_shard_and_maybe_join()`), first-writer-wins commit (`commit_extraction_result()`), feature flags |
| `outbox.py` | The transactional outbox + the polling relay (`SELECT ... FOR UPDATE SKIP LOCKED`, publish, mark posted) |
| `gates.py` | The five deterministic quality gates: `grounding`, `arithmetic`, `iban_mod97`, `plausibility`, `business_dedupe` |
| `models.py` | Tier 1 of the extraction funnel — the schema gate (pydantic validation of the model's raw JSON) |
| `artifact.py` | Deterministic GCS paths + read/write helpers for the canonical text-production artifact and OCR shard output |

**Load-bearing invariant:** every write in here is a guarded SQL statement (`UPDATE ... WHERE
state = ANY(allowed) RETURNING state`), not application-level check-then-write — the row lock and
the `RETURNING` clause *are* the concurrency control. Don't refactor `record_shard_and_maybe_join()`
into two separate queries; the whole point is that the increment and the comparison happen under
one lock.
