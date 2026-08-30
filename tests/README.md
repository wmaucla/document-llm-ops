# tests/

`pytest tests/ -v` — mock mode, real Postgres, real threads for the concurrency cases, ~4s for
59 tests. `conftest.py`'s session-scoped fixture forces mock mode regardless of the ambient shell
environment, so the suite stays hermetic even when `EXTRACTION_MODE=real` is set (e.g. after
sourcing `.env` for something else).

One test per real hazard, not per function:

- `test_state_machine.py` — illegal transitions rejected, `_running` only enterable from its own `_pending`
- `test_scatter_gather_join.py` — the join fires exactly once under **concurrent** final shards (real threads, not sequential calls — a sequential pair can't exercise the actual lock contention)
- `test_outbox_relay.py` — two relay replicas racing for the same row publish it exactly once
- `test_sweeper.py` — re-drives lost work, republishes only missing shards, DLQs past the attempt cap
- `test_read_only_role.py` — `pipeline_ro`'s inability to `INSERT` into `outbox` is a real tested grant, not an assumption about code paths
- `test_operator.py` — break-glass requires a reason, enforces the blast-radius cap, force-redrive refuses a doc that failed before triage ever set `page_count`
- `test_dlq_replay.py`, `test_deadmans_switch.py`, `test_canary.py` — each reconciler
- `test_gates.py`, `test_extraction_funnel.py`, `test_business_dedupe.py` — the five quality gates, via a programmable mock model (`injected_total`, `swapped_roles`, `refusal`, `context_overflow` scenarios)
- `test_real_llm_integration.py` — **opt-in, not in the default run** (`RUN_REAL_LLM_TESTS=1`) — talks to the real LiteLLM gateway, one call took ~170s under load in verification

See `presentations/llmops-document-pipeline-workflow.html`'s "Proven end-to-end" section for the
fixture table showing what the *host-mode `make e2e` run* proves end to end — this directory is
the unit/integration layer underneath it, not a replacement for it.
