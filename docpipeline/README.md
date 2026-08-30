# docpipeline/

The pipeline itself, mapped directly onto [the design doc](../../mlops-llm-repo)'s sections.

| Subpackage | What it is |
|---|---|
| [`core/`](core/README.md) | The ledger machinery every stage depends on — state machine, outbox, quality gates |
| [`infra/`](infra/README.md) | Thin wrappers over the two external systems (GCS, Kafka) — no business logic |
| [`text/`](text/README.md) | Text/OCR helpers shared across multiple stages — not a stage's own logic |
| [`stages/`](stages/README.md) | The pipeline proper — one module per stage |
| [`reconciliation/`](reconciliation/README.md) | Everything that keeps the system healthy, or fixes it by hand |

Two files stay top-level, not subpackaged:

- `config.py` — cross-cutting settings, read by every other module (subpackaging it would just
  add an import hop with no grouping benefit)
- `fixture_content.py` — shared invoice line-item text used by both `fixtures/generate_fixtures.py`
  and several tests, not pipeline logic

See [AGENT.md](../AGENT.md) for the mechanics and gotchas that aren't obvious from reading the
code once — the state machine's legal-move table, why the scatter-gather join has to be one
locked `UPDATE`, the connection-leak discipline, and every bug that was found live and fixed.
