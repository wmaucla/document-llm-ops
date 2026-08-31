"""Multi-line SQL for the reconciliation tier, loaded from `sql/`.

Same split as `docpipeline/core/queries.py` and the same loader: statements big
enough to want reading as SQL live in files grouped by concept, one-liners stay
inline at their call site.
"""

from __future__ import annotations

from docpipeline.core.queries import load_sql

_SQL = load_sql(__package__)

CLAIM_STUCK_BATCH = _SQL["CLAIM_STUCK_BATCH"]
FIND_REPLAYABLE = _SQL["FIND_REPLAYABLE"]
TOP_REASONS = _SQL["TOP_REASONS"]
COUNT_NOT_REPLAYABLE = _SQL["COUNT_NOT_REPLAYABLE"]
ALREADY_PENDING_TRIAGE = _SQL["ALREADY_PENDING_TRIAGE"]
