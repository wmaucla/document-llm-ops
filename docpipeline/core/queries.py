"""Loads the multi-statement SQL in `sql/`, keyed by its `-- name:` marker.

Only *multi-line* statements live in `sql/`. One-liners stay inline at their
call site, where an extra indirection would cost more than it explains — the
split is by whether the SQL is big enough to want reading as SQL, not by
dogma. `tests/test_sql_safety.py` enforces exactly that boundary.

Files are grouped by concept rather than by table, so a statement sits next to
the others it has to stay consistent with: both halves of the scatter-gather
join in one file, both post-gate writes in another. The reasoning that makes
each one correct is in the file with it, as `--` comments, rather than in the
Python that calls it.

Loading is eager and by name. A missing file or a renamed marker is an
ImportError at startup, not a surprise on the first query that needs it —
this repo has already been bitten once (AGENT.md bug #9) by a file that was
present in one container and absent in another, degrading silently instead of
crashing.
"""

from __future__ import annotations

import re
from importlib import resources

_NAME_RE = re.compile(r"^--\s*name:\s*([a-z_][a-z0-9_]*)\s*$", re.IGNORECASE)


def load_sql(package: str) -> dict[str, str]:
    """Parse a package's sql/*.sql into {NAME: statement}.

    `-- name: foo` opens a block; everything until the next marker belongs to
    it. Comment lines are kept — they are the explanation of why the statement
    is shaped the way it is, and psycopg ignores them.
    """
    loaded: dict[str, str] = {}
    root = resources.files(package).joinpath("sql")
    for entry in sorted(root.iterdir(), key=lambda p: p.name):
        if not entry.name.endswith(".sql"):
            continue
        current: str | None = None
        buf: list[str] = []
        for line in entry.read_text().splitlines():
            match = _NAME_RE.match(line.strip())
            if match:
                if current:
                    loaded[current] = "\n".join(buf).strip().rstrip(";")
                current, buf = match.group(1).upper(), []
            elif current is not None:
                buf.append(line)
        if current:
            loaded[current] = "\n".join(buf).strip().rstrip(";")
    return loaded


_SQL = load_sql(__package__)


def _require(name: str) -> str:
    if name not in _SQL:
        raise ImportError(
            f"SQL statement {name!r} not found in docpipeline/core/sql/. "
            f"Available: {sorted(_SQL)}"
        )
    return _SQL[name]


# Bound explicitly rather than injected into globals(): the names are then
# greppable, and a missing one fails at import with the list of what was found.
TRANSITION = _require("TRANSITION")
INSERT_INITIAL_DOCUMENT = _require("INSERT_INITIAL_DOCUMENT")
CLAIM_SHARD = _require("CLAIM_SHARD")
INCREMENT_SHARDS_DONE = _require("INCREMENT_SHARDS_DONE")
COMMIT_EXTRACTION_RESULT = _require("COMMIT_EXTRACTION_RESULT")
ROUTE_WITHOUT_WRITING = _require("ROUTE_WITHOUT_WRITING")
CLAIM_OUTBOX_BATCH = _require("CLAIM_OUTBOX_BATCH")
LOG_ATTEMPT = _require("LOG_ATTEMPT")
