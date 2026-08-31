"""Multi-line SQL for the pipeline stages, loaded from `sql/`.

Same split and loader as `docpipeline/core/queries.py`: statements big enough to
want reading as SQL live in files, one-liners stay inline.
"""

from __future__ import annotations

from docpipeline.core.queries import load_sql

_SQL = load_sql(__package__)

POST_DOCUMENT = _SQL["POST_DOCUMENT"]
