"""The error contract every extraction backend raises.

Lives in its own module because both backends raise it and neither should
depend on the other. `llm_client` used to import it from the deterministic
backend, which meant the production path took a dependency on its stand-in --
backwards, and the main reason that backend looked like test scaffolding when
it is a supported runtime mode.

`extraction_4.run_funnel` branches on `kind`, so the two backends are
interchangeable from the funnel's point of view: refusal and context_overflow
are terminal for a tier, transient retries in place, unparseable is a repair
rung.
"""

from __future__ import annotations


class ExtractionError(Exception):
    """kind: context_overflow | refusal | transient | unparseable"""

    def __init__(self, kind: str, message: str = ""):
        super().__init__(message or kind)
        self.kind = kind
