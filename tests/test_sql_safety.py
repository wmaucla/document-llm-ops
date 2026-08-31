"""Structural guard: no SQL in this package may be built by string formatting.

The genuinely attacker-influenced values here are the *model's own output* --
`vendor` and `invoice_no` come out of the LLM, so a malicious document can try
to smuggle SQL through them. They are safe today only because every statement
is a literal with bound parameters. That is a property of how the code happens
to be written, which is exactly the kind of property that decays silently, so
this asserts it on every run rather than trusting review to catch it.

Two interpolations are allowed by name, both audited:

- `ledger.increment_attempts` interpolates a column *name*, which cannot be a
  bound parameter. Guarded by an allowlist and an explicit raise (not an
  assert -- `python -O` strips those, taking the guard with it).
- `operator.bulk_redrive` takes a raw WHERE clause. Operator tooling, invoked
  by a human who already has database access; never a user-facing path.

Adding a third exception should require justifying it here.
"""

from __future__ import annotations

import ast
import pathlib

PACKAGE = pathlib.Path(__file__).resolve().parent.parent / "docpipeline"

# (module path suffix, function name) -> why it is allowed to interpolate.
ALLOWED = {
    ("core/ledger.py", "increment_attempts"):
        "column name, allowlisted; identifiers cannot be bound parameters",
    ("reconciliation/operator.py", "bulk_redrive"):
        "raw WHERE clause; break-glass operator tooling, not user-facing",
}

_FORMATTED = (ast.JoinedStr,)  # f"..."


def _enclosing_function(tree: ast.AST, node: ast.AST) -> str | None:
    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for child in ast.walk(fn):
                if child is node:
                    return fn.name
    return None


def _is_formatted(arg: ast.AST) -> str | None:
    """Describe how this SQL argument was built, or None if it's a literal."""
    if isinstance(arg, _FORMATTED):
        return "f-string"
    if isinstance(arg, ast.BinOp):
        if isinstance(arg.op, ast.Mod):
            return "%-formatting"
        if isinstance(arg.op, ast.Add):
            return "concatenation"
    if isinstance(arg, ast.Call) and getattr(arg.func, "attr", "") == "format":
        return ".format()"
    return None


def _sql_call_sites():
    for path in sorted(PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if getattr(node.func, "attr", "") not in ("execute", "executemany"):
                continue
            if not node.args:
                continue
            yield path, tree, node, node.args[0]


def test_no_sql_is_built_by_string_formatting():
    violations = []
    for path, tree, node, arg in _sql_call_sites():
        how = _is_formatted(arg)
        if how is None:
            continue
        rel = path.relative_to(PACKAGE.parent).as_posix().removeprefix("docpipeline/")
        fn = _enclosing_function(tree, node)
        if (rel, fn) in ALLOWED:
            continue
        violations.append(f"{rel}:{node.lineno} in {fn}() builds SQL by {how}")

    assert not violations, (
        "SQL built by string formatting:\n  " + "\n  ".join(violations)
        + "\n\nBind parameters instead. If an *identifier* genuinely cannot be "
          "bound, allowlist it in ALLOWED above with a justification."
    )


def test_every_allowed_interpolation_still_exists():
    """Keeps the allowlist honest. If one of these is refactored away, the
    entry should go too -- a stale exemption is a hole waiting to be reused."""
    for (rel, fn) in ALLOWED:
        tree = ast.parse((PACKAGE / rel).read_text())
        names = {n.name for n in ast.walk(tree)
                 if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
        assert fn in names, f"allowlisted {rel}:{fn}() no longer exists — drop the exemption"


def test_document_derived_values_are_never_interpolated():
    """The specific attack this guards: vendor and invoice_no are produced by
    the LLM from document text, so they are the one place a malicious PDF could
    reach. They must appear only as bound parameters, never inside SQL text."""
    offenders = []
    for path, _tree, node, arg in _sql_call_sites():
        if not isinstance(arg, _FORMATTED):
            continue
        rendered = ast.dump(arg)
        for field in ("vendor", "invoice_no", "extraction_result", "last_error"):
            if f"'{field}'" in rendered or f'id="{field}"' in rendered:
                offenders.append(f"{path.name}:{node.lineno} interpolates {field}")
    assert not offenders, "document-derived values inside SQL text: " + "; ".join(offenders)


# ── keeping the SQL where it can be read as SQL ──────────────────────────────

SQL_DIR = PACKAGE / "core" / "sql"
_SQL_VERBS = ("SELECT ", "INSERT ", "UPDATE ", "DELETE ", "WITH ")
# A verb alone is not enough: a docstring starting "Delete the ollama pod(s)"
# trips it. Require a clause keyword too, so it has to actually look like SQL.
_SQL_CLAUSES = (" FROM ", " INTO ", " SET ", " WHERE ", " VALUES ")


def _looks_like_sql(text: str) -> bool:
    upper = text.upper()
    return (any(upper.startswith(v) for v in _SQL_VERBS)
            and any(c in upper for c in _SQL_CLAUSES))


def test_multi_line_sql_lives_in_sql_files():
    """The split is by size, not dogma: a one-liner reads fine at its call site,
    a multi-statement query does not. Without this the boundary erodes -- the
    next big query gets written inline 'just this once' and sql/ stops being the
    place to read the SQL."""
    offenders = []
    for path in sorted(PACKAGE.rglob("*.py")):
        if path.name == "queries.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            text = node.value.strip()
            if "\n" not in text:
                continue  # one-liners are allowed inline
            if _looks_like_sql(text):
                rel = path.relative_to(PACKAGE.parent).as_posix()
                offenders.append(f"{rel}:{node.lineno}")
    assert not offenders, (
        "multi-line SQL outside docpipeline/core/sql/: " + ", ".join(offenders)
        + "\n\nMove it to a sql/*.sql file under a `-- name:` marker, grouped with "
          "the statements it has to stay consistent with."
    )


def test_every_named_statement_loads_and_is_parameterised():
    """A renamed marker or a missing file must fail loudly. It is the same trap
    as AGENT.md bug #9 -- a file present in one container and absent in another,
    degrading silently -- so the loader binds eagerly and this asserts it."""
    from docpipeline.core import queries

    names = [n for n in dir(queries) if n.isupper() and not n.startswith("_")]
    assert names, "no SQL statements loaded from sql/"
    for name in names:
        stmt = getattr(queries, name)
        assert stmt.strip(), f"{name} loaded empty"
        assert "{" not in stmt, f"{name} contains a format brace — SQL must never be formatted"


def test_sql_files_use_bound_parameters_only():
    """Nothing in sql/ may interpolate. They are read verbatim, so a format
    placeholder there would be a hole the AST check over .py files cannot see."""
    bad = []
    for path in sorted(SQL_DIR.glob("*.sql")):
        text = path.read_text()
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("--"):
                continue  # prose, not SQL
            if "{" in stripped or "}" in stripped:
                bad.append(f"{path.name}: {stripped[:60]}")
    assert not bad, "format braces in .sql: " + "; ".join(bad)
