"""Structural guards on how SQL is written, checked by AST rather than review.

`vendor` and `invoice_no` come out of the LLM, so a malicious document is the
one input that can reach them; they are safe only because every statement is a
literal with bound parameters, which is the kind of property that decays
silently between reviews. Two interpolations are allowlisted below with
justifications — adding a third should require one too.
"""

from __future__ import annotations

import ast
import pathlib

PACKAGE = pathlib.Path(__file__).resolve().parent.parent / "docpipeline"

# (module, function) -> why it may interpolate.
ALLOWED = {
    ("core/ledger.py", "increment_attempts"): "column name; identifiers cannot be bound",
    ("reconciliation/operator.py", "bulk_redrive"): "raw WHERE clause; break-glass tooling",
}

# Values the model produces from document text — the ones an attacker can reach.
DOCUMENT_DERIVED = ("vendor", "invoice_no", "extraction_result", "last_error")

_VERBS = ("SELECT ", "INSERT ", "UPDATE ", "DELETE ", "WITH ")
# A verb alone is not enough — a docstring starting "Delete the ollama pod(s)"
# trips it — so require a clause keyword too.
_CLAUSES = (" FROM ", " INTO ", " SET ", " WHERE ", " VALUES ")


def _looks_like_sql(text: str) -> bool:
    upper = text.upper()
    return any(upper.startswith(v) for v in _VERBS) and any(c in upper for c in _CLAUSES)


def _how_built(arg: ast.AST) -> str | None:
    """How this SQL argument was constructed, or None if it is a literal."""
    if isinstance(arg, ast.JoinedStr):
        return "f-string"
    if isinstance(arg, ast.BinOp):
        return {ast.Mod: "%-formatting", ast.Add: "concatenation"}.get(type(arg.op))
    if isinstance(arg, ast.Call) and getattr(arg.func, "attr", "") == "format":
        return ".format()"
    return None


def _execute_calls():
    """Yield (module, function, node, sql_arg) for every execute() call."""
    for path in sorted(PACKAGE.rglob("*.py")):
        rel = path.relative_to(PACKAGE).as_posix()
        for fn in ast.walk(ast.parse(path.read_text())):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(fn):
                if (isinstance(node, ast.Call)
                        and getattr(node.func, "attr", "") in ("execute", "executemany")
                        and node.args):
                    yield rel, fn.name, node, node.args[0]


def test_no_sql_is_built_by_string_formatting():
    bad = [f"{rel}:{node.lineno} in {fn}() builds SQL by {how}"
           for rel, fn, node, arg in _execute_calls()
           if (how := _how_built(arg)) and (rel, fn) not in ALLOWED]
    assert not bad, "\n  ".join(["Bind parameters instead:", *bad])


def test_allowlisted_interpolations_stay_honest():
    """An exemption must still exist, and must never reach a document-derived
    value — those are the fields a malicious PDF can actually influence."""
    for rel, fn in ALLOWED:
        names = {n.name for n in ast.walk(ast.parse((PACKAGE / rel).read_text()))
                 if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
        assert fn in names, f"{rel}:{fn}() is gone — drop the exemption"

    leaked = [f"{rel}:{node.lineno} interpolates {field}"
              for rel, fn, node, arg in _execute_calls()
              if isinstance(arg, ast.JoinedStr)
              for field in DOCUMENT_DERIVED
              if f"'{field}'" in ast.dump(arg)]
    assert not leaked, "document-derived values inside SQL: " + "; ".join(leaked)


def test_multi_line_sql_lives_in_sql_files():
    """One-liners read fine at their call site; anything larger belongs in
    sql/*.sql under a `-- name:` marker, or the split silently erodes."""
    bad = []
    for path in sorted(PACKAGE.rglob("*.py")):
        if path.name == "queries.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                    and "\n" in node.value.strip() and _looks_like_sql(node.value.strip())):
                bad.append(f"{path.relative_to(PACKAGE).as_posix()}:{node.lineno}")
    assert not bad, "multi-line SQL outside sql/: " + ", ".join(bad)


def test_sql_files_load_and_contain_no_format_braces():
    """A renamed marker or missing file must fail loudly at import, not on the
    first query that needs it — the same trap as AGENT.md bug #9. Braces in a
    .sql file would be a hole the AST check over .py files cannot see."""
    from docpipeline.core import queries

    names = [n for n in dir(queries) if n.isupper() and not n.startswith("_")]
    assert names, "no SQL loaded from sql/"
    for name in names:
        stmt = getattr(queries, name)
        assert stmt.strip(), f"{name} loaded empty"
        assert "{" not in stmt, f"{name} contains a format brace"

    for path in sorted(PACKAGE.rglob("sql/*.sql")):
        for line in path.read_text().splitlines():
            if not line.strip().startswith("--"):
                assert "{" not in line, f"{path.name}: format brace in {line.strip()[:60]}"
