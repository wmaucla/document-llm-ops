#!/usr/bin/env python3
"""Prints a final report: every document's terminal state plus
posted_documents count — what `make e2e` shows at the end, and what to eyeball
against the README's "What's verified" table after a fixture run."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from docpipeline.core import ledger


def main() -> None:
    conn = ledger.connect(role="rw")  # posted_documents isn't in pipeline_ro's grants
    with conn.cursor() as cur:
        cur.execute(
            "SELECT doc_id, state, doc_type, last_error FROM documents ORDER BY created_at"
        )
        rows = cur.fetchall()
        cur.execute("SELECT count(*) AS n FROM posted_documents")
        posted = cur.fetchone()["n"]

    by_state: dict[str, int] = {}
    for row in rows:
        by_state[row["state"]] = by_state.get(row["state"], 0) + 1
        print(f"{row['doc_id']:22s} {row['state']:10s} {row['doc_type']:12s} {row['last_error'] or ''}")

    print()
    print(f"total: {len(rows)}  " + "  ".join(f"{k}={v}" for k, v in sorted(by_state.items())))
    print(f"posted_documents: {posted}")


if __name__ == "__main__":
    main()
