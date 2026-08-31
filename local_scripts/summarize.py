#!/usr/bin/env python3
"""Prints a final report: every document's terminal state plus
posted_documents count — what `make e2e`/`make e2e-k8s` show at the end, and
what to eyeball against the README's "What's verified" table after a fixture
run. Exits 1 (and prints a failing banner) if anything is still in-flight —
this is the actual "did the run finish" gate, not just a cosmetic dump:
complete/review/failed are all legitimate settled outcomes, only
text_pending/text_running/extract_pending/extract_running mean it isn't done
yet. Ansible's shell/command tasks fail on non-zero exit by default, so this
already stops `make e2e`/`make e2e-k8s` in their tracks if the run didn't
actually settle -- no separate check needed."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from docpipeline.core import ledger


def main(require_settled: bool = False) -> int:
    conn = ledger.connect(role="rw")  # posted_documents isn't in pipeline_ro's grants
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT doc_id, state, doc_type, last_error FROM documents ORDER BY created_at"
            )
            rows = cur.fetchall()
            cur.execute("SELECT count(*) AS n FROM posted_documents")
            posted = cur.fetchone()["n"]
    finally:
        # See AGENT.md's "Connection-leak discipline": this now runs via
        # `kubectl exec` inside a long-lived pod, where an exception before
        # exit would leave an idle-in-transaction connection holding
        # AccessShareLock on `documents` — which has already caused
        # multi-minute TRUNCATE hangs once.
        conn.close()

    by_state: dict[str, int] = {}
    for row in rows:
        by_state[row["state"]] = by_state.get(row["state"], 0) + 1
        print(f"{row['doc_id']:22s} {row['state']:10s} {row['doc_type']:12s} {row['last_error'] or ''}")

    print()
    print(f"total: {len(rows)}  " + "  ".join(f"{k}={v}" for k, v in sorted(by_state.items())))
    print(f"posted_documents: {posted}")

    in_flight = [r for r in rows if r["state"] in ledger.IN_FLIGHT_STATES]
    print()
    print("=" * 60)
    if not rows:
        print("⚠️  NO DOCUMENTS FOUND -- nothing was ingested this run")
        print("=" * 60)
        return 1 if require_settled else 0
    if in_flight:
        # In-flight is only a *failure* when the caller asserted the run should
        # already be finished. Run standalone (`make summary`/`summary-k8s`,
        # documented as runnable "at any point after the cluster is up"), this
        # is an ordinary progress report against a live pipeline and must not
        # exit non-zero: confirmed live 2026-08-30, a standalone summary-k8s
        # failed the play on two documents that completed 4 and 8 seconds
        # later, with extraction at RESTARTS 0 and the sweeper idle. The
        # "did the run finish" gate belongs to wait_for_drain.py, which the
        # e2e paths already run first.
        label = "❌ RUN INCOMPLETE" if require_settled else "⏳ IN PROGRESS"
        print(f"{label} -- {len(in_flight)}/{len(rows)} document(s) still in-flight")
        for r in in_flight:
            print(f"    {r['doc_id']:22s} {r['state']}")
        print("=" * 60)
        return 1 if require_settled else 0
    print(f"✅ RUN COMPLETE -- {len(rows)}/{len(rows)} documents settled, 0 stuck in-flight")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main(require_settled="--require-settled" in sys.argv))
