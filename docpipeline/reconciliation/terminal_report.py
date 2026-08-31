"""Periodic report on documents parked in `failed` and `review`.

Both are terminal in practice, and both grow silently -- no consumer retries
them, no alert fires, and throughput is unaffected, so the pipeline looks
healthy while work piles up.

`review` is the bigger blind spot of the two. `failed` at least has
`dlq_replay`, which re-drives it whenever build_sha/prompt_version moves.
Nothing automatic touches `review` at all: dlq_replay skips it entirely, the
sweeper only claims in-flight states, and the sole way out is a deliberate
operator action (`force_redrive` to re-extract, or `accept_review` to override
the gate decision). A document can sit there indefinitely.

This is deliberately just a summary: counts, age, and the most common
`last_error` prefixes, so a human can see the shape of what is stuck. In
practice a real deployment would want more triage than this -- grouping by
failure class, correlating against a deploy or model version, routing
per-vendor failures to whoever owns that integration, opening tickets, or
auto-re-driving categories known to be transient. None of that is here on
purpose; the point is the cadence and the visibility, not the triage logic.

Run by the `docpipeline-terminal-report` CronJob (k8s/templates/cronjobs.yaml).
Read-only -- connects as pipeline_ro and cannot modify anything.
"""

from __future__ import annotations

import argparse
import logging

from docpipeline import config
from docpipeline.core import ledger
from docpipeline.reconciliation import queries

log = logging.getLogger(__name__)

# Enough to see a pattern without dumping the whole table into a log line.
TOP_ERRORS = 5
ERROR_PREFIX_CHARS = 80


def collect_state(cur, state: str) -> dict:
    cur.execute("SELECT count(*) AS n FROM documents WHERE state = %s", (state,))
    total = cur.fetchone()["n"]

    cur.execute(
        "SELECT count(*) AS n FROM documents "
        "WHERE state = %s AND state_updated_at > now() - interval '24 hours'",
        (state,),
    )
    last_24h = cur.fetchone()["n"]

    cur.execute(
        "SELECT min(state_updated_at) AS oldest, max(state_updated_at) AS newest "
        "FROM documents WHERE state = %s",
        (state,),
    )
    span = cur.fetchone()

    # Grouped on a prefix rather than the whole string: last_error carries
    # attempt counts and doc-specific detail, so the full text is near-unique
    # and would group into buckets of one.
    cur.execute(
        queries.TOP_REASONS,
        (ERROR_PREFIX_CHARS, state, TOP_ERRORS),
    )
    reasons = cur.fetchall()

    # A document whose build_sha/prompt_version already matches current is one
    # dlq_replay will NOT pick up -- it only re-drives what changed since. These
    # are the ones genuinely needing a human or a new deploy.
    # Only meaningful for `failed`: dlq_replay re-drives failed documents whose
    # build_sha/prompt_version moved. A `review` document is never picked up by
    # it at all, at any version, so the count would be misleadingly reassuring.
    not_replayable = None
    if state == "failed":
        cur.execute(
            queries.COUNT_NOT_REPLAYABLE,
            (config.BUILD_SHA, config.PROMPT_VERSION),
        )
        not_replayable = cur.fetchone()["n"]

    return {
        "state": state,
        "total": total,
        "last_24h": last_24h,
        "oldest": span["oldest"],
        "newest": span["newest"],
        "top_reasons": [(r["reason"], r["n"]) for r in reasons],
        "not_replayable_by_dlq": not_replayable,
    }


def collect(cur) -> dict:
    return {s: collect_state(cur, s) for s in ("failed", "review")}


def render(report: dict) -> str:
    lines = ["=" * 64, "TERMINAL-STATE REPORT", "=" * 64]
    for state in ("failed", "review"):
        r = report[state]
        lines += [f"{state.upper()}", f"  total:        {r['total']}", f"  last 24h:     {r['last_24h']}"]
        if not r["total"]:
            lines.append("  (nothing parked here)")
        else:
            lines += [f"  oldest:       {r['oldest']}", f"  newest:       {r['newest']}"]
            if r["not_replayable_by_dlq"] is not None:
                lines.append(
                    f"  stuck-for-good: {r['not_replayable_by_dlq']} "
                    "(build_sha/prompt_version unchanged — dlq-replay will skip these)"
                )
            else:
                # Said plainly rather than left as an absence: `review` having
                # no automatic handler is the point of reporting on it.
                lines.append("  stuck-for-good: all of them — nothing automatic ever re-drives "
                             "`review`; needs force_redrive or accept_review")
            lines.append("  top reasons:")
            lines += [f"    {n:>5}  {reason}" for reason, n in r["top_reasons"]]
        lines.append("")
    lines.append("=" * 64)
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fail-on-any", action="store_true",
                    help="exit 1 if anything is parked in failed or review (for wiring to "
                         "an alert; off by default so the report is a report, not a gate)")
    args = ap.parse_args()

    conn = ledger.connect(role="ro")
    try:
        with conn.cursor() as cur:
            report = collect(cur)
    finally:
        conn.close()  # see AGENT.md "Connection-leak discipline"

    print(render(report))
    parked = report["failed"]["total"] + report["review"]["total"]
    return 1 if (args.fail_on_any and parked) else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    raise SystemExit(main())
