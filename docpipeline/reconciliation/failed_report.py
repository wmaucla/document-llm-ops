"""Periodic report on documents sitting in `failed`.

`failed` is terminal in practice: the sweeper puts documents there when they
exhaust their attempt cap, and nothing takes them out again except `dlq_replay`
(and only when build_sha/prompt_version has moved) or an operator break-glass
re-drive. So a growing `failed` population is silent by construction -- no
consumer is retrying it, no alert fires, and the pipeline looks healthy because
throughput is unaffected.

This is deliberately just a summary: counts, age, and the most common
`last_error` prefixes, so a human can see the shape of what is stuck. In
practice a real deployment would want more triage than this -- grouping by
failure class, correlating against a deploy or model version, routing
per-vendor failures to whoever owns that integration, opening tickets, or
auto-re-driving categories known to be transient. None of that is here on
purpose; the point is the cadence and the visibility, not the triage logic.

Run by the `docpipeline-failed-report` CronJob (k8s/templates/cronjobs.yaml).
Read-only -- connects as pipeline_ro and cannot modify anything.
"""

from __future__ import annotations

import argparse
import logging

from docpipeline import config
from docpipeline.core import ledger

log = logging.getLogger(__name__)

# Enough to see a pattern without dumping the whole table into a log line.
TOP_ERRORS = 5
ERROR_PREFIX_CHARS = 80


def collect(cur) -> dict:
    cur.execute("SELECT count(*) AS n FROM documents WHERE state = 'failed'")
    total = cur.fetchone()["n"]

    cur.execute(
        """
        SELECT count(*) AS n
          FROM documents
         WHERE state = 'failed' AND state_updated_at > now() - interval '24 hours'
        """
    )
    last_24h = cur.fetchone()["n"]

    cur.execute(
        """
        SELECT min(state_updated_at) AS oldest, max(state_updated_at) AS newest
          FROM documents WHERE state = 'failed'
        """
    )
    span = cur.fetchone()

    # Grouped on a prefix rather than the whole string: last_error carries
    # attempt counts and doc-specific detail, so the full text is near-unique
    # and would group into buckets of one.
    cur.execute(
        """
        SELECT left(coalesce(last_error, '(none)'), %s) AS reason, count(*) AS n
          FROM documents
         WHERE state = 'failed'
         GROUP BY reason
         ORDER BY n DESC
         LIMIT %s
        """,
        (ERROR_PREFIX_CHARS, TOP_ERRORS),
    )
    reasons = cur.fetchall()

    # A document whose build_sha/prompt_version already matches current is one
    # dlq_replay will NOT pick up -- it only re-drives what changed since. These
    # are the ones genuinely needing a human or a new deploy.
    cur.execute(
        """
        SELECT count(*) AS n FROM documents
         WHERE state = 'failed'
           AND build_sha IS NOT DISTINCT FROM %s
           AND prompt_version IS NOT DISTINCT FROM %s
        """,
        (config.BUILD_SHA, config.PROMPT_VERSION),
    )
    not_replayable = cur.fetchone()["n"]

    return {
        "failed_total": total,
        "failed_last_24h": last_24h,
        "oldest": span["oldest"],
        "newest": span["newest"],
        "top_reasons": [(r["reason"], r["n"]) for r in reasons],
        "not_replayable_by_dlq": not_replayable,
    }


def render(report: dict) -> str:
    lines = [
        "=" * 60,
        "FAILED-STATE REPORT",
        "=" * 60,
        f"failed (total):      {report['failed_total']}",
        f"failed (last 24h):   {report['failed_last_24h']}",
    ]
    if report["failed_total"]:
        lines += [
            f"oldest:              {report['oldest']}",
            f"newest:              {report['newest']}",
            f"not replayable:      {report['not_replayable_by_dlq']} "
            f"(build_sha/prompt_version unchanged -- dlq-replay will skip these)",
            "",
            "top reasons:",
        ]
        lines += [f"  {n:>5}  {reason}" for reason, n in report["top_reasons"]]
    else:
        lines.append("nothing in failed — no action needed")
    lines.append("=" * 60)
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--fail-on-any", action="store_true",
                    help="exit 1 if anything is in failed (for wiring to an alert; "
                         "off by default so the report is a report, not a gate)")
    args = ap.parse_args()

    conn = ledger.connect(role="ro")
    try:
        with conn.cursor() as cur:
            report = collect(cur)
    finally:
        conn.close()  # see AGENT.md "Connection-leak discipline"

    print(render(report))
    return 1 if (args.fail_on_any and report["failed_total"]) else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    raise SystemExit(main())
