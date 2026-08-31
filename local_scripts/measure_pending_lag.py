"""Settles AGENT.md bug #3's open question: when documents pile up in
`extract_pending`, are their `ocr.completed` messages *lost*, or merely *queued*
behind slow inference?

The sweeper can't tell those apart from the `documents` table alone, which is why
one threshold currently covers both `extract_pending` and `extract_running`. The
deciding signal is consumer-group lag on `ocr.completed`, sampled at the same
moment as the document counts:

    lag > 0  while documents sit pending  ->  QUEUED. 1500s is correct; do not
                                              shorten the threshold, doing so
                                              re-creates bug #3 on _pending.
    lag ~ 0  while documents sit pending  ->  LOST. The split is worth building.

Run this from the host against a live cluster, under a backlog big enough that
documents actually queue -- see AGENT.md bug #1's forced-CPU reproduction recipe,
then `make replay-docs COUNT=10`. Reads only; safe to run any time.

    python local_scripts/measure_pending_lag.py --interval 15 --duration 1800
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import time

GROUP = "extraction"
TOPIC = "ocr.completed"


def _kubectl(args: list[str], timeout: int = 30) -> str:
    """Run a kubectl command, returning stdout. Empty string on any failure --
    a transient API blip must not kill a long sampling run."""
    try:
        out = subprocess.run(
            ["kubectl", *args], capture_output=True, text=True, timeout=timeout
        )
        return out.stdout if out.returncode == 0 else ""
    except (subprocess.TimeoutExpired, OSError):
        return ""


def sample_lag() -> tuple[int | None, str, int | None]:
    """(total_lag, group_state, members) from `rpk group describe`.

    total_lag is None when the group is mid-rebalance or unreadable -- during a
    rebalance rpk reports 0, which is indistinguishable from a genuinely drained
    topic and would produce a false LOST verdict. Better to record a gap.
    """
    raw = _kubectl(["exec", "deploy/redpanda", "--", "rpk", "group", "describe", GROUP])
    if not raw:
        return None, "unreachable", None

    def field(name: str) -> str | None:
        m = re.search(rf"^{name}\s+(.+?)\s*$", raw, re.MULTILINE)
        return m.group(1).strip() if m else None

    state = field("STATE") or "unknown"
    members = field("MEMBERS")
    lag = field("TOTAL-LAG")
    if state != "Stable":
        return None, state, int(members) if members and members.isdigit() else None
    return (
        int(lag) if lag and lag.lstrip("-").isdigit() else None,
        state,
        int(members) if members and members.isdigit() else None,
    )


def sample_states() -> dict[str, int]:
    """Per-state document counts, straight from the ledger."""
    raw = _kubectl([
        "exec", "deploy/docpipeline-triage", "--",
        "python", "-c",
        "from docpipeline.core import ledger\n"
        "c = ledger.connect()\n"
        "try:\n"
        "    cur = c.cursor()\n"
        "    cur.execute('select state, count(*) n from documents group by state')\n"
        "    print(';'.join(f\"{r['state']}={r['n']}\" for r in cur.fetchall()))\n"
        "finally:\n"
        "    c.close()\n",
    ])
    counts: dict[str, int] = {}
    for part in raw.strip().split(";"):
        if "=" in part:
            k, _, v = part.partition("=")
            if v.strip().isdigit():
                counts[k.strip()] = int(v.strip())
    return counts


def verdict(pending: int, lag: int | None, state: str) -> str:
    if lag is None:
        return f"no-reading({state})"
    if pending == 0:
        return "idle"
    if lag > 0:
        return "QUEUED"
    return "LOST?"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--interval", type=int, default=15, help="seconds between samples")
    ap.add_argument("--duration", type=int, default=1800, help="total seconds to sample")
    ap.add_argument("--csv", default="logs/pending_lag.csv", help="where to write samples")
    args = ap.parse_args()

    deadline = time.time() + args.duration
    rows: list[dict] = []
    print(f"{'time':8} {'pending':>7} {'running':>7} {'lag':>6} {'members':>7}  state/verdict")

    try:
        while time.time() < deadline:
            ts = time.strftime("%H:%M:%S")
            counts = sample_states()
            lag, gstate, members = sample_lag()
            pending = counts.get("extract_pending", 0)
            running = counts.get("extract_running", 0)
            v = verdict(pending, lag, gstate)
            print(
                f"{ts:8} {pending:>7} {running:>7} "
                f"{('-' if lag is None else lag):>6} {('-' if members is None else members):>7}  {gstate}/{v}"
            )
            rows.append({
                "time": ts, "extract_pending": pending, "extract_running": running,
                "total_lag": "" if lag is None else lag, "group_state": gstate,
                "members": "" if members is None else members, "verdict": v,
            })
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\ninterrupted -- writing what was collected", file=sys.stderr)

    if rows:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
        print(f"\n{len(rows)} samples -> {args.csv}")

    # Only samples where documents were actually waiting can decide anything.
    decisive = [r for r in rows if r["extract_pending"] > 0 and r["verdict"] in ("QUEUED", "LOST?")]
    if not decisive:
        print("INCONCLUSIVE: never caught documents pending with a readable lag. "
              "Use a bigger backlog or slower inference.")
        return 1
    queued = sum(1 for r in decisive if r["verdict"] == "QUEUED")
    lost = len(decisive) - queued
    print(f"\n{len(decisive)} decisive samples: QUEUED={queued} LOST?={lost}")
    if lost == 0:
        print("=> QUEUED. Documents were waiting their turn, not stranded. Keep one "
              "threshold; shortening extract_pending would re-create bug #3.")
    elif queued == 0:
        print("=> LOST. Messages were gone while documents sat pending. Splitting "
              "the threshold by state is justified.")
    else:
        print("=> MIXED. Both happen; a time-based threshold cannot separate them. "
              "Gate redrive on lag itself rather than on elapsed time.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
