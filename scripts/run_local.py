#!/usr/bin/env python3
"""Runs every pipeline service as a subprocess — the local stand-in for the
'same topology' Tilt setup the design doc describes, without needing
minikube/Tilt/KEDA (see this repo's README for that deviation).

Ctrl-C stops everything cleanly.
"""
from __future__ import annotations

import signal
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = REPO_ROOT / "logs"

SERVICES = [
    "docpipeline.core.outbox",
    "docpipeline.reconciliation.orphan_detector",
    "docpipeline.reconciliation.sweeper",
    "docpipeline.stages.triage",
    "docpipeline.stages.pdf_worker",
    "docpipeline.stages.ocr_shard",
    "docpipeline.stages.extraction",
    "docpipeline.stages.sink_stub",
]


def main() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    procs: list[subprocess.Popen] = []
    log_files = []

    def shutdown(*_args) -> None:
        print("\nshutting down...")
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        for f in log_files:
            f.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for module in SERVICES:
        log_path = LOG_DIR / f"{module.rsplit('.', 1)[-1]}.log"
        log_file = open(log_path, "a")
        log_files.append(log_file)
        print(f"starting {module} (log: {log_path})")
        # stdout/stderr/stdin explicitly redirected away from whatever this
        # process inherited — otherwise a caller that backgrounds run_local.py
        # under a pipe (e.g. `make e2e | tail`) never sees EOF: these 8 child
        # processes keep the pipe's write end open long after the parent make
        # invocation itself has exited.
        procs.append(subprocess.Popen(
            [sys.executable, "-m", module], cwd=REPO_ROOT,
            stdout=log_file, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        ))
        time.sleep(0.3)

    print(f"\n{len(procs)} services running. Ctrl-C to stop.\n")
    while True:
        for p in procs:
            if p.poll() is not None:
                print(f"!! {p.args} exited with code {p.returncode} — shutting down the rest")
                shutdown()
        time.sleep(2)


if __name__ == "__main__":
    main()
