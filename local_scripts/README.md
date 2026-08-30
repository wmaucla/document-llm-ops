# local_scripts/

Utilities written for local, host-mode execution (`make e2e`'s `run_local.py`, `wait_for_drain.py`)
— named to make that intent explicit. Two of them (`create_topics.py`, `summarize.py`) also get
copied into the Docker image (`Dockerfile`'s `COPY local_scripts local_scripts/`) and reused
in-cluster via one-off Jobs or `kubectl exec`, since it's the same code either way — that's reuse
of convenience, not the reason this directory exists.

| File | What it does |
|---|---|
| `run_local.py` | Host-process orchestrator for `make e2e` — spawns all 8 consumers as subprocesses, `os.setpgrp()`s itself so the whole group can be killed at once (see [AGENT.md](../AGENT.md)'s PID-capture gotcha), own SIGTERM handler for clean `Ctrl-C` shutdown |
| `wait_for_drain.py` | Polls the ledger until every document reaches a terminal state or a timeout elapses — `make e2e` (host mode) only |
| `create_topics.py` | Creates every Kafka topic, idempotent |
| `summarize.py` | Prints the final per-document state report |
