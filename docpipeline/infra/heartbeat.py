"""Liveness heartbeat -- a plain touched file a Kubernetes exec livenessProbe
polls for freshness.

A consumer that hangs on something with no timeout raises no exception, so
run_forever()'s own try/except never sees it and never logs anything (see
AGENT.md's "Known open bugs" #1) -- Kubernetes has no other way to tell
"slow" from "wedged forever." touch() is cheap enough to call on every loop
iteration and after every bounded-but-slow external call.
"""

from __future__ import annotations

import os
import time

HEARTBEAT_FILE = os.environ.get("HEARTBEAT_FILE", "/tmp/heartbeat")


def touch() -> None:
    with open(HEARTBEAT_FILE, "w") as f:
        f.write(str(time.time()))
