# docpipeline/infra/

Thin wrappers over the two external systems — no business logic lives here, just the storage and
messaging primitives every other package builds on.

| File | What it is |
|---|---|
| `gcs.py` | The storage wrapper — talks to fake-gcs-server locally and real GCS in prod through the same code path, no local/prod branch |
| `kafka_utils.py` | Producer/consumer/topic-admin helpers (`make_consumer`, `poll_json`, `ensure_topics`) |
