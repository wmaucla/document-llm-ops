FROM python:3.13-slim

# Static uv binary, not pip-bootstrapped -- matches this repo's uv-only convention.
COPY --from=ghcr.io/astral-sh/uv:0.4.6 /uv /usr/local/bin/uv

# tesseract-ocr: opt-in via OCR_ENGINE=tesseract. postgresql-client: psql for
# the in-cluster migration Job (not just libpq5, which is the client library
# psycopg links against, not the psql binary).
RUN apt-get update && apt-get install --no-install-recommends -y \
    poppler-utils \
    tesseract-ocr \
    libpq5 \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
# --system: no venv, since every Deployment's command is a bare `python -m ...`.
RUN uv pip install --system --no-cache .

COPY docpipeline docpipeline/
COPY fixtures fixtures/
COPY migrations migrations/
COPY local_scripts local_scripts/

ENV PYTHONUNBUFFERED=1

# No ENTRYPOINT/CMD — each Deployment specifies its own `python -m
# docpipeline.<service>` command (see k8s/values.yaml's services: list).
