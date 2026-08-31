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

COPY pyproject.toml uv.lock ./
# --frozen installs exactly what uv.lock pins and never re-resolves: `uv pip
# install .` read only pyproject's constraints (>=3.1, >=0.27, ...), so an image
# rebuilt tomorrow could ship different versions than `uv run` uses on the host,
# which is how "works locally, fails in cluster" starts. --no-install-project
# keeps this layer cached until the lock changes; the source is copied below and
# imported from WORKDIR. --no-dev leaves pytest and friends out of the image.
RUN uv sync --frozen --no-dev --no-install-project

# Deployments run a bare `python -m ...`, so the venv has to be on PATH.
ENV PATH="/app/.venv/bin:$PATH"

COPY docpipeline docpipeline/
COPY fixtures fixtures/
COPY migrations migrations/
COPY local_scripts local_scripts/

ENV PYTHONUNBUFFERED=1

# No ENTRYPOINT/CMD — each Deployment specifies its own `python -m
# docpipeline.<service>` command (see k8s/values.yaml's services: list).
