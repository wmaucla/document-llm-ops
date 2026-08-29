FROM python:3.13-slim

# poppler-utils: pdf2image's rasterisation backend (real dependency in prod
# too, not a local-only shim — see 'Rasterisation is a real local dependency').
# tesseract-ocr: only exercised when OCR_ENGINE=tesseract (opt-in realism check).
RUN apt-get update && apt-get install --no-install-recommends -y \
    poppler-utils \
    tesseract-ocr \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

COPY docpipeline docpipeline/
COPY fixtures fixtures/
COPY migrations migrations/

ENV PYTHONUNBUFFERED=1

# No ENTRYPOINT/CMD — each Deployment specifies its own `python -m
# docpipeline.<service>` command (see k8s/deployments.yaml).
