# One image for all ingestion adapters, config-driven (docker run ... python -m ingestion.poller <city>).
# Used in Phase 2 cloud deploy; local dev runs the poller via uv on the host.
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv export --no-group dev --no-emit-project | uv pip install --system -r -
COPY ingestion ./ingestion
COPY tests/__init__.py ./tests/__init__.py

ENTRYPOINT ["python", "-m", "ingestion.poller"]
