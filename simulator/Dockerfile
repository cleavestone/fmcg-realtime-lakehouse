# syntax=docker/dockerfile:1
# Simulator + seed image. Build context: repo root (uv workspace).
ARG PYTHON_IMAGE
ARG UV_IMAGE

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE}

COPY --from=uv /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 1) Dependencies only (cached until uv.lock or a pyproject changes).
COPY pyproject.toml uv.lock ./
COPY simulator/pyproject.toml simulator/pyproject.toml
COPY streaming/pyproject.toml streaming/pyproject.toml
COPY dbt/pyproject.toml dbt/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package simulator --no-install-workspace

# 2) The simulator package itself.
COPY simulator/src simulator/src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package simulator --no-editable

RUN useradd --system --uid 10001 --no-create-home app
USER app

ENTRYPOINT ["/app/.venv/bin/python", "-m", "simulator"]
CMD ["--help"]
