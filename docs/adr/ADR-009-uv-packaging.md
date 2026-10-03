# ADR-009: uv workspace for Python packaging

**Status:** Accepted (2026-10-03)

## Context
Three Python components (simulator, Spark jobs, dbt) run both on the host (tests, linting, IDE) and in Docker images. Installs have to be reproducible and fast in both places.

## Decision
- One uv workspace: the root `pyproject.toml` is a virtual workspace root with a `dev` dependency group (ruff, sqlfluff, pytest, pre-commit, detect-secrets). The members are `simulator`, `streaming` and `dbt`, each declaring only its runtime dependencies.
- One committed `uv.lock`, and `.python-version` set to 3.11. Direct dependencies are pinned exactly in `pyproject.toml`; transitive ones are pinned by the lockfile.
- Images: copy the uv binary from a pinned `ghcr.io/astral-sh/uv` image, install dependencies first (`uv sync --frozen --no-dev --package <member> --no-install-project`) for layer caching, then install the project. Use a BuildKit cache mount, `UV_COMPILE_BYTECODE=1` and `UV_LINK_MODE=copy`, and run as a non-root user from `/app/.venv`.
- pre-commit runs ruff, sqlfluff and detect-secrets through `uv run`, so hook versions always match the lockfile.
- CI uses `uv sync --frozen`, so a stale lockfile fails the build.

## Consequences
- One resolver and one lockfile for host and containers; no `requirements.txt` drift.
- All members must agree on shared transitive versions. A conflict (for example between dbt's and pyspark's dependencies) would require splitting the workspace.
- The `dbt` member is `package = false`: it pins tooling and is not an installable package.

## Alternatives
- **pip + requirements.txt per image:** slower, no single lock, easy drift.
- **Poetry:** slower and weaker workspace support.
