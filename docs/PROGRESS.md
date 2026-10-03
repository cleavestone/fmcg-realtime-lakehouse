# Progress

Phase-by-phase build log. Updated at the end of every phase.

| Phase | Status |
|---|---|
| 0. Scaffold and architecture docs | Implemented, awaiting verification |
| 1a. Source database and seed data | — |
| 1b. Business event simulator | — |
| 2. Kafka | — |
| 3. Debezium CDC | — |
| 4. Lake foundation | — |
| 5. Bronze ingestion | — |
| 6. Silver facts | — |
| 7. Silver dimensions (SCD2) | — |
| 8. Trino and dbt Gold | — |
| 9. Power BI report | — |
| 10. Hardening and polish | — |

---

## Phase 0: Scaffold and architecture docs

**Built**
- Repository structure (empty dirs hold a `.gitkeep`).
- README with the project pitch and a Mermaid architecture diagram; `docs/architecture.md`; ADR-001 to ADR-010 in `docs/adr/`.
- uv workspace: a virtual root `pyproject.toml` with an exactly pinned `dev` group (ruff, sqlfluff, pytest, pre-commit, detect-secrets), members `simulator`, `streaming` and `dbt` (the last is `package = false`), `.python-version` = 3.11, and `uv.lock`.
- Compose skeleton: the root `docker-compose.yml` (project `fmcg`, network `fmcg-net`, four named volumes, `include:` of `compose/*.yml`). Each domain file is `services: {}` for now.
- `Makefile` with every §5.4 target. Lifecycle, `test` and `lint` work now; later-phase targets exit with "not implemented until Phase N".
- `.pre-commit-config.yaml`: pre-commit-hooks v6.0.0 (whitespace, EOF, YAML/TOML/JSON, large files, merge conflicts, LF line endings), plus ruff, sqlfluff and detect-secrets run through `uv run` so they match the lockfile. `.secrets.baseline` is committed.
- BI is Power BI, outside the Compose stack, connected to Trino (ADR-010). No Superset service, `bi` profile or `superset-data` volume.
- `.gitignore`, `.dockerignore`, `.gitattributes` (LF everywhere), `.env.example`, `.sqlfluff` (postgres dialect).

**How to run**
```bash
uv sync
uv run pre-commit install
make lint
docker compose config -q
```

**Decisions**
- The repo lives in the WSL filesystem (`~/projects/fmcg-realtime-lakehouse`), not under `/mnt/c`, for bind-mount and I/O performance.
- Kafka UI: `provectuslabs/kafka-ui` is unmaintained, so the build uses the maintained fork `ghcr.io/kafbat/kafka-ui` (Phase 2).
- MinIO: community Docker images are no longer published. Phase 4 will either pin the last published `RELEASE.*` tag or pick an alternative S3-compatible store.
- Debezium: images come from `quay.io/debezium/connect`, which is no longer on Docker Hub (Phase 3).
- Kafka pinned to the 3.9.x line (ADR-001). Hive Metastore to be pinned at 3.1.3 and validated in Phase 4 (ADR-005).
- The base Python image is `python:3.11.x-slim-bookworm`, not trixie, because Debian trixie no longer ships OpenJDK 17, which Spark 3.5 needs.
- `make test` treats pytest exit code 5 (no tests collected) as success until tests exist.

**Known issues**
- None.
