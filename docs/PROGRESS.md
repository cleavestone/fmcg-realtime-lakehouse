# Progress

Phase-by-phase build log. Updated at the end of every phase.

| Phase | Status |
|---|---|
| 0. Scaffold and architecture docs | Done |
| 1a. Source database and seed data | Implemented, awaiting verification |
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

## Phase 1a: Source database and seed data

**Built**
- `compose/source.yml` (profile `source`):
  - **`postgres`:** pinned `postgres:16.15`, started with `wal_level=logical`, `max_replication_slots=4` and `max_wal_senders=4`. It uses the `pg-data` volume, port 5432, a memory limit and log rotation.
  - **Healthcheck:** `pg_isready -h 127.0.0.1`. It checks over TCP so the service isn't reported healthy while the init scripts are still running.
  - **`seed`:** a one-shot job built from the simulator image that waits for `postgres` to be healthy and exits 0.
- `infra/postgres/init/` (runs only on an empty volume):
  - `01_schema.sql`: 7 tables with identity primary keys, foreign keys, check constraints (statuses, channels, tiers, categories, non-negative stock, positive prices, discount 0–100), `UNIQUE (order_id, product_id)` on `order_items`, and indexes on the foreign keys and on `orders(status)` and `orders(order_ts)`.
  - `02_triggers.sql`: a `set_updated_at()` trigger on every table. `regions` and `order_items` also got an `updated_at` column so every table has one.
  - `03_replication.sh`: the `debezium` role (LOGIN REPLICATION, plus CONNECT, USAGE and SELECT only) and the `fmcg_publication` publication over the 7 tables. It's a shell script so the password comes from `.env`.
- `simulator` package:
  - `catalog.py`: 29 fictional Kenyan brands across 5 categories. Each pack size is sold as a single unit and as a case, giving 200 SKUs priced in KES.
  - `seed.py`:
    - `generate(cfg, now)` is pure: it takes the config and the clock and returns rows, so it can be tested without a database. `run_seed()` loads everything in a single transaction under a table lock and skips if data already exists.
    - Explicit IDs are inserted with `OVERRIDING SYSTEM VALUE`, then the identity sequences are moved past them.
  - `config.yaml` (`seed:` section): counts, regions with weights and warehouses, the channel mix and order shapes, tier discounts, the Zipf popularity exponent, status by order age, and inventory ranges.
  - `db.py` connects with psycopg 3 using env settings; `config.py` loads the config (`SIM_CONFIG` overrides the path).
  - `__main__.py` is a Typer CLI with `python -m simulator seed`.
- `simulator/Dockerfile`: follows the uv workspace pattern (dependency layer first, then the project, installed non-editable), uses a BuildKit cache and runs as non-root (uid 10001).
- `simulator/tests/test_seed.py`: 11 tests covering determinism, counts, references, price copying, the rep and store region match, non-negative stock, status by order age, and clock independence of master data.
- `Makefile`: `make seed` and `make psql`.

**Seed result** (`random_seed: 42`): 5 regions, 50 stores, 200 products, 10 reps, 600 inventory rows, and about 2,400 orders with about 10,500 order items spanning about 30 days. Most orders are delivered, about 6% cancelled, and recent ones are still placed, confirmed or shipped. The top 20% of SKUs account for about 70% of order lines.

**How to run**
```bash
make up P=source   # postgres + seed
make psql          # interactive psql
make seed          # re-run the seed (prints "seed skipped" when populated)
make nuke          # wipe volumes for a fresh seed
```

**Decisions**
- Company setting: a Kenyan distributor, priced in KES, with regions Nairobi, Central, Coast, Rift Valley and Western, warehouses `nairobi_dc`, `mombasa_dc` and `kisumu_dc`, and EAT business hours. Brand names are fictional.
- Determinism: master data, popularity and inventory are byte-identical across clean starts, because each section has its own random stream. Order timestamps are anchored to the moment of seeding so history always ends "today"; two seeds on the same day produce identical orders except for the ones placed later in the day.
- The seed sets current inventory directly rather than replaying stock movements for the historical orders. About 10% of SKU/warehouse pairs start low so that restock events have targets.
- Debezium gets a pre-created publication (to be used with `publication.autocreate.mode=disabled`) and no ownership or write privileges.

**Known issues**
- On WSL, Docker Engine prints "Your kernel does not support swap limit capabilities". This is harmless: memory limits still apply, without swap accounting.

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
