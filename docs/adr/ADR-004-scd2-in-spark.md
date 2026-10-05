# ADR-004: SCD Type 2 in Spark, not dbt snapshots

**Status:** Accepted (2026-10-03)

## Context
dbt snapshots implement SCD2 by comparing the current source state with the last snapshot each time `dbt snapshot` runs. Any intermediate states between two runs are lost: a price changed three times in five minutes yields one version, not three. The `valid_from` value is also the snapshot time, not the time of the change.

## Decision
Maintain SCD2 dimensions in a Spark streaming job (`spark-silver-dims`) driven by CDC events:
- Each change event with a different `row_hash` over the tracked attributes becomes a version.
- Versions are ordered by `source_lsn`. `valid_from` / `valid_to` come from `source_ts_ms`, and `valid_to` is derived with `lead()` within the batch.
- Per batch: close the current open version, then insert all new versions, in one Delta `MERGE`.
- Deletes close the current version and mark it `is_deleted`.

### Implementation details (Phase 7)
- **Tracked attributes:** a new version is created only when the SHA-256 `row_hash` over the tracked attributes changes. `updated_at` is excluded, because the trigger bumps it on every UPDATE, including no-ops.
- **Window convention:** windows are half-open, `[valid_from, valid_to)`. Each `valid_to` equals the next version's `valid_from`, and an open version has `valid_to = 9999-12-31` (not NULL, so Gold's point-in-time join is a plain range predicate).
- **First version:** the first version of an entity that came from the Debezium snapshot (`op = 'r'`) is valid from `1900-01-01`. History before CDC started is unknown, and without this the 30 days of backfilled orders would match no dimension version in Gold. Live inserts (`op = 'c'`) use their real commit time.
- **Deterministic surrogate keys:** `xxhash64(natural_key, source_lsn)`. Together with ignoring events at or below a key's latest stored LSN, this makes replays and rebuilds produce exactly the same rows.
- **One MERGE per batch:** each batch is applied as a single MERGE keyed on the surrogate key: close the existing current versions and insert the new ones.

## Consequences
- Every committed change is captured, including bursts that land within a single micro-batch.
- Validity windows reflect real commit time, which makes point-in-time joins in Gold accurate.
- More code than a dbt snapshot, so it needs unit tests (new key, change, no-op, multi-change batch, delete, replay).

## Alternatives
- **dbt snapshots:** simple, but they miss intermediate changes and use batch timing.
- **SCD2 in dbt from the Bronze change log (incremental model):** feasible and accurate, but latency is bound to the dbt schedule rather than near real time.
