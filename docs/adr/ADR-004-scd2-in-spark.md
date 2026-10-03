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

## Consequences
- Every committed change is captured, including bursts that land within a single micro-batch.
- Validity windows reflect real commit time, which makes point-in-time joins in Gold accurate.
- More code than a dbt snapshot, so it needs unit tests (new key, change, no-op, multi-change batch, delete, replay).

## Alternatives
- **dbt snapshots:** simple, but they miss intermediate changes and use batch timing.
- **SCD2 in dbt from the Bronze change log (incremental model):** feasible and accurate, but latency is bound to the dbt schedule rather than near real time.
