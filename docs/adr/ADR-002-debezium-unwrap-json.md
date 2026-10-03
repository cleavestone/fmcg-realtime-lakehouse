# ADR-002: Debezium unwrap SMT, JSON without schemas, fail-fast errors

**Status:** Accepted (2026-10-03; error handling and replica identity revised in Phase 3)

## Context
Debezium's default event envelope (`before`, `after`, `source`, `op`, ...) is verbose and couples every consumer to Debezium's format. Consumers mostly need the new row state plus a few pieces of metadata for ordering and delete handling.

## Decision
- Apply the `io.debezium.transforms.ExtractNewRecordState` SMT with:
  - `add.fields=op,source.ts_ms,source.lsn`, which produces flat `__op`, `__source_ts_ms` and `__source_lsn` fields
  - `delete.tombstone.handling.mode=rewrite`, which emits each delete as the row's last state with `__deleted="true"` and no tombstone (`tombstones.on.delete=false`)
- Use the JSON converter with `schemas.enable=false`. The message key is the primary key, so all changes to one row land in one partition, in commit order.
- Emit decimals as strings (`decimal.handling.mode=string`) and cast them to `DECIMAL` in Spark, so no precision is lost to doubles.
- Every source table uses `REPLICA IDENTITY FULL`. With the default identity (primary key only), a delete event carries placeholder values (`0`, `1970-01-01`, column defaults) for every non-key NOT NULL column. FULL makes delete events carry the row's real last state, at the cost of logging the old row on every update and delete, which is negligible at this volume.
- **Errors fail fast** (`errors.tolerance=none`, with errors logged in full). Kafka Connect dead-letter queues exist only for *sink* connectors, and `errors.tolerance=all` on a source connector silently drops the record, which for CDC means silent data loss. A failed connector shows `FAILED`, the replication slot keeps the WAL, and after a fix the connector resumes with nothing lost. Records that reach Kafka but can't be parsed are quarantined in Bronze (Phase 5), which is the pipeline's dead-letter layer.
- Credentials are resolved at runtime by Kafka's `EnvVarConfigProvider` (`${env:DEBEZIUM_DB_PASSWORD}`), so they never appear in the `connect-configs` topic.

## Consequences
- Bronze parsing is simple: one flat JSON object per record.
- With no schema in the message, Spark owns an explicit schema per table. Schema changes in Postgres require a code change.
- `__source_lsn` gives a total commit order per database, which Silver uses for dedup and SCD2 ordering. Snapshot rows (`__op="r"`) all share the snapshot's LSN, which is fine because each key appears once in a snapshot.
- A conversion error stops CDC until someone fixes it. That's the right trade for a system of record, and the stop is visible in `make connector-status` and Kafka UI.

## Alternatives
- **Avro + Schema Registry:** proper schema evolution and smaller messages. This is the production path, deferred because it adds a service and serde complexity for a weekend build.
- **Full envelope:** more information (`before` image) but more parsing logic, and nothing downstream needs `before`.
- **`errors.tolerance=all` with a DLQ topic:** not supported for source connectors (the DLQ settings are ignored), so it reduces to silently skipping changes.
