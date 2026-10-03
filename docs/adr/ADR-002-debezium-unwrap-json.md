# ADR-002: Debezium unwrap SMT, JSON without schemas

**Status:** Accepted (2026-10-03)

## Context
Debezium's default event envelope (`before`, `after`, `source`, `op`, ...) is verbose and couples every consumer to Debezium's format. Consumers mostly need the new row state plus a few pieces of metadata for ordering and delete handling.

## Decision
- Apply the `io.debezium.transforms.ExtractNewRecordState` SMT with:
  - `add.fields=op,source.ts_ms,source.lsn`, which produces flat `__op`, `__source_ts_ms` and `__source_lsn` fields
  - `delete.handling.mode=rewrite` (or the 2.x equivalent `delete.tombstone.handling.mode=rewrite`), which emits deletes as the last row state with `__deleted=true`
  - tombstones dropped
- Use the JSON converter with `schemas.enable=false`.
- Emit decimals as strings (`decimal.handling.mode=string`) and cast them to `DECIMAL` in Spark, so no precision is lost to doubles.

## Consequences
- Bronze parsing is simple: one flat JSON object per record.
- With no schema in the message, Spark owns an explicit schema per table. Schema changes in Postgres require a code change.
- `__source_lsn` gives a total commit order per database, which Silver uses for dedup and SCD2 ordering.

## Alternatives
- **Avro + Schema Registry:** proper schema evolution and smaller messages. This is the production path, deferred because it adds a service and serde complexity for a weekend build.
- **Full envelope:** more information (`before` image) but more parsing logic, and nothing downstream needs `before`.
