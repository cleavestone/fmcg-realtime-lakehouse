# ADR-007: Idempotency through checkpoints and LSN-guarded MERGE

**Status:** Accepted (2026-10-03)

## Context
Containers crash and restart, Kafka Connect delivers at least once, and Silver must be rebuildable from Bronze. None of these may create duplicates or regress state.

## Decision
- **Bronze:** Structured Streaming checkpoints on MinIO (`s3a://checkpoints/...`) plus the transactional Delta sink. Kafka offsets and the Delta commit move together, so a restart neither skips nor re-appends. `(topic, partition, offset)` is unique.
- **Silver facts:** in `foreachBatch`, reduce to the latest event per primary key by `source_lsn`, then `MERGE ... WHEN MATCHED AND source.lsn > target.source_lsn THEN UPDATE`. Replays and late duplicates become no-ops.
- **Silver dims:** a new version is inserted only when its `source_lsn` is greater than the key's current version and its `row_hash` differs. Replaying an old batch changes nothing.
- Every job has its own checkpoint location; checkpoints are never shared.

## Consequences
- Any streaming container can be killed at any time without losing or duplicating data.
- Rebuilding Silver means deleting the table and the checkpoint, then restarting the job.
- LSN must be present on every event (guaranteed by ADR-002).

## Alternatives
- **Append-only Silver with dedup at read time:** simpler writes, but every consumer pays the cost and must get it right.
- **Relying on Kafka exactly-once:** doesn't cover the Postgres → Connect hop or replays from Bronze.
