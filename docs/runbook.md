# Runbook

Operational procedures. Filled in as each component lands; completed in Phase 10.

## Start / stop

| Action | Command |
|---|---|
| Start one profile | `make up P=<profile>` |
| Start everything | `make demo` |
| Stop, keep data | `make down` |
| Stop and wipe all volumes | `make nuke` |
| Status | `make ps` |
| Logs | `make logs S=<service>` |

## CDC (Debezium)

| Check | Command |
|---|---|
| Connector and task state | `make connector-status` (both should be `RUNNING`) |
| Topic message counts vs Postgres rows | `make cdc-counts` |
| Latest events for a table | `make cdc-tail T=orders N=5` |
| Re-apply the connector config | `make register-connector` (idempotent `PUT`) |
| Replication slot health | `make psql`, then `SELECT slot_name, active, wal_status FROM pg_replication_slots;` |

### Connector FAILED
1. Find the cause: `make logs S=kafka-connect` (errors are logged with the offending record).
2. Fix it (config, permissions, schema).
3. Restart the task: `curl -X POST "localhost:8083/connectors/fmcg-postgres/restart?includeTasks=true&onlyFailed=true"`.
   Because the replication slot kept the WAL, the connector resumes at its last committed LSN and nothing is lost.

### Connect was down for a while
Nothing to do. On start it resumes from its offset in `connect-offsets`, and the slot kept the WAL. Verified: a 20-second outage with the simulator running lost no changes.

### Slot invalidated (`wal_status = lost`) or re-snapshot needed
This happens when Connect was down long enough for retained WAL to exceed `max_slot_wal_keep_size` (2GB). The slot can't resume and Debezium must take a fresh snapshot. For this local stack, the verified way is a full reset: `make nuke && make up P=cdc` (or `make demo`). A targeted re-snapshot that keeps Bronze and Silver (drop the slot and reset the connector's offsets) will be written and tested in Phase 10.

## Procedures (to be written)

- Replay Silver from Bronze (Phase 6/7)
- Recover a crashed streaming job (Phase 5)
