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

## Procedures (to be written)

- Replay Silver from Bronze (Phase 6/7)
- Reset the Debezium replication slot and re-snapshot (Phase 3)
- Recover a crashed streaming job (Phase 5)
- Common failures and fixes
