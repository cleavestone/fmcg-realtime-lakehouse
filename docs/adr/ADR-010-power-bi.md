# ADR-010: Power BI for reporting, outside the Compose stack

**Status:** Accepted (2026-10-03)

## Context
The Gold layer needs a BI front end. Running our own BI server (e.g. Apache Superset) means another service to host, initialise and keep alive. Power BI is the reporting tool the target audience already uses.

## Decision
- Use **Power BI** for reporting. It is not a container and is not part of the Compose stack.
- Power BI reads **Gold tables through Trino** (`localhost:8081`), the same SQL entry point dbt uses. It never reads Delta files on MinIO directly, so Trino and the metastore stay the single source of table definitions.
- The report is saved as a **Power BI Project (`.pbip`)** in `dashboards/`. That format is text-based (TMDL semantic model + report definition), so changes are diffable and reviewable in git.

### Connectivity (validated in the Power BI phase)
Power BI has no built-in Trino connector, so one of these is required:

| Option | Mode | Notes |
|---|---|---|
| Trino ODBC driver + Power BI ODBC source | Import | Simplest and most robust; data refreshes when the report is refreshed |
| Community Trino custom connector (`.mez`) | Import or DirectQuery | DirectQuery shows new Gold data without a manual refresh, but requires allowing uncertified connectors in Power BI Desktop |

Publishing to the Power BI Service would additionally need an on-premises data gateway on a machine that can reach Trino. That is out of scope; the build targets Power BI Desktop.

## Consequences
- One fewer service: no BI container, metadata database or init job. The `bi` profile and `superset-data` volume are removed.
- The dashboard is not created automatically by `make demo`. Opening the `.pbip` in Power BI Desktop and refreshing is a manual step, documented in the README.
- Power BI Desktop runs on Windows. With Docker in WSL2, published ports such as `localhost:8081` are reachable from Windows.
- Gold freshness in the report is bounded by the dbt schedule plus the Power BI refresh (Import) or by the dbt schedule alone (DirectQuery).

## Alternatives
- **Apache Superset in Compose:** fully automated and reproducible, but one more service to run, and not the tool the audience uses.
- **Power BI reading Delta from MinIO directly** (e.g. via a Fabric shortcut or Spark connector): bypasses the catalog, and Fabric is cloud-only.
