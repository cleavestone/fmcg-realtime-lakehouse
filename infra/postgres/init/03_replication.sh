#!/usr/bin/env bash
# Least-privilege replication user for Debezium + publication over the FMCG tables.
# A shell script (not .sql) so the password comes from the environment, not the repo.
set -euo pipefail

: "${DEBEZIUM_DB_USER:?DEBEZIUM_DB_USER must be set}"
: "${DEBEZIUM_DB_PASSWORD:?DEBEZIUM_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v dbz_user="$DEBEZIUM_DB_USER" -v dbz_password="$DEBEZIUM_DB_PASSWORD" <<'SQL'
CREATE ROLE :"dbz_user" WITH LOGIN REPLICATION PASSWORD :'dbz_password';

-- CONNECT + USAGE + SELECT is all pgoutput streaming and the initial snapshot need.
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'dbz_user') \gexec
GRANT USAGE ON SCHEMA public TO :"dbz_user";
GRANT SELECT ON regions, stores, products, sales_reps, orders, order_items, inventory
    TO :"dbz_user";

-- Created here so Debezium runs with publication.autocreate.mode=disabled
-- and never needs table ownership.
CREATE PUBLICATION fmcg_publication
    FOR TABLE regions, stores, products, sales_reps, orders, order_items, inventory;
SQL

echo "03_replication: role '${DEBEZIUM_DB_USER}' and publication 'fmcg_publication' created"
