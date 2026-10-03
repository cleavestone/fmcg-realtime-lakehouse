#!/bin/bash
# Run the stock entrypoint, but only initialise the schema when it doesn't exist yet.
set -euo pipefail

export HIVE_CONF_DIR="$HIVE_HOME/conf"
if [ -d "${HIVE_CUSTOM_CONF_DIR:-}" ]; then
  find "$HIVE_CUSTOM_CONF_DIR" -type f -exec ln -sfn {} "$HIVE_CONF_DIR"/ \;
fi

# schematool -info succeeds only when the metastore schema is already installed.
if HADOOP_CLIENT_OPTS="${SERVICE_OPTS:-}" "$HIVE_HOME/bin/schematool" -dbType postgres -info \
     > /tmp/schematool-info.log 2>&1; then
  echo "hms-entrypoint: metastore schema present, skipping init"
  export IS_RESUME=true
else
  echo "hms-entrypoint: no metastore schema yet, initialising"
  export IS_RESUME=false
fi

exec /entrypoint.sh
