#!/bin/sh
# Prepara o primary para streaming replication: usuario de replicacao + regra no pg_hba.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -c "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';"
echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"
