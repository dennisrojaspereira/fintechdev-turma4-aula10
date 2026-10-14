#!/bin/sh
# Replica por streaming (o primary decide se e sincrona). Na primeira subida copia o primary com pg_basebackup;
# -R grava primary_conninfo e standby.signal, entao o postgres sobe em modo standby.
set -e
PGDATA=/var/lib/postgresql/data/pgdata
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_isready -h pg-primary -U payment -d payment >/dev/null 2>&1; do sleep 1; done
  echo "replica: copiando o primary (pg_basebackup)"
  pg_basebackup -h pg-primary -U replicator -D "$PGDATA" -R -X stream
  chmod 700 "$PGDATA"
fi
# APP_NAME identifica a replica no primary (pg_stat_replication, synchronous_standby_names).
exec postgres -D "$PGDATA" -c hot_standby=on \
  -c "primary_conninfo=host=pg-primary user=replicator password=replicator application_name=${APP_NAME:-replica}"
