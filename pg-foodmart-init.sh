#!/usr/bin/env bash
set -Eeo pipefail

createuser -U postgres foodmart
createdb -U postgres -O foodmart foodmart

PGUSER="${PGUSER:-$POSTGRES_USER}" pg_ctl -D "$PGDATA" -m fast -w stop
PGUSER="${PGUSER:-$POSTGRES_USER}" pg_ctl -D "$PGDATA" -w start
cd /foodmart-data/ && sh FoodMartLoader.sh --db postgres