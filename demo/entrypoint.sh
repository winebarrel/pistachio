#!/bin/sh
set -e

PGDATA=/var/lib/postgresql/data

pg_ctl -D "$PGDATA" -l /tmp/pg.log start
trap 'pg_ctl -D "$PGDATA" -m fast stop >/dev/null 2>&1 || true' EXIT

until pg_isready -q; do sleep 0.1; done

[ -f /demo/manual.txt ] && cat /demo/manual.txt

exec "$@"
