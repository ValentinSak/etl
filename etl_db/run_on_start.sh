#!/bin/bash
set -e

docker-entrypoint.sh postgres &

until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
  echo "Waiting for PostgreSQL to start..."
  sleep 2
done

echo "Running create_schema.sql..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /sql_scripts/create_schema.sql
sleep 2
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /sql_scripts/fill_tables.sql

wait
