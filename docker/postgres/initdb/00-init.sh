#!/bin/bash
# Runs once on first PostgreSQL start: Airflow metadata DB + operational schema.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
  SELECT 'CREATE DATABASE airflow OWNER ${POSTGRES_USER}'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
SQL

for f in /sql/ddl/*.sql; do
  echo "Applying ${f}"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$f"
done
