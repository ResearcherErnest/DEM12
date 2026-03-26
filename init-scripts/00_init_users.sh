#!/bin/bash
set -e

# Default passwords if not provided in env
AIRFLOW_PASS="${AIRFLOW_DB_PASSWORD:-change_me_airflow}"
METABASE_PASS="${MB_DB_PASS:-change_me_metabase}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  -- Create additional databases
  SELECT 'CREATE DATABASE airflow'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

  SELECT 'CREATE DATABASE metabase'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

  -- Create service accounts
  DO \$\$
  BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
      CREATE USER airflow_user WITH PASSWORD '${AIRFLOW_PASS}';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase_user') THEN
      CREATE USER metabase_user WITH PASSWORD '${METABASE_PASS}';
    END IF;
  END
  \$\$;

  -- Grant database-level privileges
  GRANT ALL PRIVILEGES ON DATABASE airflow  TO airflow_user;
  GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase_user;
  GRANT ALL PRIVILEGES ON DATABASE sales    TO ${POSTGRES_USER};
  GRANT CONNECT ON DATABASE sales           TO metabase_user;
EOSQL

# PG 15+ revoked default CREATE on public schema â€” grant it explicitly
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname airflow <<-EOSQL2
  GRANT CREATE ON SCHEMA public TO airflow_user;
  GRANT USAGE  ON SCHEMA public TO airflow_user;
EOSQL2

# Metabase schema grants + citext extension (required by Metabase migrations)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname metabase <<-EOSQL3
  CREATE EXTENSION IF NOT EXISTS citext;
  GRANT CREATE ON SCHEMA public TO metabase_user;
  GRANT USAGE  ON SCHEMA public TO metabase_user;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES    TO metabase_user;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO metabase_user;
EOSQL3

# Grant metabase_user read access to all sales tables (for dashboard queries)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname sales <<-EOSQL4
  GRANT USAGE  ON SCHEMA public TO metabase_user;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO metabase_user;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES    TO metabase_user;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO metabase_user;
EOSQL4