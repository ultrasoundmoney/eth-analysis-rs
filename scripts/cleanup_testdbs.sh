#!/bin/bash

# Script to delete PostgreSQL databases with names starting with 'testdb_'
# It assumes psql can connect using environment variables for credentials,
# or you may need to add connection parameters to the psql commands.

# Database to connect to for running DROP DATABASE commands (a maintenance DB)
MAINTENANCE_DB="postgres"
# User for connecting to the maintenance DB (if not set by PGUSER env var)
# PSQL_USER="your_postgres_user" 

echo "Fetching list of databases starting with 'testdb_'..."

# Construct psql command, allowing for user override via environment variables
# -d specifies the database to connect to.
# -t enables tuples-only output (no headers, footers, etc.).
# -A enables unaligned table output mode.
# -c specifies the command to run.
PSQL_CMD="psql -d ${PGDATABASE:-$MAINTENANCE_DB} -tAc"
# If you need to specify a user directly and it's not in PGUSER:
# PSQL_CMD="psql -U $PSQL_USER -d ${PGDATABASE:-$MAINTENANCE_DB} -tAc"


DATABASES_TO_DROP=$($PSQL_CMD "SELECT datname FROM pg_database WHERE datname LIKE 'testdb_%';")

if [ -z "$DATABASES_TO_DROP" ]; then
  echo "No databases found with names starting with 'testdb_'."
  exit 0
fi

echo ""
echo "The following databases are slated for deletion:"
echo "$DATABASES_TO_DROP"
echo ""

read -p "Are you sure you want to permanently delete these databases? (yes/no): " confirmation

if [[ "$confirmation" != "yes" ]]; then
  echo "Aborted by user. No databases were deleted."
  exit 0
fi

echo ""
for db_name in $DATABASES_TO_DROP; do
  echo "Attempting to drop database: $db_name"
  # Drop database while connected to the maintenance DB
  # Quoting the database name to handle any special characters, though unlikely with nanoid.
  # IF EXISTS prevents errors if the database was somehow deleted between the check and now.
  $PSQL_CMD "DROP DATABASE IF EXISTS \"$db_name\";"
  if [ $? -eq 0 ]; then
    echo "Successfully dropped $db_name"
  else
    echo "Failed to drop $db_name. It might have active connections or other issues."
    echo "You may need to terminate connections manually: "
    echo "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$db_name';"
  fi
done

echo ""
echo "Cleanup process complete." 