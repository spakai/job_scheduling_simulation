#!/usr/bin/env bash
set -euo pipefail

cqlsh cassandra -u cassandra -p cassandra -f /setup/schema.cql
cqlsh cassandra -u cassandra -p cassandra -e "CREATE ROLE IF NOT EXISTS worker WITH PASSWORD = 'worker-local' AND LOGIN = true"
cqlsh cassandra -u cassandra -p cassandra -e "CREATE ROLE IF NOT EXISTS seed_manager WITH PASSWORD = 'seed-local' AND LOGIN = true"
cqlsh cassandra -u cassandra -p cassandra -e "GRANT SELECT ON KEYSPACE worker_demo TO worker"
cqlsh cassandra -u cassandra -p cassandra -e "GRANT MODIFY ON TABLE worker_demo.records_by_bucket TO worker"
cqlsh cassandra -u cassandra -p cassandra -e "GRANT MODIFY ON TABLE worker_demo.update_operations_by_bucket TO worker"
cqlsh cassandra -u cassandra -p cassandra -e "GRANT ALL PERMISSIONS ON KEYSPACE worker_demo TO seed_manager"
cqlsh cassandra -u seed_manager -p seed-local -f /setup/seed.cql
