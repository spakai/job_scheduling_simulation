#!/usr/bin/env bash
set -euo pipefail

connect_url=${CONNECT_URL:-http://localhost:8083}
config_file=${1:-infra/kafka/connect/edr-jdbc-sink.json}
python_command=${PYTHON_COMMAND:-python3}
name=$($python_command -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$config_file")
config=$($python_command -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$config_file")

if curl -fsS "$connect_url/connectors/$name" >/dev/null 2>&1; then
  curl -fsS -X PUT -H 'Content-Type: application/json' --data "$config" \
    "$connect_url/connectors/$name/config"
else
  curl -fsS -X POST -H 'Content-Type: application/json' --data @"$config_file" \
    "$connect_url/connectors"
fi
for attempt in 1 2 3 4 5; do
  if curl -fsS "$connect_url/connectors/$name/status"; then
    exit 0
  fi
  sleep 1
done
exit 1
