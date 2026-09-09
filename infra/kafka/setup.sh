#!/usr/bin/env bash
set -euo pipefail

kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic job-lifecycle-edr.v1 --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic job-lifecycle-edr-dlq.v1 --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic job-requests.v1 --partitions 6 --replication-factor 1
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic job-results.v1 --partitions 6 --replication-factor 1
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic job-requests-dlq.v1 --partitions 6 --replication-factor 1

schema=$(python -c 'import json; print(json.dumps(open("/setup/schemas/job-lifecycle-edr-v1.json").read()))')
curl -fsS -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data '{"compatibility":"BACKWARD_TRANSITIVE"}' \
  http://schema-registry:8081/config/job-lifecycle-edr.v1-value
curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data "{\"schemaType\":\"JSON\",\"schema\":${schema}}" \
  http://schema-registry:8081/subjects/job-lifecycle-edr.v1-value/versions

for topic in job-requests.v1 job-results.v1; do
  schema_file="/setup/schemas/${topic//./-}.json"
  schema=$(python -c 'import json,sys; print(json.dumps(open(sys.argv[1]).read()))' "$schema_file")
  curl -fsS -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data '{"compatibility":"BACKWARD_TRANSITIVE"}' \
    "http://schema-registry:8081/config/${topic}-value"
  curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data "{\"schemaType\":\"JSON\",\"schema\":${schema}}" \
    "http://schema-registry:8081/subjects/${topic}-value/versions"
done
