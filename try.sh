#!/usr/bin/env bash
set -euo pipefail

mode=${1:-new-chaos}
project=${JOB_VISIBILITY_COMPOSE_PROJECT:-job-visibility-resilience}

export JOB_VISIBILITY_COMPOSE_PROJECT=$project
export RUN_CASSANDRA_TESTS=1
export RUN_OUTAGE_TESTS=1
export RUN_CHAOS_TESTS=1

scripts/infra bootstrap

case "$mode" in
  new-chaos)
    scripts/infra test-resilience -vv tests/integration/test_outage_matrix.py \
      -k 'poison_record or identity_collision or connect_restart'
    ;;
  full)
    scripts/infra test-resilience
    ;;
  spec005)
    scripts/infra test-postgres -vv \
      tests/integration/test_application_faults.py \
      tests/integration/test_resource_faults.py
    ;;
  *)
    echo "usage: $0 [new-chaos|spec005|full]" >&2
    exit 2
    ;;
esac

scripts/infra diagnostics
