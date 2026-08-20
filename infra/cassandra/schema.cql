CREATE KEYSPACE IF NOT EXISTS worker_demo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS worker_demo.records_by_bucket (
    dataset_id text,
    bucket int,
    record_id bigint,
    input_number int,
    fibonacci_result text,
    checksum bigint,
    pending_operation_id uuid,
    last_operation_id uuid,
    updated_at timestamp,
    PRIMARY KEY ((dataset_id, bucket), record_id)
);

CREATE TABLE IF NOT EXISTS worker_demo.update_operations_by_bucket (
    dataset_id text,
    bucket int,
    record_id bigint,
    operation_id uuid,
    previous_checksum bigint,
    new_checksum bigint,
    fibonacci_result text,
    applied_at timestamp,
    PRIMARY KEY ((dataset_id, bucket), record_id, operation_id)
);

CREATE TABLE IF NOT EXISTS worker_demo.datasets (
    dataset_id text PRIMARY KEY,
    dataset_version int,
    record_count bigint,
    bucket_count int,
    seed bigint,
    created_at timestamp
);
