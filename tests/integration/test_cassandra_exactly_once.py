from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock
from uuid import uuid4

import pytest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from job_visibility.config import CassandraConfig
from job_visibility.scheduler import CassandraDriverClient, WorkloadRecord


def _config() -> CassandraConfig:
    return CassandraConfig(
        contact_points=(os.getenv("CASSANDRA_HOST", "localhost"),),
        port=int(os.getenv("CASSANDRA_PORT", "9042")),
        request_timeout_ms=2_000,
        execution_timeout_ms=10_000,
        max_record_count=1,
        page_size=1,
        read_concurrency=1,
    )


def _seed(dataset_id: str) -> None:
    config = _config()
    cluster = Cluster(
        list(config.contact_points),
        port=config.port,
        auth_provider=PlainTextAuthProvider(username="seed_manager", password="seed-local"),
    )
    session = cluster.connect("worker_demo")
    try:
        session.execute(
            """INSERT INTO datasets
            (dataset_id,dataset_version,record_count,bucket_count,seed,created_at)
            VALUES (%s,1,1,1,3003,toTimestamp(now()))""",
            (dataset_id,),
        )
        session.execute(
            """INSERT INTO records_by_bucket
            (dataset_id,bucket,record_id,input_number,checksum)
            VALUES (%s,0,0,20,0)""",
            (dataset_id,),
        )
    finally:
        session.shutdown()
        cluster.shutdown()


def _state(client: CassandraDriverClient, dataset_id: str) -> object:
    return client.session.execute(
        """SELECT checksum,last_operation_id,pending_operation_id FROM records_by_bucket
        WHERE dataset_id=%s AND bucket=0 AND record_id=0""",
        (dataset_id,),
    ).one()


@pytest.mark.integration
@pytest.mark.cassandra
def test_post_finalize_response_loss_reconciles_exactly_once() -> None:
    if os.getenv("RUN_CASSANDRA_TESTS") != "1" and os.getenv("RUN_RESILIENCE_TESTS") != "1":
        pytest.skip("set RUN_CASSANDRA_TESTS=1 to run Cassandra chaos tests")
    dataset_id = f"r-worker-05-{uuid4()}"
    operation_id = uuid4()
    _seed(dataset_id)
    injected = False

    def lose_response(_: WorkloadRecord, operation: object) -> None:
        nonlocal injected
        if not injected:
            injected = True
            raise TimeoutError(f"lost response for {operation}")

    client = CassandraDriverClient(
        _config(),
        username="worker",
        password="worker-local",
        after_finalize=lose_response,
    )
    record = WorkloadRecord(dataset_id, 0, 0, 20, 0)
    try:
        first = client.apply_once(record=record, operation_id=operation_id, fibonacci_result="6765")
        replay = client.apply_once(
            record=record, operation_id=operation_id, fibonacci_result="6765"
        )
        state = _state(client, dataset_id)

        assert first == (True, 1)
        assert replay == (True, 1)
        assert state.checksum == 1
        assert state.last_operation_id == operation_id
    finally:
        client.close()


@pytest.mark.integration
@pytest.mark.cassandra
def test_two_workers_force_one_checksum_conflict_then_apply_distinct_operations_once() -> None:
    if os.getenv("RUN_CASSANDRA_TESTS") != "1" and os.getenv("RUN_RESILIENCE_TESTS") != "1":
        pytest.skip("set RUN_CASSANDRA_TESTS=1 to run Cassandra chaos tests")
    dataset_id = f"r-worker-06-{uuid4()}"
    _seed(dataset_id)
    barrier = Barrier(2)
    barrier_lock = Lock()
    barrier_calls = 0

    def synchronize(_: WorkloadRecord, __: object) -> None:
        nonlocal barrier_calls
        with barrier_lock:
            barrier_calls += 1
            should_wait = barrier_calls <= 2
        if should_wait:
            barrier.wait(timeout=5)

    clients = [
        CassandraDriverClient(
            _config(),
            username="worker",
            password="worker-local",
            before_reserve=synchronize,
        )
        for _ in range(2)
    ]
    operations = [uuid4(), uuid4()]
    original = WorkloadRecord(dataset_id, 0, 0, 20, 0)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(
                    client.apply_once,
                    record=original,
                    operation_id=operation,
                    fibonacci_result="6765",
                )
                for client, operation in zip(clients, operations, strict=True)
            ]
            results = [future.result(timeout=10) for future in futures]

        assert sorted(results) == [(False, 0), (True, 1)]
        loser = results.index((False, 0))
        retry = clients[loser].apply_once(
            record=WorkloadRecord(dataset_id, 0, 0, 20, 1),
            operation_id=operations[loser],
            fibonacci_result="6765",
        )
        replay = clients[loser].apply_once(
            record=WorkloadRecord(dataset_id, 0, 0, 20, 1),
            operation_id=operations[loser],
            fibonacci_result="6765",
        )
        state = _state(clients[loser], dataset_id)

        assert retry == (True, 2)
        assert replay == (True, 2)
        assert state.checksum == 2
        assert barrier_calls >= 2
    finally:
        for client in clients:
            client.close()
