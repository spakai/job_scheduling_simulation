from __future__ import annotations

import random
from collections.abc import Callable, Sequence
from uuid import UUID

from cassandra.cluster import Cluster, Session
from cassandra.concurrent import execute_concurrent_with_args

from job_visibility.config import CassandraConfig

from .cassandra_workload import WorkloadRecord


class CassandraDriverClient:
    """Bounded Cassandra adapter with operation-marker reconciliation."""

    def __init__(
        self,
        config: CassandraConfig,
        *,
        username: str,
        password: str,
        before_reserve: Callable[[WorkloadRecord, UUID], None] | None = None,
        after_finalize: Callable[[WorkloadRecord, UUID], None] | None = None,
    ) -> None:
        from cassandra.auth import PlainTextAuthProvider

        self.config = config
        self.cluster = Cluster(
            list(config.contact_points),
            port=config.port,
            auth_provider=PlainTextAuthProvider(username=username, password=password),
            connect_timeout=config.request_timeout_ms / 1000,
        )
        self.session: Session = self.cluster.connect(config.keyspace)
        self.before_reserve = before_reserve
        self.after_finalize = after_finalize
        self.session.default_timeout = config.request_timeout_ms / 1000
        self._record = self.session.prepare("""SELECT dataset_id,bucket,record_id,
            input_number,checksum,pending_operation_id,last_operation_id FROM records_by_bucket
            WHERE dataset_id=? AND bucket=? AND record_id=?""")
        self._operation = self.session.prepare("""SELECT new_checksum
            FROM update_operations_by_bucket WHERE dataset_id=? AND bucket=?
            AND record_id=? AND operation_id=?""")
        self._reserve = self.session.prepare("""UPDATE records_by_bucket
            SET pending_operation_id=? WHERE dataset_id=? AND bucket=? AND record_id=?
            IF checksum=? AND pending_operation_id=null""")
        self._finish = self.session.prepare("""UPDATE records_by_bucket SET
            fibonacci_result=?,checksum=?,last_operation_id=?,pending_operation_id=null,
            updated_at=toTimestamp(now()) WHERE dataset_id=? AND bucket=? AND record_id=?
            IF pending_operation_id=? AND checksum=?""")
        self._mark = self.session.prepare("""INSERT INTO update_operations_by_bucket
            (dataset_id,bucket,record_id,operation_id,previous_checksum,new_checksum,
             fibonacci_result,applied_at) VALUES (?,?,?,?,?,?,?,toTimestamp(now()))""")

    def select_records(
        self, *, dataset_id: str, record_count: int, seed: int
    ) -> Sequence[WorkloadRecord]:
        metadata = self.session.execute(
            """SELECT record_count,bucket_count FROM datasets
            WHERE dataset_id=%s""",
            (dataset_id,),
        ).one()
        if metadata is None or record_count > metadata.record_count:
            return []
        ids = random.Random(seed).sample(range(metadata.record_count), record_count)
        arguments = [
            (dataset_id, identifier % metadata.bucket_count, identifier) for identifier in ids
        ]
        results = execute_concurrent_with_args(
            self.session,
            self._record,
            arguments,
            concurrency=self.config.read_concurrency,
            raise_on_first_error=True,
        )
        return [
            WorkloadRecord(
                row.dataset_id, row.bucket, row.record_id, row.input_number, row.checksum
            )
            for success, result in results
            for row in result
            if success
        ]

    def apply_once(
        self, *, record: WorkloadRecord, operation_id: UUID, fibonacci_result: str
    ) -> tuple[bool, int]:
        state = self._read_record(record)
        if state.last_operation_id == operation_id:
            self._write_marker(record, operation_id, fibonacci_result, state.checksum)
            return True, state.checksum
        existing = self._find_operation(record, operation_id)
        if existing is not None and state.checksum == existing:
            return True, existing
        new_checksum = record.checksum + 1
        if state.pending_operation_id != operation_id:
            if self.before_reserve is not None:
                self.before_reserve(record, operation_id)
            reserved = self.session.execute(
                self._reserve,
                (operation_id, record.dataset_id, record.bucket, record.record_id, record.checksum),
            ).one()
            if not reserved.applied:
                return False, state.checksum
        values = (
            fibonacci_result,
            new_checksum,
            operation_id,
            record.dataset_id,
            record.bucket,
            record.record_id,
            operation_id,
            record.checksum,
        )
        try:
            applied = self.session.execute(self._finish, values).one()
            if self.after_finalize is not None:
                self.after_finalize(record, operation_id)
            if not applied.applied:
                return False, record.checksum
        except Exception:
            state = self._read_record(record)
            if state.last_operation_id != operation_id:
                raise
            new_checksum = state.checksum
        self._write_marker(record, operation_id, fibonacci_result, new_checksum)
        return True, new_checksum

    def _read_record(self, record: WorkloadRecord) -> object:
        return self.session.execute(
            self._record, (record.dataset_id, record.bucket, record.record_id)
        ).one()

    def _write_marker(
        self, record: WorkloadRecord, operation_id: UUID, result: str, checksum: int
    ) -> None:
        self.session.execute(
            self._mark,
            (
                record.dataset_id,
                record.bucket,
                record.record_id,
                operation_id,
                checksum - 1,
                checksum,
                result,
            ),
        )

    def _find_operation(self, record: WorkloadRecord, operation_id: UUID) -> int | None:
        row = self.session.execute(
            self._operation, (record.dataset_id, record.bucket, record.record_id, operation_id)
        ).one()
        return row.new_checksum if row else None

    def close(self) -> None:
        self.session.shutdown()
        self.cluster.shutdown()
