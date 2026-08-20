from dataclasses import dataclass
from uuid import UUID

from job_visibility.scheduler import CassandraDriverClient, WorkloadRecord


@dataclass
class State:
    checksum: int
    pending_operation_id: UUID | None
    last_operation_id: UUID | None


class Result:
    def __init__(self, row: object) -> None:
        self.row = row

    def one(self) -> object:
        return self.row


def client_with(session: object) -> CassandraDriverClient:
    client = object.__new__(CassandraDriverClient)
    client.session = session  # type: ignore[assignment]
    client._record = "record"
    client._operation = "operation"
    client._reserve = "reserve"
    client._finish = "finish"
    client._mark = "mark"
    return client


def test_lost_finalization_response_reconciles_without_second_increment() -> None:
    operation = UUID("00000000-0000-0000-0000-000000000003")

    class Session:
        record_reads = 0
        markers: list[tuple[object, ...]] = []

        def execute(self, statement: object, values: tuple[object, ...]) -> Result:
            if statement == "record":
                self.record_reads += 1
                state = (
                    State(7, None, None) if self.record_reads == 1 else State(8, None, operation)
                )
                return Result(state)
            if statement == "operation":
                return Result(None)
            if statement == "reserve":
                return Result(type("Applied", (), {"applied": True})())
            if statement == "finish":
                raise TimeoutError("response lost after apply")
            assert statement == "mark"
            self.markers.append(values)
            return Result(None)

    session = Session()
    client = client_with(session)
    applied, checksum = client.apply_once(
        record=WorkloadRecord("dataset", 0, 1, 20, 7),
        operation_id=operation,
        fibonacci_result="6765",
    )

    assert applied is True
    assert checksum == 8
    assert session.record_reads == 2
    assert len(session.markers) == 1
    assert session.markers[0][5] == 8


def test_conditional_reservation_conflict_never_blindly_finishes() -> None:
    operation = UUID("00000000-0000-0000-0000-000000000004")

    class Session:
        statements: list[object] = []

        def execute(self, statement: object, values: tuple[object, ...]) -> Result:
            self.statements.append(statement)
            if statement == "record":
                return Result(State(7, None, None))
            if statement == "operation":
                return Result(None)
            if statement == "reserve":
                return Result(type("Applied", (), {"applied": False})())
            raise AssertionError("finalization must not run after a lost reservation")

    session = Session()
    applied, checksum = client_with(session).apply_once(
        record=WorkloadRecord("dataset", 0, 1, 20, 7),
        operation_id=operation,
        fibonacci_result="6765",
    )

    assert applied is False
    assert checksum == 7
    assert "finish" not in session.statements
