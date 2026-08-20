import pytest

from job_visibility.testing import PollTimeout, poll_until


class FakeTime:
    def __init__(self) -> None:
        self.value = 0.0

    def clock(self) -> float:
        return self.value

    def wait(self, seconds: float) -> None:
        self.value += seconds


def test_poll_returns_last_accepted_value() -> None:
    values = iter((0, 1, 2))
    fake = FakeTime()

    result = poll_until(
        lambda: next(values),
        lambda value: value == 2,
        description="value two",
        timeout_seconds=1,
        interval_seconds=0.1,
        clock=fake.clock,
        wait=fake.wait,
    )

    assert result == 2


def test_poll_timeout_reports_condition_elapsed_attempts_and_last_value() -> None:
    fake = FakeTime()

    with pytest.raises(PollTimeout) as raised:
        poll_until(
            lambda: {"status": "starting"},
            lambda value: value["status"] == "ready",
            description="connector ready",
            timeout_seconds=0.25,
            interval_seconds=0.1,
            clock=fake.clock,
            wait=fake.wait,
        )

    assert raised.value.last_value == {"status": "starting"}
    assert raised.value.attempts == 4
    assert "connector ready" in str(raised.value)
    assert "starting" in str(raised.value)


def test_poll_can_retry_declared_transient_exceptions() -> None:
    fake = FakeTime()
    attempts = 0

    def observe() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("dependency reconnecting")
        return "ready"

    result = poll_until(
        observe,
        lambda value: value == "ready",
        description="dependency recovery",
        timeout_seconds=1,
        interval_seconds=0.1,
        clock=fake.clock,
        wait=fake.wait,
        retry_exceptions=(ConnectionError,),
    )

    assert result == "ready"
    assert attempts == 3


@pytest.mark.parametrize("timeout,interval", [(0, 0.1), (1, 0)])
def test_poll_rejects_non_positive_bounds(timeout: float, interval: float) -> None:
    with pytest.raises(ValueError):
        poll_until(
            lambda: True,
            bool,
            description="valid bounds",
            timeout_seconds=timeout,
            interval_seconds=interval,
        )
