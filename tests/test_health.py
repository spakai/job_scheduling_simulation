from datetime import UTC, datetime, timedelta

from job_visibility.observability import HealthService


def test_queue_age_marks_stale_backlog_unhealthy() -> None:
    result = HealthService.evaluate_queue(
        {"count": 2, "oldest": datetime.now(UTC) - timedelta(seconds=11)},
        threshold_seconds=10,
    )

    assert result["healthy"] is False
    assert result["ageSeconds"] >= 11


def test_empty_queue_is_healthy() -> None:
    result = HealthService.evaluate_queue({"count": 0, "oldest": None}, threshold_seconds=10)

    assert result["healthy"] is True
    assert result["ageSeconds"] == 0


def test_failed_connector_task_is_not_healthy() -> None:
    result = HealthService._dependency(
        lambda: {
            "connector": {"state": "RUNNING"},
            "tasks": [{"id": 0, "state": "FAILED"}],
        }
    )

    assert isinstance(result, dict)
    assert result["healthy"] is False
