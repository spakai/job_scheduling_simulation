import pytest

from job_visibility.scenarios import SCENARIOS


@pytest.mark.parametrize("scenario_id", list(SCENARIOS))
def test_scenario(scenario_id: str) -> None:
    result = SCENARIOS[scenario_id]()
    failures = [assertion.name for assertion in result.assertions if not assertion.passed]
    assert result.result == "PASS", f"{scenario_id}: {failures}"
