from pathlib import Path

import pytest

from job_visibility.testing import ComposeOutageController


@pytest.mark.parametrize("project", ["default", "job-visibility-", "other-resilience", "../bad"])
def test_outage_controller_rejects_unscoped_projects(project: str) -> None:
    with pytest.raises(ValueError, match="job-visibility"):
        ComposeOutageController(project)


def test_outage_controller_rejects_unapproved_services() -> None:
    controller = ComposeOutageController("job-visibility-test", workdir=Path.cwd())

    with pytest.raises(ValueError, match="not approved"):
        controller.stop("schema-registry")
