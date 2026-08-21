from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from job_visibility.testing import ResourcePressureController


class RecordingRunner:
    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def __call__(self, command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        self.commands.append(command)
        if "ps" in command:
            output = "container-123\n"
        elif "stats" in command:
            output = json.dumps({"CPUPerc": "25.0%", "MemUsage": "32MiB / 128MiB"})
        else:
            output = ""
        return subprocess.CompletedProcess(command, 0, output, "")


def test_pressure_controller_rejects_broad_projects_and_targets() -> None:
    with pytest.raises(ValueError, match="job-visibility-chaos"):
        ResourcePressureController("job-visibility-resilience")
    controller = ResourcePressureController("job-visibility-chaos-test", run=RecordingRunner())
    with pytest.raises(ValueError, match="not approved"):
        controller.cpu("host", cpus=1, duration_seconds=5)
    with pytest.raises(ValueError, match="disposable"):
        controller.disk("scheduler-api", megabytes=10)


def test_cpu_and_memory_pressure_are_finite_and_container_scoped() -> None:
    runner = RecordingRunner()
    controller = ResourcePressureController(
        "job-visibility-chaos-test", workdir=Path("/workspace"), run=runner
    )

    controller.cpu("scheduler-worker", cpus=0.25, duration_seconds=10)
    controller.memory("visibility-api", megabytes=64, duration_seconds=5)

    assert ["docker", "update", "--cpus", "0.25", "container-123"] in runner.commands
    assert all("container-123" in command for command in runner.commands if "exec" in command)
    assert any("time.sleep(5)" in part for command in runner.commands for part in command)


def test_disk_pressure_uses_fixed_file_and_bounded_size() -> None:
    runner = RecordingRunner()
    controller = ResourcePressureController("job-visibility-chaos-disk", run=runner)

    controller.disk("edr-postgres", megabytes=32)
    controller.cleanup("edr-postgres")

    flattened = [part for command in runner.commands for part in command]
    assert "of=/var/lib/postgresql/data/.chaos-pressure" in flattened
    assert "count=32" in flattened
    assert "/var/lib/postgresql/data/.chaos-pressure" in flattened


def test_stats_are_parsed_for_evidence() -> None:
    controller = ResourcePressureController("job-visibility-chaos-stats", run=RecordingRunner())

    assert controller.stats("projector")["CPUPerc"] == "25.0%"
