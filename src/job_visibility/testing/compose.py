from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

from .polling import poll_until

_PROJECT = re.compile(r"^job-visibility-[a-z0-9-]+$")
_SERVICES = {"scheduler-postgres", "edr-postgres", "kafka"}


class ComposeOutageController:
    """Explicitly scoped process control for destructive outage acceptance tests."""

    def __init__(self, project: str, *, workdir: Path | None = None) -> None:
        if not _PROJECT.fullmatch(project):
            raise ValueError("outage tests require a job-visibility-* Compose project")
        self.project = project
        self.workdir = workdir or Path.cwd()

    def stop(self, service: str) -> None:
        self._service(service)
        self._run("stop", "--timeout", "10", service)

    def start(self, service: str, *, timeout_seconds: float = 120) -> None:
        self._service(service)
        self._run("start", service)
        poll_until(
            lambda: self.state(service),
            lambda state: (
                state.get("Status") == "running"
                and state.get("Health", {}).get("Status", "healthy") == "healthy"
            ),
            description=f"Compose service {service} to become healthy",
            timeout_seconds=timeout_seconds,
            interval_seconds=1,
            retry_exceptions=(subprocess.SubprocessError, json.JSONDecodeError),
        )

    def state(self, service: str) -> dict[str, Any]:
        self._service(service)
        container_id = self._run("ps", "-q", service).stdout.strip()
        if not container_id:
            return {"Status": "missing"}
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{json .State}}", container_id],
            cwd=self.workdir,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(result.stdout)

    def _run(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["docker", "compose", "--project-name", self.project, *arguments],
            cwd=self.workdir,
            check=True,
            capture_output=True,
            text=True,
        )

    @staticmethod
    def _service(service: str) -> None:
        if service not in _SERVICES:
            raise ValueError(f"service is not approved for outage tests: {service}")
