"""Strictly scoped Docker resource controls for Spec 005 experiments."""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

_PROJECT = re.compile(r"^job-visibility-chaos-[a-z0-9-]+$")
_SERVICES = {
    "scheduler-api",
    "visibility-api",
    "scheduler-worker",
    "outbox-publisher",
    "projector",
    "scheduler-postgres",
    "edr-postgres",
    "kafka-connect",
}
_CPU_PRESSURE = (
    "import time\n"
    "end=time.monotonic()+DURATION\n"
    "while time.monotonic()<end:\n"
    "  sum(i*i for i in range(10000))\n"
)
_MEMORY_PRESSURE = (
    "import time\n"
    "chunks=[]\n"
    "for _ in range(MEGABYTES): chunks.append(bytearray(1024*1024))\n"
    "time.sleep(DURATION)\n"
)


class ResourcePressureController:
    """Apply finite pressure only to allowlisted services in chaos-named projects."""

    def __init__(
        self,
        project: str,
        *,
        workdir: Path | None = None,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        if not _PROJECT.fullmatch(project):
            raise ValueError("resource pressure requires job-visibility-chaos-* Compose project")
        self.project = project
        self.workdir = workdir or Path.cwd()
        self.run = run

    def cpu(self, service: str, *, cpus: float, duration_seconds: int) -> None:
        if not 0.1 <= cpus <= 4:
            raise ValueError("cpus must be between 0.1 and 4")
        self._duration(duration_seconds)
        container = self._container(service)
        self._docker("update", "--cpus", str(cpus), container)
        code = _CPU_PRESSURE.replace("DURATION", str(duration_seconds))
        self._docker("exec", "-d", container, "python", "-c", code)

    def memory(self, service: str, *, megabytes: int, duration_seconds: int) -> None:
        if not 16 <= megabytes <= 1024:
            raise ValueError("memory pressure must be between 16 and 1024 MiB")
        self._duration(duration_seconds)
        container = self._container(service)
        code = _MEMORY_PRESSURE.replace("MEGABYTES", str(megabytes)).replace(
            "DURATION", str(duration_seconds)
        )
        self._docker("exec", "-d", container, "python", "-c", code)

    def disk(self, service: str, *, megabytes: int) -> None:
        if service not in {"scheduler-postgres", "edr-postgres"}:
            raise ValueError("disk pressure is restricted to disposable chaos PostgreSQL volumes")
        if not 1 <= megabytes <= 512:
            raise ValueError("disk pressure must be between 1 and 512 MiB")
        container = self._container(service)
        self._docker(
            "exec",
            container,
            "dd",
            "if=/dev/zero",
            "of=/var/lib/postgresql/data/.chaos-pressure",
            "bs=1M",
            f"count={megabytes}",
            "conv=fsync",
        )

    def cleanup(self, service: str) -> None:
        container = self._container(service)
        self._docker("update", "--cpus", "0", container, check=False)
        if service in {"scheduler-postgres", "edr-postgres"}:
            self._docker(
                "exec",
                container,
                "rm",
                "-f",
                "/var/lib/postgresql/data/.chaos-pressure",
                check=False,
            )

    def stats(self, service: str) -> dict[str, Any]:
        container = self._container(service)
        result = self._docker("stats", "--no-stream", "--format", "{{json .}}", container)
        return json.loads(result.stdout)

    def _container(self, service: str) -> str:
        self._service(service)
        result = self._compose("ps", "-q", service)
        container = result.stdout.strip()
        if not container:
            raise RuntimeError(f"chaos target is not running: {service}")
        return container

    def _compose(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return self.run(
            ["docker", "compose", "--project-name", self.project, *arguments],
            cwd=self.workdir,
            check=True,
            capture_output=True,
            text=True,
        )

    def _docker(self, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self.run(
            ["docker", *arguments],
            cwd=self.workdir,
            check=check,
            capture_output=True,
            text=True,
        )

    @staticmethod
    def _duration(value: int) -> None:
        if not 1 <= value <= 60:
            raise ValueError("pressure duration must be between 1 and 60 seconds")

    @staticmethod
    def _service(service: str) -> None:
        if service not in _SERVICES:
            raise ValueError(f"service is not approved for resource pressure: {service}")
