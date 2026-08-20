"""Infrastructure-free projection repository used by simulations and contract tests."""

from __future__ import annotations

from copy import deepcopy

from job_visibility.engine import JobNotFoundError, VersionConflictError
from job_visibility.model import JobVisibility


class InMemoryVisibilityRepository:
    def __init__(self) -> None:
        self._jobs: dict[str, JobVisibility] = {}

    def get(self, job_id: str, *, for_update: bool = False) -> JobVisibility | None:
        del for_update
        job = self._jobs.get(job_id)
        return deepcopy(job) if job is not None else None

    def require(self, job_id: str) -> JobVisibility:
        job = self.get(job_id)
        if job is None:
            raise JobNotFoundError(job_id)
        return job

    def save(self, job: JobVisibility, *, expected_version: int | None = None) -> None:
        current = self._jobs.get(job.job_id)
        actual = current.version if current else 0
        if expected_version is not None and actual != expected_version:
            raise VersionConflictError(
                f"job {job.job_id}: expected version {expected_version}, actual {actual}"
            )
        self._jobs[job.job_id] = deepcopy(job)

    def job_ids(self) -> list[str]:
        return sorted(self._jobs)
