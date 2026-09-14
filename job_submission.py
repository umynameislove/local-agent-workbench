from __future__ import annotations

import hashlib
import string
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from db import (
    JobAlreadyExistsError,
    JobRecord,
    JobRepository,
    JobRepositoryError,
    ProjectNotFoundError,
    ProjectRecord,
    ProjectRepository,
    ProjectRepositoryError,
)
from engine import (
    CONFIG_VERSION,
    JobCreate,
    JobRuntime,
    ProcessRunnerError,
    ProjectConfig,
    run_process,
)

PROJECT_POLICY_VERSION = 1
_ALLOWED_FIELDS = frozenset({"project_id", "request", "runtime", "model"})
_REQUIRED_FIELDS = frozenset({"project_id", "request"})
_MAX_REQUEST_BYTES = 262_144


class JobSubmissionError(RuntimeError):
    """Base error for sanitized job submission failures."""


class JobSubmissionValidationError(JobSubmissionError):
    """Raised when a client submission violates the public contract."""


class JobSubmissionNotFoundError(JobSubmissionError):
    """Raised when the selected project does not exist."""


class JobSubmissionConflictError(JobSubmissionError):
    """Raised when current project state cannot produce a coherent job."""


class JobSubmissionUnavailableError(JobSubmissionError):
    """Raised when required local infrastructure is unavailable."""


@dataclass(frozen=True)
class JobSubmission:
    project_id: str
    request: str
    runtime: JobRuntime = JobRuntime.AUTO
    model: str | None = None

    @classmethod
    def from_payload(cls, payload: object) -> JobSubmission:
        if not isinstance(payload, Mapping) or any(not isinstance(key, str) for key in payload):
            raise JobSubmissionValidationError("Job submission must be an object.")
        fields = set(payload)
        if missing := _REQUIRED_FIELDS - fields:
            names = ", ".join(sorted(missing))
            raise JobSubmissionValidationError(f"Job submission requires {names}.")
        if fields - _ALLOWED_FIELDS:
            raise JobSubmissionValidationError("Job submission contains unsupported fields.")

        project_id = payload["project_id"]
        request = payload["request"]
        runtime_value = payload.get("runtime", JobRuntime.AUTO.value)
        model = payload.get("model")
        if (
            not isinstance(project_id, str)
            or not project_id
            or project_id != project_id.strip()
            or "\x00" in project_id
        ):
            raise JobSubmissionValidationError("Job project_id is invalid.")
        if (
            not isinstance(request, str)
            or not request.strip()
            or "\x00" in request
            or len(request.encode("utf-8")) > _MAX_REQUEST_BYTES
        ):
            raise JobSubmissionValidationError("Job request is invalid.")
        if not isinstance(runtime_value, str):
            raise JobSubmissionValidationError("Job runtime is invalid.")
        try:
            runtime = JobRuntime(runtime_value)
        except ValueError:
            raise JobSubmissionValidationError("Job runtime is invalid.") from None
        if model is not None and (
            not isinstance(model, str) or not model or model != model.strip() or "\x00" in model
        ):
            raise JobSubmissionValidationError("Job model is invalid.")
        return cls(project_id=project_id, request=request, runtime=runtime, model=model)


class JobSubmissionService:
    """Create one durable job from server observed project and Git state."""

    def __init__(
        self,
        projects: ProjectRepository,
        jobs: JobRepository,
        *,
        active_project_ids: frozenset[str],
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        if not isinstance(projects, ProjectRepository) or not isinstance(jobs, JobRepository):
            raise TypeError("Job submission requires project and job repositories.")
        if id_factory is not None and not callable(id_factory):
            raise TypeError("Job id factory must be callable.")
        if not isinstance(active_project_ids, frozenset) or any(
            not isinstance(project_id, str) for project_id in active_project_ids
        ):
            raise TypeError("Active project ids must be a frozenset of strings.")
        self._projects = projects
        self._jobs = jobs
        self._active_project_ids = active_project_ids
        self._id_factory = id_factory or (lambda: f"job-{uuid4().hex}")

    async def submit(self, payload: object) -> JobRecord:
        submission = JobSubmission.from_payload(payload)
        project = self._load_project(submission.project_id)
        repo_head = await self._read_repo_head(Path(project.root))
        snapshot = {
            "config_version": CONFIG_VERSION,
            "model": submission.model,
            "policy_version": PROJECT_POLICY_VERSION,
            "project": self._project_snapshot(project.to_config()),
            "prompt_hash": f"sha256:{hashlib.sha256(submission.request.encode()).hexdigest()}",
            "repo_head": repo_head,
            "runtime": submission.runtime.value,
        }
        job = JobCreate(
            id=self._id_factory(),
            project_id=submission.project_id,
            request=submission.request,
            request_snapshot=snapshot,
            runtime=submission.runtime,
            model=submission.model,
        )
        try:
            return self._jobs.create(job)
        except JobAlreadyExistsError as error:
            raise JobSubmissionConflictError("Job identity already exists.") from error
        except JobRepositoryError as error:
            raise JobSubmissionUnavailableError("Job storage is unavailable.") from error

    def _load_project(self, project_id: str) -> ProjectRecord:
        if project_id not in self._active_project_ids:
            raise JobSubmissionNotFoundError("Project does not exist.")
        try:
            return self._projects.get(project_id)
        except ProjectNotFoundError as error:
            raise JobSubmissionNotFoundError("Project does not exist.") from error
        except ProjectRepositoryError as error:
            raise JobSubmissionUnavailableError("Project storage is unavailable.") from error

    @staticmethod
    async def _read_repo_head(root: Path) -> str:
        try:
            result = await run_process(
                ("git", "rev-parse", "--verify", "HEAD^{commit}"),
                cwd=root,
                timeout=10,
            )
        except ValueError as error:
            raise JobSubmissionConflictError("Project repository is unavailable.") from error
        except ProcessRunnerError as error:
            raise JobSubmissionUnavailableError("Git inspection is unavailable.") from error
        if result.returncode != 0:
            raise JobSubmissionConflictError("Project repository has no readable commit.")
        try:
            head = result.stdout.decode("ascii").strip()
        except UnicodeDecodeError:
            raise JobSubmissionConflictError(
                "Project repository returned an invalid commit."
            ) from None
        if len(head) not in {40, 64} or any(
            character not in string.hexdigits for character in head
        ):
            raise JobSubmissionConflictError("Project repository returned an invalid commit.")
        return head.lower()

    @staticmethod
    def _project_snapshot(project: ProjectConfig) -> dict[str, object]:
        return {
            "cloud_allowed": project.cloud_allowed,
            "id": project.id,
            "permission_mode": project.permission_mode.value,
            "root": project.root,
            "sensitivity": project.sensitivity.value,
        }
