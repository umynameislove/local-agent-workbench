from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from engine import (
    TERMINAL_JOB_STATES,
    ApprovalCreate,
    ApprovalDecision,
    ApprovalResolution,
    EventCreate,
    ForbiddenJobTransitionError,
    JobCreate,
    JobRuntime,
    JobState,
    JobUpdate,
    MemoryReferenceCreate,
    PermissionMode,
    PlannerItemCreate,
    PlannerItemKind,
    ProjectConfig,
    RecoveryAction,
    RecoveryIssue,
    Sensitivity,
    UsageCreate,
    validate_job_transition,
)


class DatabaseError(RuntimeError):
    """Base error for safe database layer failures."""


class MigrationDefinitionError(DatabaseError):
    """Raised when embedded migrations are not a valid sequence."""


class DatabaseVersionError(DatabaseError):
    """Raised when stored schema metadata is invalid or unsupported."""


class MigrationApplyError(DatabaseError):
    """Raised when a migration cannot be applied atomically."""


class ProjectRepositoryError(DatabaseError):
    """Base error for safe project persistence failures."""


class ProjectValidationError(ProjectRepositoryError):
    """Raised when a project violates the persistence contract."""


class ProjectAlreadyExistsError(ProjectRepositoryError):
    """Raised when a project id or root is already registered."""


class ProjectNotFoundError(ProjectRepositoryError):
    """Raised when a requested project does not exist."""


class JobRepositoryError(DatabaseError):
    """Base error for safe job persistence failures."""


class JobValidationError(JobRepositoryError):
    """Raised when a job violates the persistence contract."""


class JobAlreadyExistsError(JobRepositoryError):
    """Raised when a job id is already registered."""


class JobNotFoundError(JobRepositoryError):
    """Raised when a requested job does not exist."""


class EventRepositoryError(DatabaseError):
    """Base error for safe event persistence failures."""


class EventValidationError(EventRepositoryError):
    """Raised when an event violates the persistence contract."""


class EventNotFoundError(EventRepositoryError):
    """Raised when a requested event does not exist."""


class EventIdempotencyConflictError(EventRepositoryError):
    """Raised when an idempotency key is reused for a different event."""


class AtomicTransitionError(DatabaseError):
    """Base error for atomic job transition failures."""


class AtomicTransitionValidationError(AtomicTransitionError):
    """Raised when an atomic transition request is invalid."""


class AtomicTransitionConflictError(AtomicTransitionError):
    """Raised when an atomic transition conflicts with committed state."""


class AtomicTransitionStateError(AtomicTransitionConflictError):
    """Raised when a job state change is outside the lifecycle contract."""


class ApprovalRepositoryError(DatabaseError):
    """Base error for safe approval persistence failures."""


class ApprovalValidationError(ApprovalRepositoryError):
    """Raised when approval data violates the persistence contract."""


class ApprovalAlreadyExistsError(ApprovalRepositoryError):
    """Raised when an approval id is already registered."""


class ApprovalNotFoundError(ApprovalRepositoryError):
    """Raised when a requested approval does not exist."""


class ApprovalExpiredError(ApprovalRepositoryError):
    """Raised when an expired approval is resolved."""


class ApprovalPayloadMismatchError(ApprovalRepositoryError):
    """Raised when the current payload differs from the approval request."""


class ApprovalDecisionConflictError(ApprovalRepositoryError):
    """Raised when a completed approval receives a conflicting retry."""


class PlannerRepositoryError(DatabaseError):
    """Base error for safe planner persistence failures."""


class PlannerValidationError(PlannerRepositoryError):
    """Raised when planner data violates the persistence contract."""


class PlannerAlreadyExistsError(PlannerRepositoryError):
    """Raised when a planner item id is already registered."""


class PlannerNotFoundError(PlannerRepositoryError):
    """Raised when a requested planner item does not exist."""


class PlannerPromotionConflictError(PlannerRepositoryError):
    """Raised when a planner item receives a conflicting promotion."""


class UsageRepositoryError(DatabaseError):
    """Base error for safe usage persistence failures."""


class UsageValidationError(UsageRepositoryError):
    """Raised when usage data violates the persistence contract."""


class UsageNotFoundError(UsageRepositoryError):
    """Raised when a requested usage observation does not exist."""


class MemoryReferenceRepositoryError(DatabaseError):
    """Base error for safe projmem reference persistence failures."""


class MemoryReferenceValidationError(MemoryReferenceRepositoryError):
    """Raised when a projmem reference violates the persistence contract."""


class MemoryReferenceNotFoundError(MemoryReferenceRepositoryError):
    """Raised when a requested projmem reference does not exist."""


class RecoveryServiceError(DatabaseError):
    """Raised when durable restart state cannot be loaded safely."""


class BackupServiceError(DatabaseError):
    """Raised when a verified database backup cannot be created safely."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    statements: tuple[str, ...]


@dataclass(frozen=True)
class ProjectRecord:
    id: str
    root: str
    sensitivity: Sensitivity
    cloud_allowed: bool
    permission_mode: PermissionMode
    created_at: str
    updated_at: str

    def to_config(self) -> ProjectConfig:
        return ProjectConfig(
            id=self.id,
            root=self.root,
            sensitivity=self.sensitivity,
            cloud_allowed=self.cloud_allowed,
            permission_mode=self.permission_mode,
        )


@dataclass(frozen=True)
class JobRecord:
    id: str
    project_id: str
    request: str
    request_snapshot: dict[str, Any]
    state: JobState
    runtime: JobRuntime
    model: str | None
    worktree_path: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class EventRecord:
    id: int
    job_id: str
    sequence: int
    event_type: str
    payload: dict[str, Any]
    payload_hash: str
    idempotency_key: str | None
    created_at: str


@dataclass(frozen=True)
class AtomicTransitionRecord:
    job: JobRecord
    event: EventRecord


@dataclass(frozen=True)
class ApprovalRecord:
    id: str
    job_id: str
    payload_hash: str
    decision: ApprovalDecision | None
    actor: str | None
    channel: str | None
    expires_at: datetime
    created_at: str
    decided_at: str | None


@dataclass(frozen=True)
class RecoveryRecord:
    job: JobRecord
    action: RecoveryAction
    worktree: Path | None
    approval: ApprovalRecord | None = None
    issue: RecoveryIssue | None = None


@dataclass(frozen=True)
class PlannerItemRecord:
    id: str
    kind: PlannerItemKind
    title: str
    project_id: str | None
    details: str | None
    due_at: datetime | None
    source: str | None
    source_key: str | None
    promoted_job_id: str | None
    created_at: str
    updated_at: str
    promoted_at: str | None


@dataclass(frozen=True)
class PlannerPromotionRecord:
    item: PlannerItemRecord
    job: JobRecord


@dataclass(frozen=True)
class UsageRecord:
    id: int
    provider: str
    job_id: str | None
    model: str | None
    input_tokens: int | None
    output_tokens: int | None
    cost_usd: Decimal | None
    latency_ms: int | None
    quota_limit: Decimal | None
    quota_remaining: Decimal | None
    quota_unit: str | None
    quota_reset_at: datetime | None
    rate_limited: bool | None
    recorded_at: str


@dataclass(frozen=True)
class MemoryReferenceRecord:
    id: int
    projmem_record_id: str
    job_id: str
    event_id: int | None
    created_at: str


MIGRATIONS = (
    Migration(
        version=1,
        name="initialize_schema_version",
        statements=(
            """
            CREATE TABLE schema_version (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                version INTEGER NOT NULL CHECK (version >= 0),
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "INSERT INTO schema_version (singleton, version) VALUES (1, 0)",
        ),
    ),
    Migration(
        version=2,
        name="create_projects",
        statements=(
            """
            CREATE TABLE projects (
                id TEXT PRIMARY KEY
                    CHECK (length(id) > 0 AND id = trim(id)),
                root TEXT NOT NULL UNIQUE
                    CHECK (length(root) > 0 AND root = trim(root)),
                sensitivity TEXT NOT NULL
                    CHECK (sensitivity IN ('public', 'private', 'internal', 'restricted')),
                cloud_allowed INTEGER NOT NULL
                    CHECK (cloud_allowed IN (0, 1)),
                permission_mode TEXT NOT NULL
                    CHECK (permission_mode IN ('read-only', 'sandboxed-write', 'never')),
                created_at TEXT NOT NULL
                    DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL
                    DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
                CHECK (
                    sensitivity NOT IN ('internal', 'restricted')
                    OR cloud_allowed = 0
                )
            )
            """,
        ),
    ),
    Migration(
        version=3,
        name="create_jobs",
        statements=(
            """
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY
                    CHECK (length(id) > 0 AND id = trim(id) AND instr(id, char(0)) = 0),
                project_id TEXT NOT NULL
                    REFERENCES projects(id) ON DELETE RESTRICT,
                request TEXT NOT NULL
                    CHECK (length(trim(request)) > 0 AND instr(request, char(0)) = 0),
                request_snapshot TEXT NOT NULL
                    CHECK (
                        json_valid(request_snapshot)
                        AND json_type(request_snapshot) = 'object'
                    ),
                state TEXT NOT NULL CHECK (state IN (
                    'created', 'classified', 'planning', 'queued', 'running',
                    'waiting_input', 'waiting_approval', 'verifying', 'review_ready',
                    'approved', 'rejected', 'applying', 'completed', 'failed',
                    'blocked', 'cancelled'
                )),
                runtime TEXT NOT NULL
                    CHECK (runtime IN ('auto', 'claude', 'codex', 'local')),
                model TEXT CHECK (
                    model IS NULL OR (
                        length(model) > 0 AND model = trim(model)
                        AND instr(model, char(0)) = 0
                    )
                ),
                worktree_path TEXT CHECK (
                    worktree_path IS NULL OR (
                        length(worktree_path) > 0 AND worktree_path = trim(worktree_path)
                        AND instr(worktree_path, char(0)) = 0
                    )
                ),
                created_at TEXT NOT NULL
                    DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL
                    DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
            """,
            "CREATE INDEX jobs_project_created_idx ON jobs (project_id, created_at, id)",
        ),
    ),
    Migration(
        version=4,
        name="create_events",
        statements=(
            """
            CREATE TABLE events (
                id INTEGER PRIMARY KEY CHECK (id > 0),
                job_id TEXT NOT NULL
                    REFERENCES jobs(id) ON DELETE RESTRICT,
                sequence INTEGER NOT NULL CHECK (sequence > 0),
                event_type TEXT NOT NULL CHECK (
                    length(event_type) > 0
                    AND length(CAST(event_type AS BLOB)) <= 128
                    AND event_type = trim(event_type)
                    AND instr(event_type, char(0)) = 0
                ),
                payload TEXT NOT NULL CHECK (
                    json_valid(payload)
                    AND json_type(payload) = 'object'
                    AND length(CAST(payload AS BLOB)) <= 1048576
                ),
                payload_hash TEXT NOT NULL CHECK (
                    length(payload_hash) = 64
                    AND payload_hash NOT GLOB '*[^0-9a-f]*'
                ),
                created_at TEXT NOT NULL
                    DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
                UNIQUE (job_id, sequence)
            )
            """,
            """
            CREATE TRIGGER events_prevent_update
            BEFORE UPDATE ON events
            BEGIN
                SELECT RAISE(ABORT, 'Events are append only.');
            END
            """,
            """
            CREATE TRIGGER events_prevent_delete
            BEFORE DELETE ON events
            BEGIN
                SELECT RAISE(ABORT, 'Events are append only.');
            END
            """,
        ),
    ),
    Migration(
        version=5,
        name="add_event_idempotency",
        statements=(
            """
            ALTER TABLE events ADD COLUMN idempotency_key TEXT CHECK (
                idempotency_key IS NULL OR (
                    length(idempotency_key) > 0
                    AND length(CAST(idempotency_key AS BLOB)) <= 128
                    AND idempotency_key = trim(idempotency_key)
                    AND instr(idempotency_key, char(0)) = 0
                )
            )
            """,
            """
            CREATE UNIQUE INDEX events_job_idempotency_idx
            ON events (job_id, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            """,
        ),
    ),
    Migration(
        version=6,
        name="create_approvals",
        statements=(
            """
            CREATE TABLE approvals (
                id TEXT PRIMARY KEY CHECK (
                    length(id) > 0
                    AND length(CAST(id AS BLOB)) <= 128
                    AND id = trim(id)
                    AND instr(id, char(0)) = 0
                ),
                job_id TEXT NOT NULL
                    REFERENCES jobs(id) ON DELETE RESTRICT,
                payload_hash TEXT NOT NULL CHECK (
                    length(payload_hash) = 64
                    AND payload_hash NOT GLOB '*[^0-9a-f]*'
                ),
                decision TEXT CHECK (
                    decision IS NULL OR decision IN (
                        'approved', 'rejected', 'changes_requested'
                    )
                ),
                actor TEXT CHECK (
                    actor IS NULL OR (
                        length(actor) > 0
                        AND length(CAST(actor AS BLOB)) <= 128
                        AND actor = trim(actor)
                        AND instr(actor, char(0)) = 0
                    )
                ),
                channel TEXT CHECK (
                    channel IS NULL OR (
                        length(channel) > 0
                        AND length(CAST(channel AS BLOB)) <= 64
                        AND channel = trim(channel)
                        AND instr(channel, char(0)) = 0
                    )
                ),
                expires_at INTEGER NOT NULL CHECK (expires_at > 0),
                created_at TEXT NOT NULL
                    DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
                decided_at TEXT,
                CHECK (
                    (
                        decision IS NULL AND actor IS NULL
                        AND channel IS NULL AND decided_at IS NULL
                    ) OR (
                        decision IS NOT NULL AND actor IS NOT NULL
                        AND channel IS NOT NULL AND decided_at IS NOT NULL
                    )
                )
            )
            """,
            """
            CREATE INDEX approvals_job_created_idx
            ON approvals (job_id, created_at, id)
            """,
            """
            CREATE TRIGGER approvals_require_pending_insert
            BEFORE INSERT ON approvals
            WHEN NEW.decision IS NOT NULL
                OR NEW.actor IS NOT NULL
                OR NEW.channel IS NOT NULL
                OR NEW.decided_at IS NOT NULL
            BEGIN
                SELECT RAISE(ABORT, 'Approval must begin pending.');
            END
            """,
            """
            CREATE TRIGGER approvals_validate_decision
            BEFORE UPDATE ON approvals
            WHEN OLD.decision IS NOT NULL
                OR NEW.id IS NOT OLD.id
                OR NEW.job_id IS NOT OLD.job_id
                OR NEW.payload_hash IS NOT OLD.payload_hash
                OR NEW.expires_at IS NOT OLD.expires_at
                OR NEW.created_at IS NOT OLD.created_at
                OR NEW.decision IS NULL
                OR NEW.actor IS NULL
                OR NEW.channel IS NULL
                OR NEW.decided_at IS NULL
            BEGIN
                SELECT RAISE(ABORT, 'Approval decision is immutable.');
            END
            """,
            """
            CREATE TRIGGER approvals_prevent_delete
            BEFORE DELETE ON approvals
            BEGIN
                SELECT RAISE(ABORT, 'Approval records are immutable.');
            END
            """,
        ),
    ),
    Migration(
        version=7,
        name="create_planner",
        statements=(
            """
            CREATE TABLE planner (
                id TEXT PRIMARY KEY CHECK (
                    length(id) > 0
                    AND length(CAST(id AS BLOB)) <= 128
                    AND id = trim(id)
                    AND instr(id, char(0)) = 0
                ),
                kind TEXT NOT NULL CHECK (
                    kind IN ('deadline', 'blocker', 'watcher')
                ),
                project_id TEXT
                    REFERENCES projects(id) ON DELETE RESTRICT,
                title TEXT NOT NULL CHECK (
                    length(trim(title)) > 0
                    AND length(CAST(title AS BLOB)) <= 4096
                    AND title = trim(title)
                    AND instr(title, char(0)) = 0
                ),
                details TEXT CHECK (
                    details IS NULL OR (
                        length(trim(details)) > 0
                        AND length(CAST(details AS BLOB)) <= 262144
                        AND instr(details, char(0)) = 0
                    )
                ),
                due_at INTEGER CHECK (due_at IS NULL OR due_at > 0),
                source TEXT CHECK (
                    source IS NULL OR (
                        length(source) > 0
                        AND length(CAST(source AS BLOB)) <= 128
                        AND source = trim(source)
                        AND instr(source, char(0)) = 0
                    )
                ),
                source_key TEXT CHECK (
                    source_key IS NULL OR (
                        length(source_key) > 0
                        AND length(CAST(source_key AS BLOB)) <= 512
                        AND source_key = trim(source_key)
                        AND instr(source_key, char(0)) = 0
                    )
                ),
                promoted_job_id TEXT
                    REFERENCES jobs(id) ON DELETE RESTRICT,
                created_at TEXT NOT NULL
                    DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at TEXT NOT NULL
                    DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
                promoted_at TEXT,
                CHECK (kind != 'deadline' OR due_at IS NOT NULL),
                CHECK (kind != 'watcher' OR source IS NOT NULL),
                CHECK (source_key IS NULL OR source IS NOT NULL),
                CHECK (
                    (promoted_job_id IS NULL AND promoted_at IS NULL)
                    OR (promoted_job_id IS NOT NULL AND promoted_at IS NOT NULL)
                )
            )
            """,
            """
            CREATE INDEX planner_project_created_idx
            ON planner (project_id, created_at, id)
            """,
            """
            CREATE INDEX planner_kind_due_idx
            ON planner (kind, due_at, created_at, id)
            """,
            """
            CREATE UNIQUE INDEX planner_promoted_job_idx
            ON planner (promoted_job_id)
            WHERE promoted_job_id IS NOT NULL
            """,
            """
            CREATE TRIGGER planner_require_pending_insert
            BEFORE INSERT ON planner
            WHEN NEW.promoted_job_id IS NOT NULL OR NEW.promoted_at IS NOT NULL
            BEGIN
                SELECT RAISE(ABORT, 'Planner item must begin pending.');
            END
            """,
            """
            CREATE TRIGGER planner_validate_promotion
            BEFORE UPDATE ON planner
            WHEN OLD.promoted_job_id IS NOT NULL
                OR NEW.id IS NOT OLD.id
                OR NEW.kind IS NOT OLD.kind
                OR NEW.project_id IS NOT OLD.project_id
                OR NEW.title IS NOT OLD.title
                OR NEW.details IS NOT OLD.details
                OR NEW.due_at IS NOT OLD.due_at
                OR NEW.source IS NOT OLD.source
                OR NEW.source_key IS NOT OLD.source_key
                OR NEW.created_at IS NOT OLD.created_at
                OR NEW.promoted_job_id IS NULL
                OR NEW.promoted_at IS NULL
            BEGIN
                SELECT RAISE(ABORT, 'Planner promotion is immutable.');
            END
            """,
            """
            CREATE TRIGGER planner_protect_promoted_delete
            BEFORE DELETE ON planner
            WHEN OLD.promoted_job_id IS NOT NULL
            BEGIN
                SELECT RAISE(ABORT, 'Promoted planner items are immutable.');
            END
            """,
        ),
    ),
    Migration(
        version=8,
        name="create_usage",
        statements=(
            """
            CREATE TABLE usage (
                id INTEGER PRIMARY KEY CHECK (id > 0),
                provider TEXT NOT NULL CHECK (
                    length(provider) > 0
                    AND length(CAST(provider AS BLOB)) <= 128
                    AND provider = trim(provider)
                    AND instr(provider, char(0)) = 0
                ),
                job_id TEXT REFERENCES jobs(id) ON DELETE RESTRICT,
                model TEXT CHECK (
                    model IS NULL OR (
                        length(model) > 0
                        AND length(CAST(model AS BLOB)) <= 512
                        AND model = trim(model)
                        AND instr(model, char(0)) = 0
                    )
                ),
                input_tokens INTEGER CHECK (
                    input_tokens IS NULL OR input_tokens >= 0
                ),
                output_tokens INTEGER CHECK (
                    output_tokens IS NULL OR output_tokens >= 0
                ),
                cost_usd TEXT CHECK (
                    cost_usd IS NULL OR (
                        length(CAST(cost_usd AS BLOB)) <= 128
                        AND json_valid(cost_usd)
                        AND json_type(cost_usd) IN ('integer', 'real')
                        AND CAST(cost_usd AS REAL) >= 0
                    )
                ),
                latency_ms INTEGER CHECK (
                    latency_ms IS NULL OR latency_ms >= 0
                ),
                quota_limit TEXT CHECK (
                    quota_limit IS NULL OR (
                        length(CAST(quota_limit AS BLOB)) <= 128
                        AND json_valid(quota_limit)
                        AND json_type(quota_limit) IN ('integer', 'real')
                        AND CAST(quota_limit AS REAL) >= 0
                    )
                ),
                quota_remaining TEXT CHECK (
                    quota_remaining IS NULL OR (
                        length(CAST(quota_remaining AS BLOB)) <= 128
                        AND json_valid(quota_remaining)
                        AND json_type(quota_remaining) IN ('integer', 'real')
                        AND CAST(quota_remaining AS REAL) >= 0
                    )
                ),
                quota_unit TEXT CHECK (
                    quota_unit IS NULL OR (
                        length(quota_unit) > 0
                        AND length(CAST(quota_unit AS BLOB)) <= 64
                        AND quota_unit = trim(quota_unit)
                        AND instr(quota_unit, char(0)) = 0
                    )
                ),
                quota_reset_at INTEGER CHECK (
                    quota_reset_at IS NULL OR quota_reset_at > 0
                ),
                rate_limited INTEGER CHECK (
                    rate_limited IS NULL OR rate_limited IN (0, 1)
                ),
                recorded_at TEXT NOT NULL
                    DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
                CHECK (
                    input_tokens IS NOT NULL OR output_tokens IS NOT NULL
                    OR cost_usd IS NOT NULL OR latency_ms IS NOT NULL
                    OR quota_limit IS NOT NULL OR quota_remaining IS NOT NULL
                    OR quota_reset_at IS NOT NULL OR rate_limited IS NOT NULL
                ),
                CHECK (
                    (quota_limit IS NULL AND quota_remaining IS NULL AND quota_unit IS NULL)
                    OR (
                        quota_unit IS NOT NULL
                        AND (quota_limit IS NOT NULL OR quota_remaining IS NOT NULL)
                    )
                ),
                CHECK (
                    quota_limit IS NULL OR quota_remaining IS NULL
                    OR CAST(quota_remaining AS REAL) <= CAST(quota_limit AS REAL)
                )
            )
            """,
            """
            CREATE INDEX usage_job_recorded_idx
            ON usage (job_id, recorded_at, id)
            """,
            """
            CREATE INDEX usage_provider_recorded_idx
            ON usage (provider, recorded_at, id)
            """,
            """
            CREATE TRIGGER usage_prevent_update
            BEFORE UPDATE ON usage
            BEGIN
                SELECT RAISE(ABORT, 'Usage observations are immutable.');
            END
            """,
            """
            CREATE TRIGGER usage_prevent_delete
            BEFORE DELETE ON usage
            BEGIN
                SELECT RAISE(ABORT, 'Usage observations are immutable.');
            END
            """,
        ),
    ),
    Migration(
        version=9,
        name="create_memory_refs",
        statements=(
            """
            CREATE UNIQUE INDEX events_id_job_idx
            ON events (id, job_id)
            """,
            """
            CREATE TABLE memory_refs (
                id INTEGER PRIMARY KEY CHECK (id > 0),
                projmem_record_id TEXT NOT NULL CHECK (
                    length(projmem_record_id) > 0
                    AND length(CAST(projmem_record_id AS BLOB)) <= 512
                    AND projmem_record_id = trim(projmem_record_id)
                    AND instr(projmem_record_id, char(0)) = 0
                ),
                job_id TEXT NOT NULL
                    REFERENCES jobs(id) ON DELETE RESTRICT,
                event_id INTEGER CHECK (event_id IS NULL OR event_id > 0),
                created_at TEXT NOT NULL
                    DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')),
                FOREIGN KEY (event_id, job_id)
                    REFERENCES events(id, job_id) ON DELETE RESTRICT
            )
            """,
            """
            CREATE UNIQUE INDEX memory_refs_job_scope_idx
            ON memory_refs (projmem_record_id, job_id)
            WHERE event_id IS NULL
            """,
            """
            CREATE UNIQUE INDEX memory_refs_event_scope_idx
            ON memory_refs (projmem_record_id, job_id, event_id)
            WHERE event_id IS NOT NULL
            """,
            """
            CREATE INDEX memory_refs_job_created_idx
            ON memory_refs (job_id, created_at, id)
            """,
            """
            CREATE INDEX memory_refs_event_created_idx
            ON memory_refs (event_id, created_at, id)
            WHERE event_id IS NOT NULL
            """,
            """
            CREATE TRIGGER memory_refs_prevent_update
            BEFORE UPDATE ON memory_refs
            BEGIN
                SELECT RAISE(ABORT, 'Memory references are immutable.');
            END
            """,
            """
            CREATE TRIGGER memory_refs_prevent_delete
            BEFORE DELETE ON memory_refs
            BEGIN
                SELECT RAISE(ABORT, 'Memory references are immutable.');
            END
            """,
        ),
    ),
)

LATEST_SCHEMA_VERSION = MIGRATIONS[-1].version


def _validate_migrations(migrations: Sequence[Migration]) -> tuple[Migration, ...]:
    normalized = tuple(migrations)
    if not normalized:
        raise MigrationDefinitionError("At least one migration is required.")
    if any(
        not isinstance(migration.version, int) or isinstance(migration.version, bool)
        for migration in normalized
    ):
        raise MigrationDefinitionError("Migration versions must be integers.")
    expected_versions = list(range(1, len(normalized) + 1))
    actual_versions = [migration.version for migration in normalized]
    if actual_versions != expected_versions:
        raise MigrationDefinitionError("Migration versions must be sequential and start at one.")
    if any(not migration.name.strip() for migration in normalized):
        raise MigrationDefinitionError("Every migration requires a name.")
    if len({migration.name for migration in normalized}) != len(normalized):
        raise MigrationDefinitionError("Migration names must be unique.")
    if any(not migration.statements for migration in normalized):
        raise MigrationDefinitionError("Every migration requires at least one statement.")
    if any(not statement.strip() for migration in normalized for statement in migration.statements):
        raise MigrationDefinitionError("Migration statements must not be empty.")
    return normalized


class Database:
    def __init__(
        self,
        path: Path,
        *,
        migrations: Sequence[Migration] = MIGRATIONS,
    ) -> None:
        self.path = path
        self.migrations = _validate_migrations(migrations)

    @property
    def latest_schema_version(self) -> int:
        return self.migrations[-1].version if self.migrations else 0

    def initialize(self) -> int:
        if not self.path.parent.is_dir():
            raise DatabaseError("Database parent directory does not exist.")

        try:
            connection = sqlite3.connect(self.path, timeout=5.0, isolation_level=None)
        except sqlite3.Error as error:
            raise DatabaseError("Database could not be opened.") from error
        target_version: int | None = None

        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("BEGIN IMMEDIATE")
            current_version = self._read_schema_version(connection)
            if current_version > self.latest_schema_version:
                raise DatabaseVersionError(
                    "Database schema is newer than this application supports."
                )

            for migration in self.migrations[current_version:]:
                target_version = migration.version
                for statement in migration.statements:
                    connection.execute(statement)
                updated = connection.execute(
                    """
                    UPDATE schema_version
                    SET version = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE singleton = 1
                    """,
                    (migration.version,),
                )
                if updated.rowcount != 1:
                    raise DatabaseVersionError("Schema version metadata is invalid.")

            if self._read_schema_version(connection) != self.latest_schema_version:
                raise DatabaseVersionError("Schema version metadata is invalid.")
            connection.execute("COMMIT")
            return self.latest_schema_version
        except DatabaseError:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        except sqlite3.Error as error:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            version = target_version if target_version is not None else "unknown"
            raise MigrationApplyError(f"Migration {version} failed.") from error
        finally:
            connection.close()

    @staticmethod
    def _read_schema_version(connection: sqlite3.Connection) -> int:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'"
        ).fetchone()
        if table is None:
            return 0

        try:
            columns = connection.execute("PRAGMA table_info(schema_version)").fetchall()
            shape = [(column[1], column[2].upper(), column[3], column[5]) for column in columns]
            expected_shape = [
                ("singleton", "INTEGER", 0, 1),
                ("version", "INTEGER", 1, 0),
                ("updated_at", "TEXT", 1, 0),
            ]
            if shape != expected_shape:
                raise DatabaseVersionError("Schema version metadata is invalid.")
            rows = connection.execute("SELECT singleton, version FROM schema_version").fetchall()
        except sqlite3.Error as error:
            raise DatabaseVersionError("Schema version metadata is invalid.") from error

        if len(rows) != 1 or rows[0][0] != 1:
            raise DatabaseVersionError("Schema version metadata is invalid.")
        version = rows[0][1]
        if not isinstance(version, int) or version < 0:
            raise DatabaseVersionError("Schema version metadata is invalid.")
        return version


class BackupService:
    """Create a verified snapshot without replacing existing files."""

    _DEFAULT_TIMEOUT = 5.0
    _BACKUP_PAGES = 128
    _BACKUP_SLEEP = 0.01
    _PROGRESS_OPS = 1_000
    _SOURCE_SIDECARS = ("", "-wal", "-shm", "-journal")

    def __init__(self, database: Database, *, timeout: float = _DEFAULT_TIMEOUT) -> None:
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            raise ValueError("Backup timeout must be a positive finite number.")
        if not math.isfinite(float(timeout)) or float(timeout) <= 0:
            raise ValueError("Backup timeout must be a positive finite number.")
        self.database = database
        self.timeout = float(timeout)

    def create(self, destination: Path) -> Path:
        """Create and return a verified backup at a new destination path."""

        deadline = time.monotonic() + self.timeout
        destination_path = Path(destination)
        destination_real, destination_parent = self._validate_destination(destination_path)
        source_path = self._validate_source()
        temporary_path: Path | None = None
        source_connection: sqlite3.Connection | None = None
        destination_connection: sqlite3.Connection | None = None

        try:
            source_connection = self._open_source(source_path, deadline)
            try:
                source_version = Database._read_schema_version(source_connection)
            except DatabaseError as error:
                raise BackupServiceError("Backup source schema metadata is invalid.") from error
            if source_version != self.database.latest_schema_version:
                raise BackupServiceError("Backup source schema is incompatible.")

            temporary_path = self._create_temporary(destination_parent, destination_path)
            destination_connection = sqlite3.connect(
                temporary_path,
                timeout=self._remaining(deadline),
                isolation_level=None,
            )
            destination_connection.execute("PRAGMA foreign_keys = ON")
            destination_connection.execute(
                f"PRAGMA busy_timeout = {self._busy_timeout_ms(deadline)}"
            )

            def backup_progress(_status: int, _remaining: int, _total: int) -> None:
                if time.monotonic() >= deadline:
                    raise BackupServiceError("Backup timed out.")

            try:
                source_connection.backup(
                    destination_connection,
                    pages=self._BACKUP_PAGES,
                    progress=backup_progress,
                    sleep=self._BACKUP_SLEEP,
                )
            except sqlite3.Error as error:
                if time.monotonic() >= deadline:
                    raise BackupServiceError("Backup timed out.") from error
                raise BackupServiceError("Backup could not be copied safely.") from error
            if time.monotonic() >= deadline:
                raise BackupServiceError("Backup timed out.")

            self._verify(destination_connection, source_version, deadline)
            journal_mode = destination_connection.execute("PRAGMA journal_mode = DELETE").fetchone()
            if journal_mode is None or str(journal_mode[0]).lower() != "delete":
                raise BackupServiceError("Backup journal mode could not be finalized safely.")
            destination_connection.close()
            destination_connection = None
            self._remove_sidecars(temporary_path)
            self._publish(temporary_path, destination_real)
            temporary_path = None
            return destination_path
        except BackupServiceError:
            raise
        except (OSError, sqlite3.Error, ValueError) as error:
            if time.monotonic() >= deadline:
                raise BackupServiceError("Backup timed out.") from error
            raise BackupServiceError("Backup could not be created safely.") from error
        finally:
            if destination_connection is not None:
                destination_connection.close()
            if source_connection is not None:
                source_connection.close()
            if temporary_path is not None:
                self._remove_temporary(temporary_path)

    def _validate_source(self) -> Path:
        source = Path(self.database.path)
        if source.is_symlink():
            raise BackupServiceError("Backup source aliases are not allowed.")
        if not source.is_file():
            raise BackupServiceError("Backup source database is unavailable.")
        try:
            resolved = source.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise BackupServiceError("Backup source database is unavailable.") from error
        if not resolved.is_file():
            raise BackupServiceError("Backup source database is unavailable.")
        return resolved

    def _validate_destination(self, destination: Path) -> tuple[Path, Path]:
        try:
            if any(
                os.path.lexists(Path(f"{destination}{suffix}")) for suffix in self._SOURCE_SIDECARS
            ):
                raise BackupServiceError("Backup destination must be a new file.")
            parent = destination.parent
            if not parent.is_dir():
                raise BackupServiceError("Backup destination parent is unavailable.")
            resolved_parent = parent.resolve(strict=True)
            resolved_destination = resolved_parent / destination.name
        except BackupServiceError:
            raise
        except (OSError, RuntimeError) as error:
            raise BackupServiceError("Backup destination is unavailable.") from error

        if self._public_repository_root(resolved_destination) is not None:
            raise BackupServiceError("Backup destination must be outside a public repository.")

        source = Path(self.database.path)
        try:
            resolved_source = source.resolve(strict=False)
        except (OSError, RuntimeError) as error:
            raise BackupServiceError("Backup source database is unavailable.") from error
        source_candidates = {Path(f"{resolved_source}{suffix}") for suffix in self._SOURCE_SIDECARS}
        if resolved_destination in source_candidates:
            raise BackupServiceError("Backup destination conflicts with source database files.")
        return resolved_destination, resolved_parent

    @staticmethod
    def _public_repository_root(path: Path) -> Path | None:
        current = path if path.is_dir() else path.parent
        for parent in (current, *current.parents):
            marker = parent / ".git"
            if marker.is_dir() or marker.is_file():
                return parent
        return None

    def _open_source(self, source: Path, deadline: float) -> sqlite3.Connection:
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                f"{source.as_uri()}?mode=ro",
                uri=True,
                timeout=self._remaining(deadline),
                isolation_level=None,
            )
            connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms(deadline)}")
            connection.execute("PRAGMA foreign_keys = ON")
            return connection
        except (OSError, sqlite3.Error, ValueError, BackupServiceError) as error:
            if connection is not None:
                connection.close()
            raise BackupServiceError("Backup source database could not be opened.") from error

    def _create_temporary(self, parent: Path, destination: Path) -> Path:
        descriptor = -1
        temporary: Path | None = None
        try:
            descriptor, name = tempfile.mkstemp(
                prefix=f".{destination.name}.",
                suffix=".tmp",
                dir=parent,
            )
            temporary = Path(name)
            os.close(descriptor)
            descriptor = -1
            return temporary
        except OSError as error:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary is not None:
                self._remove_temporary(temporary)
            raise BackupServiceError("Backup temporary storage is unavailable.") from error

    def _verify(
        self,
        connection: sqlite3.Connection,
        expected_version: int,
        deadline: float,
    ) -> None:
        state = {"timed_out": False}

        def progress() -> int:
            if time.monotonic() >= deadline:
                state["timed_out"] = True
                return 1
            return 0

        connection.set_progress_handler(progress, self._PROGRESS_OPS)
        try:
            try:
                stored_version = Database._read_schema_version(connection)
                if stored_version != expected_version:
                    raise BackupServiceError("Backup schema metadata is incompatible.")
                integrity = connection.execute("PRAGMA integrity_check").fetchall()
                if integrity != [("ok",)]:
                    raise BackupServiceError("Backup integrity verification failed.")
                foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
                if foreign_keys:
                    raise BackupServiceError("Backup foreign key verification failed.")
            except BackupServiceError:
                raise
            except DatabaseError as error:
                raise BackupServiceError("Backup schema metadata is invalid.") from error
            except sqlite3.Error as error:
                if state["timed_out"] or time.monotonic() >= deadline:
                    raise BackupServiceError("Backup timed out.") from error
                raise BackupServiceError("Backup integrity verification failed.") from error
            if state["timed_out"] or time.monotonic() >= deadline:
                raise BackupServiceError("Backup timed out.")
        finally:
            connection.set_progress_handler(None, 0)

    def _publish(self, temporary: Path, destination: Path) -> None:
        if any(os.path.lexists(Path(f"{destination}{suffix}")) for suffix in self._SOURCE_SIDECARS):
            raise BackupServiceError("Backup destination must be a new file.")
        try:
            descriptor = os.open(temporary, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as error:
            raise BackupServiceError("Backup could not be synchronized safely.") from error
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as error:
            raise BackupServiceError("Backup destination must be a new file.") from error
        except OSError as error:
            raise BackupServiceError("Backup could not be published safely.") from error
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        except OSError as error:
            raise BackupServiceError("Backup publication cleanup failed.") from error

    @staticmethod
    def _remove_sidecars(path: Path) -> None:
        for suffix in ("-wal", "-shm", "-journal"):
            try:
                Path(f"{path}{suffix}").unlink()
            except FileNotFoundError:
                continue
            except OSError:
                continue

    def _remove_temporary(self, path: Path) -> None:
        self._remove_sidecars(path)
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass

    def _remaining(self, deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise BackupServiceError("Backup timed out.")
        return remaining

    def _busy_timeout_ms(self, deadline: float) -> int:
        return max(0, min(100, int(self._remaining(deadline) * 1000)))


class _Repository:
    error_type: type[DatabaseError]
    storage_name: str

    def __init__(self, database: Database) -> None:
        self.database = database

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if not self.database.path.is_file():
            raise self.error_type(f"{self.storage_name} storage is unavailable.")
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(self.database.path, timeout=5.0, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            yield connection
        except sqlite3.Error as error:
            raise self.error_type(f"{self.storage_name} storage operation failed.") from error
        finally:
            if connection is not None:
                connection.close()

    @contextmanager
    def _write_connection(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.execute("COMMIT")
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise


class ProjectRepository(_Repository):
    error_type = ProjectRepositoryError
    storage_name = "Project"

    def create(self, project: ProjectConfig) -> ProjectRecord:
        self._validate_project(project)
        with self._write_connection() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO projects (
                        id, root, sensitivity, cloud_allowed, permission_mode
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        project.id,
                        project.root,
                        project.sensitivity.value,
                        int(project.cloud_allowed),
                        project.permission_mode.value,
                    ),
                )
                row = self._select_by_id(connection, project.id)
                if row is None:
                    raise ProjectRepositoryError("Project could not be created.")
                stored = self._to_record(row)
            except sqlite3.IntegrityError as error:
                if self._is_unique_violation(error):
                    raise ProjectAlreadyExistsError(
                        "A project with this id or root already exists."
                    ) from error
                raise ProjectRepositoryError("Project could not be created.") from error
        return stored

    def get(self, project_id: str) -> ProjectRecord:
        normalized_id = self._validate_project_id(project_id)
        with self._connection() as connection:
            row = self._select_by_id(connection, normalized_id)
        if row is None:
            raise ProjectNotFoundError("Project does not exist.")
        return self._to_record(row)

    def list(self) -> tuple[ProjectRecord, ...]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT id, root, sensitivity, cloud_allowed, permission_mode,
                       created_at, updated_at
                FROM projects
                ORDER BY id
                """
            ).fetchall()
        return tuple(self._to_record(row) for row in rows)

    def update(self, project: ProjectConfig) -> ProjectRecord:
        self._validate_project(project)
        with self._write_connection() as connection:
            try:
                result = connection.execute(
                    """
                    UPDATE projects
                    SET root = ?,
                        sensitivity = ?,
                        cloud_allowed = ?,
                        permission_mode = ?,
                        updated_at = STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE id = ?
                    """,
                    (
                        project.root,
                        project.sensitivity.value,
                        int(project.cloud_allowed),
                        project.permission_mode.value,
                        project.id,
                    ),
                )
                if result.rowcount != 1:
                    raise ProjectNotFoundError("Project does not exist.")
                row = self._select_by_id(connection, project.id)
                if row is None:
                    raise ProjectRepositoryError("Project could not be updated.")
                stored = self._to_record(row)
            except sqlite3.IntegrityError as error:
                if self._is_unique_violation(error):
                    raise ProjectAlreadyExistsError(
                        "A project with this id or root already exists."
                    ) from error
                raise ProjectRepositoryError("Project could not be updated.") from error
        return stored

    def delete(self, project_id: str) -> None:
        normalized_id = self._validate_project_id(project_id)
        with self._write_connection() as connection:
            result = connection.execute("DELETE FROM projects WHERE id = ?", (normalized_id,))
            if result.rowcount != 1:
                raise ProjectNotFoundError("Project does not exist.")

    @staticmethod
    def _select_by_id(
        connection: sqlite3.Connection,
        project_id: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT id, root, sensitivity, cloud_allowed, permission_mode,
                   created_at, updated_at
            FROM projects
            WHERE id = ?
            """,
            (project_id,),
        ).fetchone()

    @staticmethod
    def _validate_project(project: ProjectConfig) -> None:
        if not isinstance(project, ProjectConfig):
            raise ProjectValidationError("Project must use the supported configuration contract.")
        ProjectRepository._validate_text(project.id, field="id")
        ProjectRepository._validate_text(project.root, field="root")
        if not isinstance(project.sensitivity, Sensitivity):
            raise ProjectValidationError("Project sensitivity is invalid.")
        if not isinstance(project.cloud_allowed, bool):
            raise ProjectValidationError("Project cloud policy is invalid.")
        if not isinstance(project.permission_mode, PermissionMode):
            raise ProjectValidationError("Project permission policy is invalid.")
        if (
            project.sensitivity in {Sensitivity.INTERNAL, Sensitivity.RESTRICTED}
            and project.cloud_allowed
        ):
            raise ProjectValidationError("Project cloud policy conflicts with sensitivity.")

    @staticmethod
    def _validate_project_id(project_id: str) -> str:
        ProjectRepository._validate_text(project_id, field="id")
        return project_id

    @staticmethod
    def _validate_text(value: object, *, field: str) -> None:
        if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
            raise ProjectValidationError(f"Project {field} is invalid.")

    @staticmethod
    def _to_record(row: sqlite3.Row) -> ProjectRecord:
        try:
            cloud_allowed = row["cloud_allowed"]
            created_at = row["created_at"]
            updated_at = row["updated_at"]
            if cloud_allowed not in (0, 1):
                raise ValueError
            if not isinstance(created_at, str) or not created_at:
                raise ValueError
            if not isinstance(updated_at, str) or not updated_at:
                raise ValueError
            project = ProjectConfig(
                id=row["id"],
                root=row["root"],
                sensitivity=Sensitivity(row["sensitivity"]),
                cloud_allowed=bool(cloud_allowed),
                permission_mode=PermissionMode(row["permission_mode"]),
            )
            ProjectRepository._validate_project(project)
            return ProjectRecord(
                id=project.id,
                root=project.root,
                sensitivity=project.sensitivity,
                cloud_allowed=project.cloud_allowed,
                permission_mode=project.permission_mode,
                created_at=created_at,
                updated_at=updated_at,
            )
        except (IndexError, KeyError, ProjectValidationError, TypeError, ValueError) as error:
            raise ProjectRepositoryError("Stored project data is invalid.") from error

    @staticmethod
    def _is_unique_violation(error: sqlite3.IntegrityError) -> bool:
        return getattr(error, "sqlite_errorname", "") in {
            "SQLITE_CONSTRAINT_PRIMARYKEY",
            "SQLITE_CONSTRAINT_UNIQUE",
        }


class JobRepository(_Repository):
    error_type = JobRepositoryError
    storage_name = "Job"

    def create(self, job: JobCreate) -> JobRecord:
        snapshot = self._validate_job(job)
        with self._write_connection() as connection:
            try:
                stored = self._insert(connection, job, snapshot)
            except sqlite3.IntegrityError as error:
                error_name = getattr(error, "sqlite_errorname", "")
                if error_name in {"SQLITE_CONSTRAINT_PRIMARYKEY", "SQLITE_CONSTRAINT_UNIQUE"}:
                    raise JobAlreadyExistsError("A job with this id already exists.") from error
                if error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
                    raise JobValidationError("Job project does not exist.") from error
                raise JobRepositoryError("Job could not be created.") from error
        return stored

    def update(self, job: JobUpdate) -> JobRecord:
        self._validate_update(job)
        with self._write_connection() as connection:
            return self._update(connection, job)

    def get(self, job_id: str) -> JobRecord:
        normalized_id = self._validate_text(job_id, field="id")
        with self._connection() as connection:
            row = self._select_by_id(connection, normalized_id)
        if row is None:
            raise JobNotFoundError("Job does not exist.")
        return self._to_record(row)

    def list(self, *, project_id: str | None = None) -> tuple[JobRecord, ...]:
        parameters: tuple[str, ...] = ()
        where = ""
        if project_id is not None:
            parameters = (self._validate_text(project_id, field="project_id"),)
            where = "WHERE project_id = ?"
        with self._connection() as connection:
            rows = connection.execute(
                f"""
                SELECT id, project_id, request, request_snapshot, state,
                       runtime, model, worktree_path, created_at, updated_at
                FROM jobs
                {where}
                ORDER BY created_at, id
                """,
                parameters,
            ).fetchall()
        return tuple(self._to_record(row) for row in rows)

    @classmethod
    def _insert(
        cls,
        connection: sqlite3.Connection,
        job: JobCreate,
        snapshot: str,
    ) -> JobRecord:
        connection.execute(
            """
            INSERT INTO jobs (
                id, project_id, request, request_snapshot, state,
                runtime, model, worktree_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job.id,
                job.project_id,
                job.request,
                snapshot,
                job.state.value,
                job.runtime.value,
                job.model,
                job.worktree_path,
            ),
        )
        row = cls._select_by_id(connection, job.id)
        if row is None:
            raise JobRepositoryError("Job could not be created.")
        return cls._to_record(row)

    @classmethod
    def _update(
        cls,
        connection: sqlite3.Connection,
        job: JobUpdate,
    ) -> JobRecord:
        result = connection.execute(
            """
            UPDATE jobs
            SET state = ?, runtime = ?, model = ?, worktree_path = ?,
                updated_at = STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ?
            """,
            (job.state.value, job.runtime.value, job.model, job.worktree_path, job.id),
        )
        if result.rowcount != 1:
            raise JobNotFoundError("Job does not exist.")
        row = cls._select_by_id(connection, job.id)
        if row is None:
            raise JobRepositoryError("Job could not be updated.")
        return cls._to_record(row)

    @staticmethod
    def _select_by_id(connection: sqlite3.Connection, job_id: str) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT id, project_id, request, request_snapshot, state,
                   runtime, model, worktree_path, created_at, updated_at
            FROM jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()

    @classmethod
    def _validate_job(cls, job: JobCreate) -> str:
        if not isinstance(job, JobCreate):
            raise JobValidationError("Job must use the supported creation contract.")
        cls._validate_text(job.id, field="id")
        cls._validate_text(job.project_id, field="project_id")
        if not isinstance(job.request, str) or not job.request.strip() or "\x00" in job.request:
            raise JobValidationError("Job request is invalid.")
        if len(job.request.encode("utf-8")) > 262_144:
            raise JobValidationError("Job request is too large.")
        if not isinstance(job.state, JobState):
            raise JobValidationError("Job state is invalid.")
        if not isinstance(job.runtime, JobRuntime):
            raise JobValidationError("Job runtime is invalid.")
        cls._validate_optional_text(job.model, field="model")
        cls._validate_optional_text(job.worktree_path, field="worktree_path")
        if not isinstance(job.request_snapshot, Mapping):
            raise JobValidationError("Job request snapshot must be an object.")
        try:
            source_snapshot = dict(job.request_snapshot)
            snapshot = json.dumps(
                source_snapshot,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            decoded_snapshot = json.loads(snapshot)
        except (RecursionError, TypeError, ValueError) as error:
            raise JobValidationError("Job request snapshot is invalid.") from error
        if decoded_snapshot != source_snapshot:
            raise JobValidationError("Job request snapshot is not JSON canonical.")
        if len(snapshot.encode("utf-8")) > 1_048_576:
            raise JobValidationError("Job request snapshot is too large.")
        return snapshot

    @classmethod
    def _validate_update(cls, job: JobUpdate) -> None:
        if not isinstance(job, JobUpdate):
            raise JobValidationError("Job must use the supported update contract.")
        cls._validate_text(job.id, field="id")
        if not isinstance(job.state, JobState) or not isinstance(job.runtime, JobRuntime):
            raise JobValidationError("Job state or runtime is invalid.")
        cls._validate_optional_text(job.model, field="model")
        cls._validate_optional_text(job.worktree_path, field="worktree_path")

    @staticmethod
    def _validate_text(value: object, *, field: str) -> str:
        if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
            raise JobValidationError(f"Job {field} is invalid.")
        return value

    @staticmethod
    def _validate_optional_text(value: object, *, field: str) -> None:
        if value is not None and (
            not isinstance(value, str) or not value or value != value.strip() or "\x00" in value
        ):
            raise JobValidationError(f"Job {field} is invalid.")

    @classmethod
    def _to_record(cls, row: sqlite3.Row) -> JobRecord:
        try:
            snapshot = json.loads(row["request_snapshot"])
            if not isinstance(snapshot, dict):
                raise ValueError
            job = JobCreate(
                id=row["id"],
                project_id=row["project_id"],
                request=row["request"],
                request_snapshot=snapshot,
                state=JobState(row["state"]),
                runtime=JobRuntime(row["runtime"]),
                model=row["model"],
                worktree_path=row["worktree_path"],
            )
            cls._validate_job(job)
            created_at = row["created_at"]
            updated_at = row["updated_at"]
            if not isinstance(created_at, str) or not created_at:
                raise ValueError
            if not isinstance(updated_at, str) or not updated_at:
                raise ValueError
            return JobRecord(
                id=job.id,
                project_id=job.project_id,
                request=job.request,
                request_snapshot=snapshot,
                state=job.state,
                runtime=job.runtime,
                model=job.model,
                worktree_path=job.worktree_path,
                created_at=created_at,
                updated_at=updated_at,
            )
        except (IndexError, KeyError, JobValidationError, TypeError, ValueError) as error:
            raise JobRepositoryError("Stored job data is invalid.") from error


class EventRepository(_Repository):
    error_type = EventRepositoryError
    storage_name = "Event"

    def append(self, event: EventCreate) -> EventRecord:
        payload, payload_hash, idempotency_key = self._validate_event(event)
        with self._write_connection() as connection:
            try:
                stored = self._append(
                    connection,
                    event,
                    payload,
                    payload_hash,
                    idempotency_key,
                )
            except sqlite3.IntegrityError as error:
                error_name = getattr(error, "sqlite_errorname", "")
                if error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
                    raise EventValidationError("Event job does not exist.") from error
                raise EventRepositoryError("Event could not be appended.") from error
        return stored

    @classmethod
    def _append(
        cls,
        connection: sqlite3.Connection,
        event: EventCreate,
        payload: str,
        payload_hash: str,
        idempotency_key: str | None,
    ) -> EventRecord:
        existing = cls._matching_idempotent_event(
            connection,
            event,
            payload_hash,
            idempotency_key,
        )
        if existing is not None:
            return existing
        latest = connection.execute(
            """
            SELECT sequence
            FROM events
            WHERE job_id = ?
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (event.job_id,),
        ).fetchone()
        previous_sequence = 0 if latest is None else latest["sequence"]
        if (
            not isinstance(previous_sequence, int)
            or isinstance(previous_sequence, bool)
            or previous_sequence < 0
            or previous_sequence >= 9_223_372_036_854_775_807
        ):
            raise EventRepositoryError("Stored event sequence is invalid.")
        result = connection.execute(
            """
            INSERT INTO events (
                job_id, sequence, event_type, payload, payload_hash,
                idempotency_key
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.job_id,
                previous_sequence + 1,
                event.event_type,
                payload,
                payload_hash,
                idempotency_key,
            ),
        )
        event_id = result.lastrowid
        if not isinstance(event_id, int) or event_id < 1:
            raise EventRepositoryError("Event could not be appended.")
        row = cls._select_by_id(connection, event_id)
        if row is None:
            raise EventRepositoryError("Event could not be appended.")
        return cls._to_record(row)

    @classmethod
    def _matching_idempotent_event(
        cls,
        connection: sqlite3.Connection,
        event: EventCreate,
        payload_hash: str,
        idempotency_key: str | None,
    ) -> EventRecord | None:
        if idempotency_key is None:
            return None
        existing = cls._select_by_idempotency_key(
            connection,
            event.job_id,
            idempotency_key,
        )
        if existing is None:
            return None
        stored = cls._to_record(existing)
        if stored.event_type != event.event_type or stored.payload_hash != payload_hash:
            raise EventIdempotencyConflictError("Event idempotency key conflicts with stored data.")
        return stored

    def get(self, event_id: int) -> EventRecord:
        normalized_id = self._validate_event_id(event_id)
        with self._connection() as connection:
            row = self._select_by_id(connection, normalized_id)
        if row is None:
            raise EventNotFoundError("Event does not exist.")
        return self._to_record(row)

    def list(self, job_id: str) -> tuple[EventRecord, ...]:
        normalized_job_id = self._validate_text(job_id, field="job_id")
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT id, job_id, sequence, event_type, payload,
                       payload_hash, idempotency_key, created_at
                FROM events
                WHERE job_id = ?
                ORDER BY sequence
                """,
                (normalized_job_id,),
            ).fetchall()
        return tuple(self._to_record(row) for row in rows)

    @staticmethod
    def _select_by_id(connection: sqlite3.Connection, event_id: int) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT id, job_id, sequence, event_type, payload,
                   payload_hash, idempotency_key, created_at
            FROM events
            WHERE id = ?
            """,
            (event_id,),
        ).fetchone()

    @staticmethod
    def _select_by_idempotency_key(
        connection: sqlite3.Connection, job_id: str, idempotency_key: str
    ) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT id, job_id, sequence, event_type, payload,
                   payload_hash, idempotency_key, created_at
            FROM events
            WHERE job_id = ? AND idempotency_key = ?
            """,
            (job_id, idempotency_key),
        ).fetchone()

    @classmethod
    def _validate_event(cls, event: EventCreate) -> tuple[str, str, str | None]:
        if not isinstance(event, EventCreate):
            raise EventValidationError("Event must use the supported creation contract.")
        cls._validate_text(event.job_id, field="job_id")
        cls._validate_text(event.event_type, field="event_type", maximum_bytes=128)
        idempotency_key = cls._validate_optional_text(
            event.idempotency_key, field="idempotency_key", maximum_bytes=128
        )
        payload, payload_hash = cls._canonical_payload(event.payload)
        return payload, payload_hash, idempotency_key

    @staticmethod
    def _validate_event_id(event_id: object) -> int:
        if not isinstance(event_id, int) or isinstance(event_id, bool) or event_id < 1:
            raise EventValidationError("Event id is invalid.")
        return event_id

    @staticmethod
    def _validate_text(value: object, *, field: str, maximum_bytes: int | None = None) -> str:
        if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
            raise EventValidationError(f"Event {field} is invalid.")
        if maximum_bytes is not None and len(value.encode("utf-8")) > maximum_bytes:
            raise EventValidationError(f"Event {field} is too large.")
        return value

    @classmethod
    def _validate_optional_text(
        cls, value: object, *, field: str, maximum_bytes: int | None = None
    ) -> str | None:
        if value is None:
            return None
        return cls._validate_text(value, field=field, maximum_bytes=maximum_bytes)

    @staticmethod
    def _canonical_payload(payload: object) -> tuple[str, str]:
        if not isinstance(payload, Mapping):
            raise EventValidationError("Event payload must be an object.")
        try:
            source_payload = dict(payload)
            canonical = json.dumps(
                source_payload,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            decoded = json.loads(canonical)
        except (RecursionError, TypeError, ValueError) as error:
            raise EventValidationError("Event payload is invalid.") from error
        if decoded != source_payload:
            raise EventValidationError("Event payload is not JSON canonical.")
        if len(canonical.encode("utf-8")) > 1_048_576:
            raise EventValidationError("Event payload is too large.")
        payload_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return canonical, payload_hash

    @classmethod
    def _to_record(cls, row: sqlite3.Row) -> EventRecord:
        try:
            event_id = cls._validate_event_id(row["id"])
            job_id = cls._validate_text(row["job_id"], field="job_id")
            sequence = row["sequence"]
            if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
                raise EventValidationError("Event sequence is invalid.")
            event_type = cls._validate_text(
                row["event_type"], field="event_type", maximum_bytes=128
            )
            stored_payload = row["payload"]
            if not isinstance(stored_payload, str):
                raise EventValidationError("Event payload is invalid.")
            decoded_payload = json.loads(stored_payload)
            canonical_payload, expected_hash = cls._canonical_payload(decoded_payload)
            payload_hash = row["payload_hash"]
            if canonical_payload != stored_payload or payload_hash != expected_hash:
                raise EventValidationError("Event payload integrity check failed.")
            idempotency_key = cls._validate_optional_text(
                row["idempotency_key"], field="idempotency_key", maximum_bytes=128
            )
            created_at = row["created_at"]
            if not isinstance(created_at, str) or not created_at:
                raise EventValidationError("Event timestamp is invalid.")
            return EventRecord(
                id=event_id,
                job_id=job_id,
                sequence=sequence,
                event_type=event_type,
                payload=decoded_payload,
                payload_hash=payload_hash,
                idempotency_key=idempotency_key,
                created_at=created_at,
            )
        except (
            IndexError,
            KeyError,
            EventValidationError,
            RecursionError,
            TypeError,
            ValueError,
        ) as error:
            raise EventRepositoryError("Stored event data is invalid.") from error


class AtomicTransitionService(_Repository):
    error_type = AtomicTransitionError
    storage_name = "Atomic transition"

    def transition(
        self,
        job: JobUpdate,
        event: EventCreate,
    ) -> AtomicTransitionRecord:
        try:
            JobRepository._validate_update(job)
            payload, payload_hash, idempotency_key = EventRepository._validate_event(event)
        except (JobValidationError, EventValidationError) as error:
            raise AtomicTransitionValidationError(
                "Atomic transition request is invalid."
            ) from error
        if job.id != event.job_id:
            raise AtomicTransitionValidationError(
                "Atomic transition job and event identifiers must match."
            )

        with self._write_connection() as connection:
            try:
                existing_event = EventRepository._matching_idempotent_event(
                    connection,
                    event,
                    payload_hash,
                    idempotency_key,
                )
                if existing_event is not None:
                    return self._resolve_retry(connection, job, existing_event)
                row = JobRepository._select_by_id(connection, job.id)
                if row is None:
                    raise JobNotFoundError("Job does not exist.")
                current_job = JobRepository._to_record(row)
                validate_job_transition(current_job.state, job.state)
                stored_job = JobRepository._update(connection, job)
                stored_event = EventRepository._append(
                    connection,
                    event,
                    payload,
                    payload_hash,
                    idempotency_key,
                )
                return AtomicTransitionRecord(job=stored_job, event=stored_event)
            except JobNotFoundError as error:
                raise AtomicTransitionValidationError(
                    "Atomic transition job does not exist."
                ) from error
            except EventIdempotencyConflictError as error:
                raise AtomicTransitionConflictError(
                    "Atomic transition idempotency key conflicts with committed data."
                ) from error
            except ForbiddenJobTransitionError as error:
                raise AtomicTransitionStateError(
                    "Atomic job state transition is not allowed."
                ) from error
            except sqlite3.IntegrityError as error:
                raise AtomicTransitionError("Atomic transition could not be recorded.") from error
            except (JobRepositoryError, EventRepositoryError) as error:
                raise AtomicTransitionError(
                    "Atomic transition encountered invalid stored data."
                ) from error

    @classmethod
    def _resolve_retry(
        cls,
        connection: sqlite3.Connection,
        requested: JobUpdate,
        event: EventRecord,
    ) -> AtomicTransitionRecord:
        row = JobRepository._select_by_id(connection, requested.id)
        if row is None:
            raise JobNotFoundError("Job does not exist.")
        stored_job = JobRepository._to_record(row)
        if not cls._matches_requested_state(stored_job, requested):
            raise AtomicTransitionConflictError(
                "Atomic transition retry conflicts with current job state."
            )
        return AtomicTransitionRecord(job=stored_job, event=event)

    @staticmethod
    def _matches_requested_state(stored: JobRecord, requested: JobUpdate) -> bool:
        return (
            stored.state == requested.state
            and stored.runtime == requested.runtime
            and stored.model == requested.model
            and stored.worktree_path == requested.worktree_path
        )


class ApprovalRepository(_Repository):
    error_type = ApprovalRepositoryError
    storage_name = "Approval"
    _epoch = datetime(1970, 1, 1, tzinfo=UTC)

    def __init__(
        self,
        database: Database,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(database)
        self.clock = clock or self._utc_now

    def create(self, approval: ApprovalCreate) -> ApprovalRecord:
        payload_hash, expires_at_us = self._validate_approval(approval)
        with self._write_connection() as connection:
            _, now_us = self._read_clock()
            if expires_at_us <= now_us:
                raise ApprovalExpiredError("Approval expiry must be in the future.")
            try:
                connection.execute(
                    """
                    INSERT INTO approvals (
                        id, job_id, payload_hash, expires_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (approval.id, approval.job_id, payload_hash, expires_at_us),
                )
                row = self._select_by_id(connection, approval.id)
                if row is None:
                    raise ApprovalRepositoryError("Approval could not be created.")
                stored = self._to_record(row)
            except sqlite3.IntegrityError as error:
                error_name = getattr(error, "sqlite_errorname", "")
                if error_name in {
                    "SQLITE_CONSTRAINT_PRIMARYKEY",
                    "SQLITE_CONSTRAINT_UNIQUE",
                }:
                    raise ApprovalAlreadyExistsError(
                        "An approval with this id already exists."
                    ) from error
                if error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
                    raise ApprovalValidationError("Approval job does not exist.") from error
                raise ApprovalRepositoryError("Approval could not be created.") from error
        return stored

    def decide(
        self,
        approval_id: str,
        resolution: ApprovalResolution,
    ) -> ApprovalRecord:
        normalized_id = self._validate_text(approval_id, field="id", maximum_bytes=128)
        payload_hash = self._validate_resolution(resolution)
        with self._write_connection() as connection:
            row = self._select_by_id(connection, normalized_id)
            if row is None:
                raise ApprovalNotFoundError("Approval does not exist.")
            stored = self._to_record(row)
            if stored.payload_hash != payload_hash:
                raise ApprovalPayloadMismatchError(
                    "Approval payload does not match the requested action."
                )
            if stored.decision is not None:
                if (
                    stored.decision == resolution.decision
                    and stored.actor == resolution.actor
                    and stored.channel == resolution.channel
                ):
                    return stored
                raise ApprovalDecisionConflictError("Approval already has a different decision.")
            now, now_us = self._read_clock()
            if now_us >= self._datetime_to_epoch_us(stored.expires_at):
                raise ApprovalExpiredError("Approval has expired.")
            try:
                result = connection.execute(
                    """
                    UPDATE approvals
                    SET decision = ?, actor = ?, channel = ?, decided_at = ?
                    WHERE id = ? AND decision IS NULL
                    """,
                    (
                        resolution.decision.value,
                        resolution.actor,
                        resolution.channel,
                        self._format_timestamp(now),
                        normalized_id,
                    ),
                )
                if result.rowcount != 1:
                    raise ApprovalDecisionConflictError("Approval decision could not be recorded.")
                decided = self._select_by_id(connection, normalized_id)
                if decided is None:
                    raise ApprovalRepositoryError("Approval decision could not be recorded.")
                stored = self._to_record(decided)
            except sqlite3.IntegrityError as error:
                raise ApprovalRepositoryError("Approval decision could not be recorded.") from error
        return stored

    def get(self, approval_id: str) -> ApprovalRecord:
        normalized_id = self._validate_text(approval_id, field="id", maximum_bytes=128)
        with self._connection() as connection:
            row = self._select_by_id(connection, normalized_id)
        if row is None:
            raise ApprovalNotFoundError("Approval does not exist.")
        return self._to_record(row)

    def list(self, *, job_id: str | None = None) -> tuple[ApprovalRecord, ...]:
        parameters: tuple[str, ...] = ()
        where = ""
        if job_id is not None:
            parameters = (self._validate_text(job_id, field="job_id"),)
            where = "WHERE job_id = ?"
        with self._connection() as connection:
            rows = connection.execute(
                f"""
                SELECT id, job_id, payload_hash, decision, actor, channel,
                       expires_at, created_at, decided_at
                FROM approvals
                {where}
                ORDER BY created_at, id
                """,
                parameters,
            ).fetchall()
        return tuple(self._to_record(row) for row in rows)

    @staticmethod
    def _select_by_id(connection: sqlite3.Connection, approval_id: str) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT id, job_id, payload_hash, decision, actor, channel,
                   expires_at, created_at, decided_at
            FROM approvals
            WHERE id = ?
            """,
            (approval_id,),
        ).fetchone()

    @classmethod
    def _validate_approval(cls, approval: ApprovalCreate) -> tuple[str, int]:
        if not isinstance(approval, ApprovalCreate):
            raise ApprovalValidationError("Approval must use the supported creation contract.")
        cls._validate_text(approval.id, field="id", maximum_bytes=128)
        cls._validate_text(approval.job_id, field="job_id")
        payload_hash = cls._payload_hash(approval.payload)
        expires_at = cls._normalize_datetime(approval.expires_at, field="expires_at")
        return payload_hash, cls._datetime_to_epoch_us(expires_at)

    @classmethod
    def _validate_resolution(cls, resolution: ApprovalResolution) -> str:
        if not isinstance(resolution, ApprovalResolution):
            raise ApprovalValidationError("Approval must use the supported resolution contract.")
        if not isinstance(resolution.decision, ApprovalDecision):
            raise ApprovalValidationError("Approval decision is invalid.")
        cls._validate_text(resolution.actor, field="actor", maximum_bytes=128)
        cls._validate_text(resolution.channel, field="channel", maximum_bytes=64)
        return cls._payload_hash(resolution.payload)

    @staticmethod
    def _validate_text(
        value: object,
        *,
        field: str,
        maximum_bytes: int | None = None,
    ) -> str:
        if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
            raise ApprovalValidationError(f"Approval {field} is invalid.")
        if maximum_bytes is not None and len(value.encode("utf-8")) > maximum_bytes:
            raise ApprovalValidationError(f"Approval {field} is too large.")
        return value

    @staticmethod
    def _validate_optional_text(
        value: object,
        *,
        field: str,
        maximum_bytes: int,
    ) -> str | None:
        if value is None:
            return None
        return ApprovalRepository._validate_text(value, field=field, maximum_bytes=maximum_bytes)

    @staticmethod
    def _payload_hash(payload: object) -> str:
        if not isinstance(payload, Mapping):
            raise ApprovalValidationError("Approval payload must be an object.")
        try:
            source_payload = dict(payload)
            canonical = json.dumps(
                source_payload,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            decoded = json.loads(canonical)
        except (RecursionError, TypeError, ValueError) as error:
            raise ApprovalValidationError("Approval payload is invalid.") from error
        if decoded != source_payload:
            raise ApprovalValidationError("Approval payload is not JSON canonical.")
        if len(canonical.encode("utf-8")) > 1_048_576:
            raise ApprovalValidationError("Approval payload is too large.")
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def _normalize_datetime(cls, value: object, *, field: str) -> datetime:
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ApprovalValidationError(f"Approval {field} must include a timezone.")
        try:
            offset = value.utcoffset()
            normalized = value.astimezone(UTC)
        except (OverflowError, TypeError, ValueError) as error:
            raise ApprovalValidationError(f"Approval {field} is invalid.") from error
        if offset is None or normalized <= cls._epoch:
            raise ApprovalValidationError(f"Approval {field} is invalid.")
        return normalized

    @classmethod
    def _datetime_to_epoch_us(cls, value: datetime) -> int:
        normalized = cls._normalize_datetime(value, field="timestamp")
        delta = normalized - cls._epoch
        return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds

    @classmethod
    def _epoch_us_to_datetime(cls, value: object) -> datetime:
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ApprovalValidationError("Approval expiry is invalid.")
        try:
            return cls._epoch + timedelta(microseconds=value)
        except OverflowError as error:
            raise ApprovalValidationError("Approval expiry is invalid.") from error

    @classmethod
    def _validate_timestamp(cls, value: object, *, field: str) -> str:
        if not isinstance(value, str) or not value.endswith("Z"):
            raise ApprovalValidationError(f"Approval {field} is invalid.")
        try:
            parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
        except ValueError as error:
            raise ApprovalValidationError(f"Approval {field} is invalid.") from error
        cls._normalize_datetime(parsed, field=field)
        return value

    def _read_clock(self) -> tuple[datetime, int]:
        try:
            now = self._normalize_datetime(self.clock(), field="clock")
        except ApprovalValidationError as error:
            raise ApprovalRepositoryError("Approval clock is invalid.") from error
        return now, self._datetime_to_epoch_us(now)

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(UTC)

    @staticmethod
    def _format_timestamp(value: datetime) -> str:
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")

    @classmethod
    def _to_record(cls, row: sqlite3.Row) -> ApprovalRecord:
        try:
            approval_id = cls._validate_text(row["id"], field="id", maximum_bytes=128)
            job_id = cls._validate_text(row["job_id"], field="job_id")
            payload_hash = row["payload_hash"]
            if (
                not isinstance(payload_hash, str)
                or len(payload_hash) != 64
                or any(character not in "0123456789abcdef" for character in payload_hash)
            ):
                raise ApprovalValidationError("Approval payload hash is invalid.")
            raw_decision = row["decision"]
            decision = None if raw_decision is None else ApprovalDecision(raw_decision)
            actor = cls._validate_optional_text(row["actor"], field="actor", maximum_bytes=128)
            channel = cls._validate_optional_text(row["channel"], field="channel", maximum_bytes=64)
            expires_at = cls._epoch_us_to_datetime(row["expires_at"])
            created_at = cls._validate_timestamp(row["created_at"], field="created_at")
            decided_at_value = row["decided_at"]
            decided_at = (
                None
                if decided_at_value is None
                else cls._validate_timestamp(decided_at_value, field="decided_at")
            )
            if decision is None:
                if actor is not None or channel is not None or decided_at is not None:
                    raise ApprovalValidationError("Approval decision state is invalid.")
            elif actor is None or channel is None or decided_at is None:
                raise ApprovalValidationError("Approval decision state is invalid.")
            return ApprovalRecord(
                id=approval_id,
                job_id=job_id,
                payload_hash=payload_hash,
                decision=decision,
                actor=actor,
                channel=channel,
                expires_at=expires_at,
                created_at=created_at,
                decided_at=decided_at,
            )
        except (
            IndexError,
            KeyError,
            ApprovalValidationError,
            OverflowError,
            TypeError,
            ValueError,
        ) as error:
            raise ApprovalRepositoryError("Stored approval data is invalid.") from error


class RecoveryService:
    _actions: Mapping[JobState, RecoveryAction] = {
        JobState.CREATED: RecoveryAction.SAFE_RESUME,
        JobState.CLASSIFIED: RecoveryAction.SAFE_RESUME,
        JobState.PLANNING: RecoveryAction.SAFE_RESUME,
        JobState.QUEUED: RecoveryAction.SAFE_RESUME,
        JobState.RUNNING: RecoveryAction.RECONCILE_IN_FLIGHT,
        JobState.WAITING_INPUT: RecoveryAction.WAIT_FOR_INPUT,
        JobState.WAITING_APPROVAL: RecoveryAction.WAIT_FOR_APPROVAL,
        JobState.VERIFYING: RecoveryAction.SAFE_RESUME,
        JobState.REVIEW_READY: RecoveryAction.READY_FOR_REVIEW,
        JobState.APPROVED: RecoveryAction.READY_TO_APPLY,
        JobState.APPLYING: RecoveryAction.RECONCILE_IN_FLIGHT,
    }
    _worktree_required = frozenset(
        {
            JobState.RUNNING,
            JobState.WAITING_APPROVAL,
            JobState.VERIFYING,
            JobState.REVIEW_READY,
            JobState.APPROVED,
            JobState.APPLYING,
        }
    )

    def __init__(
        self,
        database: Database,
        worktrees_root: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database = database
        self.worktrees_root = worktrees_root
        self.clock = clock or self._utc_now

    def load(self) -> tuple[RecoveryRecord, ...]:
        root = self._resolve_root()
        now = self._read_clock()
        try:
            jobs = JobRepository(self.database).list()
            approvals = ApprovalRepository(self.database).list()
        except (JobRepositoryError, ApprovalRepositoryError) as error:
            raise RecoveryServiceError("Recovery state could not be loaded safely.") from error

        pending_by_job: dict[str, list[ApprovalRecord]] = {}
        for approval in approvals:
            if approval.decision is None:
                pending_by_job.setdefault(approval.job_id, []).append(approval)

        recovered = []
        for job in jobs:
            if job.state in TERMINAL_JOB_STATES:
                continue
            action = self._actions.get(job.state)
            if action is None:
                raise RecoveryServiceError("Recovery behavior is undefined for a job state.")
            recovered.append(
                self._classify(
                    job,
                    action,
                    tuple(pending_by_job.get(job.id, ())),
                    root,
                    now,
                )
            )
        return tuple(recovered)

    def _classify(
        self,
        job: JobRecord,
        action: RecoveryAction,
        pending: tuple[ApprovalRecord, ...],
        root: Path,
        now: datetime,
    ) -> RecoveryRecord:
        if job.state is JobState.WAITING_APPROVAL:
            if not pending:
                return self._attention(job, RecoveryIssue.APPROVAL_REQUIRED)
            if len(pending) != 1:
                return self._attention(job, RecoveryIssue.MULTIPLE_PENDING_APPROVALS)
            approval = pending[0]
            if approval.expires_at <= now:
                return self._attention(job, RecoveryIssue.APPROVAL_EXPIRED)
        else:
            approval = None
            if pending:
                return self._attention(job, RecoveryIssue.UNEXPECTED_PENDING_APPROVAL)

        worktree, issue = self._resolve_worktree(job, root)
        if issue is not None:
            return self._attention(job, issue, approval=approval)
        return RecoveryRecord(
            job=job,
            action=action,
            worktree=worktree,
            approval=approval,
        )

    def _resolve_worktree(
        self,
        job: JobRecord,
        root: Path,
    ) -> tuple[Path | None, RecoveryIssue | None]:
        if job.worktree_path is None:
            issue = (
                RecoveryIssue.WORKTREE_REQUIRED if job.state in self._worktree_required else None
            )
            return None, issue

        candidate = Path(job.worktree_path)
        if not candidate.is_absolute():
            return None, RecoveryIssue.WORKTREE_OUTSIDE_RUNTIME
        try:
            resolved = candidate.resolve(strict=True)
        except OSError:
            return None, RecoveryIssue.WORKTREE_UNAVAILABLE
        if not resolved.is_relative_to(root):
            return None, RecoveryIssue.WORKTREE_OUTSIDE_RUNTIME
        if not resolved.is_dir():
            return None, RecoveryIssue.WORKTREE_UNAVAILABLE
        return resolved, None

    @staticmethod
    def _attention(
        job: JobRecord,
        issue: RecoveryIssue,
        *,
        approval: ApprovalRecord | None = None,
    ) -> RecoveryRecord:
        return RecoveryRecord(
            job=job,
            action=RecoveryAction.NEEDS_ATTENTION,
            worktree=None,
            approval=approval,
            issue=issue,
        )

    def _resolve_root(self) -> Path:
        try:
            root = self.worktrees_root.resolve(strict=True)
        except OSError as error:
            raise RecoveryServiceError("Recovery worktree storage is unavailable.") from error
        if not root.is_dir():
            raise RecoveryServiceError("Recovery worktree storage is unavailable.")
        return root

    def _read_clock(self) -> datetime:
        try:
            value = self.clock()
            if not isinstance(value, datetime) or value.tzinfo is None:
                raise TypeError
            if value.utcoffset() is None:
                raise ValueError
            return value.astimezone(UTC)
        except (OverflowError, TypeError, ValueError) as error:
            raise RecoveryServiceError("Recovery clock is invalid.") from error

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(UTC)


class PlannerRepository(_Repository):
    error_type = PlannerRepositoryError
    storage_name = "Planner"
    _epoch = datetime(1970, 1, 1, tzinfo=UTC)

    def create(self, item: PlannerItemCreate) -> PlannerItemRecord:
        due_at_us = self._validate_item(item)
        with self._write_connection() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO planner (
                        id, kind, project_id, title, details, due_at,
                        source, source_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item.id,
                        item.kind.value,
                        item.project_id,
                        item.title,
                        item.details,
                        due_at_us,
                        item.source,
                        item.source_key,
                    ),
                )
                row = self._select_by_id(connection, item.id)
                if row is None:
                    raise PlannerRepositoryError("Planner item could not be created.")
                stored = self._to_record(row)
            except sqlite3.IntegrityError as error:
                error_name = getattr(error, "sqlite_errorname", "")
                if error_name in {
                    "SQLITE_CONSTRAINT_PRIMARYKEY",
                    "SQLITE_CONSTRAINT_UNIQUE",
                }:
                    raise PlannerAlreadyExistsError(
                        "A planner item with this id already exists."
                    ) from error
                if error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
                    raise PlannerValidationError("Planner item project does not exist.") from error
                raise PlannerRepositoryError("Planner item could not be created.") from error
        return stored

    def promote(self, item_id: str, job: JobCreate) -> PlannerPromotionRecord:
        normalized_id = self._validate_text(item_id, field="id", maximum_bytes=128)
        try:
            snapshot = JobRepository._validate_job(job)
        except JobValidationError as error:
            raise PlannerValidationError("Planner promotion job is invalid.") from error

        with self._write_connection() as connection:
            row = self._select_by_id(connection, normalized_id)
            if row is None:
                raise PlannerNotFoundError("Planner item does not exist.")
            item = self._to_record(row)
            if item.promoted_job_id is not None:
                return self._resolve_retry(connection, item, job, snapshot)
            if item.project_id is not None and item.project_id != job.project_id:
                raise PlannerValidationError("Planner promotion job must use the item project.")

            try:
                created_job = JobRepository._insert(connection, job, snapshot)
                result = connection.execute(
                    """
                    UPDATE planner
                    SET promoted_job_id = ?,
                        promoted_at = STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        updated_at = STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now')
                    WHERE id = ? AND promoted_job_id IS NULL
                    """,
                    (job.id, normalized_id),
                )
                if result.rowcount != 1:
                    raise PlannerPromotionConflictError("Planner item could not be promoted.")
                promoted_row = self._select_by_id(connection, normalized_id)
                if promoted_row is None:
                    raise PlannerRepositoryError("Planner promotion could not be recorded.")
                promoted = self._to_record(promoted_row)
            except sqlite3.IntegrityError as error:
                error_name = getattr(error, "sqlite_errorname", "")
                if error_name in {
                    "SQLITE_CONSTRAINT_PRIMARYKEY",
                    "SQLITE_CONSTRAINT_UNIQUE",
                }:
                    raise PlannerPromotionConflictError(
                        "Planner promotion conflicts with an existing job."
                    ) from error
                if error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
                    raise PlannerValidationError(
                        "Planner promotion project does not exist."
                    ) from error
                raise PlannerRepositoryError("Planner item could not be promoted.") from error
            except JobRepositoryError as error:
                raise PlannerRepositoryError("Stored promotion job data is invalid.") from error
        return PlannerPromotionRecord(item=promoted, job=created_job)

    def get(self, item_id: str) -> PlannerItemRecord:
        normalized_id = self._validate_text(item_id, field="id", maximum_bytes=128)
        with self._connection() as connection:
            row = self._select_by_id(connection, normalized_id)
        if row is None:
            raise PlannerNotFoundError("Planner item does not exist.")
        return self._to_record(row)

    def list(
        self,
        *,
        kind: PlannerItemKind | None = None,
        project_id: str | None = None,
        promoted: bool | None = None,
    ) -> tuple[PlannerItemRecord, ...]:
        clauses: list[str] = []
        parameters: list[str] = []
        if kind is not None:
            if not isinstance(kind, PlannerItemKind):
                raise PlannerValidationError("Planner item kind is invalid.")
            clauses.append("kind = ?")
            parameters.append(kind.value)
        if project_id is not None:
            clauses.append("project_id = ?")
            parameters.append(self._validate_text(project_id, field="project_id"))
        if promoted is not None:
            if not isinstance(promoted, bool):
                raise PlannerValidationError("Planner promoted filter is invalid.")
            clauses.append("promoted_job_id IS NOT NULL" if promoted else "promoted_job_id IS NULL")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connection() as connection:
            rows = connection.execute(
                f"""
                SELECT id, kind, project_id, title, details, due_at,
                       source, source_key, promoted_job_id, created_at,
                       updated_at, promoted_at
                FROM planner
                {where}
                ORDER BY due_at IS NULL, due_at, created_at, id
                """,
                tuple(parameters),
            ).fetchall()
        return tuple(self._to_record(row) for row in rows)

    @staticmethod
    def _select_by_id(connection: sqlite3.Connection, item_id: str) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT id, kind, project_id, title, details, due_at,
                   source, source_key, promoted_job_id, created_at,
                   updated_at, promoted_at
            FROM planner
            WHERE id = ?
            """,
            (item_id,),
        ).fetchone()

    @classmethod
    def _validate_item(cls, item: PlannerItemCreate) -> int | None:
        if not isinstance(item, PlannerItemCreate):
            raise PlannerValidationError("Planner item must use the supported creation contract.")
        cls._validate_text(item.id, field="id", maximum_bytes=128)
        if not isinstance(item.kind, PlannerItemKind):
            raise PlannerValidationError("Planner item kind is invalid.")
        cls._validate_text(item.title, field="title", maximum_bytes=4096)
        cls._validate_optional_text(item.project_id, field="project_id")
        cls._validate_optional_content(item.details, field="details", maximum_bytes=262_144)
        cls._validate_optional_text(item.source, field="source", maximum_bytes=128)
        cls._validate_optional_text(item.source_key, field="source_key", maximum_bytes=512)
        if item.source_key is not None and item.source is None:
            raise PlannerValidationError("Planner source key requires a source.")
        if item.kind is PlannerItemKind.WATCHER and item.source is None:
            raise PlannerValidationError("Watcher items require a source.")
        if item.kind is PlannerItemKind.DEADLINE and item.due_at is None:
            raise PlannerValidationError("Deadline items require a due time.")
        if item.due_at is None:
            return None
        return cls._datetime_to_epoch_us(cls._normalize_datetime(item.due_at, field="due_at"))

    @classmethod
    def _resolve_retry(
        cls,
        connection: sqlite3.Connection,
        item: PlannerItemRecord,
        job: JobCreate,
        snapshot: str,
    ) -> PlannerPromotionRecord:
        if item.promoted_job_id != job.id:
            raise PlannerPromotionConflictError("Planner item already has a different promotion.")
        row = JobRepository._select_by_id(connection, job.id)
        if row is None:
            raise PlannerRepositoryError("Stored promotion job is unavailable.")
        try:
            stored_job = JobRepository._to_record(row)
        except JobRepositoryError as error:
            raise PlannerRepositoryError("Stored promotion job data is invalid.") from error
        if not cls._job_matches(stored_job, job, snapshot):
            raise PlannerPromotionConflictError("Planner promotion conflicts with stored job data.")
        return PlannerPromotionRecord(item=item, job=stored_job)

    @staticmethod
    def _job_matches(stored: JobRecord, requested: JobCreate, snapshot: str) -> bool:
        return (
            stored.id == requested.id
            and stored.project_id == requested.project_id
            and stored.request == requested.request
            and stored.request_snapshot == json.loads(snapshot)
            and stored.state == requested.state
            and stored.runtime == requested.runtime
            and stored.model == requested.model
            and stored.worktree_path == requested.worktree_path
        )

    @staticmethod
    def _validate_text(
        value: object,
        *,
        field: str,
        maximum_bytes: int | None = None,
    ) -> str:
        if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
            raise PlannerValidationError(f"Planner {field} is invalid.")
        if maximum_bytes is not None and len(value.encode("utf-8")) > maximum_bytes:
            raise PlannerValidationError(f"Planner {field} is too large.")
        return value

    @classmethod
    def _validate_optional_text(
        cls,
        value: object,
        *,
        field: str,
        maximum_bytes: int | None = None,
    ) -> str | None:
        if value is None:
            return None
        return cls._validate_text(value, field=field, maximum_bytes=maximum_bytes)

    @staticmethod
    def _validate_optional_content(
        value: object,
        *,
        field: str,
        maximum_bytes: int,
    ) -> str | None:
        if value is None:
            return None
        if (
            not isinstance(value, str)
            or not value.strip()
            or "\x00" in value
            or len(value.encode("utf-8")) > maximum_bytes
        ):
            raise PlannerValidationError(f"Planner {field} is invalid.")
        return value

    @classmethod
    def _normalize_datetime(cls, value: object, *, field: str) -> datetime:
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise PlannerValidationError(f"Planner {field} must include a timezone.")
        try:
            offset = value.utcoffset()
            normalized = value.astimezone(UTC)
        except (OverflowError, TypeError, ValueError) as error:
            raise PlannerValidationError(f"Planner {field} is invalid.") from error
        if offset is None or normalized <= cls._epoch:
            raise PlannerValidationError(f"Planner {field} is invalid.")
        return normalized

    @classmethod
    def _datetime_to_epoch_us(cls, value: datetime) -> int:
        normalized = cls._normalize_datetime(value, field="timestamp")
        delta = normalized - cls._epoch
        return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds

    @classmethod
    def _epoch_us_to_datetime(cls, value: object) -> datetime:
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise PlannerValidationError("Planner due time is invalid.")
        try:
            return cls._epoch + timedelta(microseconds=value)
        except OverflowError as error:
            raise PlannerValidationError("Planner due time is invalid.") from error

    @classmethod
    def _validate_timestamp(cls, value: object, *, field: str) -> str:
        if not isinstance(value, str) or not value.endswith("Z"):
            raise PlannerValidationError(f"Planner {field} is invalid.")
        try:
            parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
        except ValueError as error:
            raise PlannerValidationError(f"Planner {field} is invalid.") from error
        cls._normalize_datetime(parsed, field=field)
        return value

    @classmethod
    def _to_record(cls, row: sqlite3.Row) -> PlannerItemRecord:
        try:
            due_at_value = row["due_at"]
            due_at = None if due_at_value is None else cls._epoch_us_to_datetime(due_at_value)
            item = PlannerItemCreate(
                id=row["id"],
                kind=PlannerItemKind(row["kind"]),
                title=row["title"],
                project_id=row["project_id"],
                details=row["details"],
                due_at=due_at,
                source=row["source"],
                source_key=row["source_key"],
            )
            cls._validate_item(item)
            promoted_job_id = cls._validate_optional_text(
                row["promoted_job_id"], field="promoted_job_id"
            )
            created_at = cls._validate_timestamp(row["created_at"], field="created_at")
            updated_at = cls._validate_timestamp(row["updated_at"], field="updated_at")
            promoted_at_value = row["promoted_at"]
            promoted_at = (
                None
                if promoted_at_value is None
                else cls._validate_timestamp(promoted_at_value, field="promoted_at")
            )
            if (promoted_job_id is None) != (promoted_at is None):
                raise PlannerValidationError("Planner promotion state is invalid.")
            return PlannerItemRecord(
                id=item.id,
                kind=item.kind,
                title=item.title,
                project_id=item.project_id,
                details=item.details,
                due_at=item.due_at,
                source=item.source,
                source_key=item.source_key,
                promoted_job_id=promoted_job_id,
                created_at=created_at,
                updated_at=updated_at,
                promoted_at=promoted_at,
            )
        except (
            IndexError,
            KeyError,
            PlannerValidationError,
            OverflowError,
            TypeError,
            ValueError,
        ) as error:
            raise PlannerRepositoryError("Stored planner data is invalid.") from error


class UsageRepository(_Repository):
    error_type = UsageRepositoryError
    storage_name = "Usage"
    _epoch = datetime(1970, 1, 1, tzinfo=UTC)
    _maximum_integer = 9_223_372_036_854_775_807

    def record(self, usage: UsageCreate) -> UsageRecord:
        cost, quota_limit, quota_remaining, quota_reset_at = self._validate_usage(usage)
        with self._write_connection() as connection:
            try:
                result = connection.execute(
                    """
                    INSERT INTO usage (
                        provider, job_id, model, input_tokens, output_tokens,
                        cost_usd, latency_ms, quota_limit, quota_remaining,
                        quota_unit, quota_reset_at, rate_limited
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        usage.provider,
                        usage.job_id,
                        usage.model,
                        usage.input_tokens,
                        usage.output_tokens,
                        cost,
                        usage.latency_ms,
                        quota_limit,
                        quota_remaining,
                        usage.quota_unit,
                        quota_reset_at,
                        usage.rate_limited,
                    ),
                )
                row = self._select_by_id(connection, result.lastrowid)
                if row is None:
                    raise UsageRepositoryError("Usage observation could not be recorded.")
                stored = self._to_record(row)
            except sqlite3.IntegrityError as error:
                if getattr(error, "sqlite_errorname", "") == "SQLITE_CONSTRAINT_FOREIGNKEY":
                    raise UsageValidationError("Usage job does not exist.") from error
                raise UsageRepositoryError("Usage observation could not be recorded.") from error
        return stored

    def get(self, usage_id: int) -> UsageRecord:
        normalized_id = self._validate_id(usage_id)
        with self._connection() as connection:
            row = self._select_by_id(connection, normalized_id)
        if row is None:
            raise UsageNotFoundError("Usage observation does not exist.")
        return self._to_record(row)

    def list(
        self,
        *,
        job_id: str | None = None,
        provider: str | None = None,
        model: str | None = None,
    ) -> tuple[UsageRecord, ...]:
        clauses: list[str] = []
        parameters: list[str] = []
        for field, value, maximum_bytes in (
            ("job_id", job_id, 128),
            ("provider", provider, 128),
            ("model", model, 512),
        ):
            if value is not None:
                clauses.append(f"{field} = ?")
                parameters.append(
                    self._validate_text(value, field=field, maximum_bytes=maximum_bytes)
                )
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connection() as connection:
            rows = connection.execute(
                f"""
                SELECT id, provider, job_id, model, input_tokens, output_tokens,
                       cost_usd, latency_ms, quota_limit, quota_remaining,
                       quota_unit, quota_reset_at, rate_limited, recorded_at
                FROM usage
                {where}
                ORDER BY recorded_at, id
                """,
                tuple(parameters),
            ).fetchall()
        return tuple(self._to_record(row) for row in rows)

    @staticmethod
    def _select_by_id(connection: sqlite3.Connection, usage_id: object) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT id, provider, job_id, model, input_tokens, output_tokens,
                   cost_usd, latency_ms, quota_limit, quota_remaining,
                   quota_unit, quota_reset_at, rate_limited, recorded_at
            FROM usage
            WHERE id = ?
            """,
            (usage_id,),
        ).fetchone()

    @classmethod
    def _validate_usage(
        cls,
        usage: UsageCreate,
    ) -> tuple[str | None, str | None, str | None, int | None]:
        if not isinstance(usage, UsageCreate):
            raise UsageValidationError("Usage must use the supported creation contract.")
        cls._validate_text(usage.provider, field="provider", maximum_bytes=128)
        cls._validate_optional_text(usage.job_id, field="job_id", maximum_bytes=128)
        cls._validate_optional_text(usage.model, field="model", maximum_bytes=512)
        cls._validate_optional_integer(usage.input_tokens, field="input_tokens")
        cls._validate_optional_integer(usage.output_tokens, field="output_tokens")
        cls._validate_optional_integer(usage.latency_ms, field="latency_ms")
        cost = cls._validate_optional_decimal(usage.cost_usd, field="cost_usd")
        quota_limit = cls._validate_optional_decimal(usage.quota_limit, field="quota_limit")
        quota_remaining = cls._validate_optional_decimal(
            usage.quota_remaining, field="quota_remaining"
        )
        cls._validate_optional_text(usage.quota_unit, field="quota_unit", maximum_bytes=64)
        has_quota_value = usage.quota_limit is not None or usage.quota_remaining is not None
        if has_quota_value != (usage.quota_unit is not None):
            raise UsageValidationError("Usage quota values require exactly one quota unit.")
        if (
            usage.quota_limit is not None
            and usage.quota_remaining is not None
            and usage.quota_remaining > usage.quota_limit
        ):
            raise UsageValidationError("Usage quota remaining exceeds the observed limit.")
        quota_reset_at = (
            None
            if usage.quota_reset_at is None
            else cls._datetime_to_epoch_us(usage.quota_reset_at)
        )
        if usage.rate_limited is not None and not isinstance(usage.rate_limited, bool):
            raise UsageValidationError("Usage rate_limited must be a boolean or null.")
        observed_values = (
            usage.input_tokens,
            usage.output_tokens,
            usage.cost_usd,
            usage.latency_ms,
            usage.quota_limit,
            usage.quota_remaining,
            usage.quota_reset_at,
            usage.rate_limited,
        )
        if all(value is None for value in observed_values):
            raise UsageValidationError("Usage requires at least one observed value.")
        return cost, quota_limit, quota_remaining, quota_reset_at

    @classmethod
    def _validate_id(cls, value: object) -> int:
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value <= 0
            or value > cls._maximum_integer
        ):
            raise UsageValidationError("Usage id is invalid.")
        return value

    @staticmethod
    def _validate_text(value: object, *, field: str, maximum_bytes: int) -> str:
        if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
            raise UsageValidationError(f"Usage {field} is invalid.")
        if len(value.encode("utf-8")) > maximum_bytes:
            raise UsageValidationError(f"Usage {field} is too large.")
        return value

    @classmethod
    def _validate_optional_text(
        cls,
        value: object,
        *,
        field: str,
        maximum_bytes: int,
    ) -> str | None:
        if value is None:
            return None
        return cls._validate_text(value, field=field, maximum_bytes=maximum_bytes)

    @classmethod
    def _validate_optional_integer(cls, value: object, *, field: str) -> int | None:
        if value is None:
            return None
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            or value > cls._maximum_integer
        ):
            raise UsageValidationError(f"Usage {field} is invalid.")
        return value

    @staticmethod
    def _validate_optional_decimal(value: object, *, field: str) -> str | None:
        if value is None:
            return None
        if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
            raise UsageValidationError(f"Usage {field} is invalid.")
        if len(value.as_tuple().digits) > 100 or not -100 <= value.as_tuple().exponent <= 100:
            raise UsageValidationError(f"Usage {field} is too precise or too large.")
        serialized = format(value, "f")
        if "." in serialized:
            serialized = serialized.rstrip("0").rstrip(".")
        if value.is_zero():
            serialized = "0"
        if len(serialized.encode("utf-8")) > 128:
            raise UsageValidationError(f"Usage {field} is too precise or too large.")
        return serialized

    @classmethod
    def _datetime_to_epoch_us(cls, value: object) -> int:
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise UsageValidationError("Usage quota reset time must include a timezone.")
        try:
            offset = value.utcoffset()
            normalized = value.astimezone(UTC)
        except (OverflowError, TypeError, ValueError) as error:
            raise UsageValidationError("Usage quota reset time is invalid.") from error
        if offset is None or normalized <= cls._epoch:
            raise UsageValidationError("Usage quota reset time is invalid.")
        delta = normalized - cls._epoch
        return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds

    @classmethod
    def _epoch_us_to_datetime(cls, value: object) -> datetime:
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise UsageValidationError("Usage quota reset time is invalid.")
        try:
            return cls._epoch + timedelta(microseconds=value)
        except OverflowError as error:
            raise UsageValidationError("Usage quota reset time is invalid.") from error

    @classmethod
    def _parse_decimal(cls, value: object, *, field: str) -> Decimal | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise UsageValidationError(f"Usage {field} is invalid.")
        try:
            parsed = Decimal(value)
        except InvalidOperation as error:
            raise UsageValidationError(f"Usage {field} is invalid.") from error
        if cls._validate_optional_decimal(parsed, field=field) != value:
            raise UsageValidationError(f"Usage {field} is not canonical.")
        return parsed

    @classmethod
    def _validate_timestamp(cls, value: object) -> str:
        if not isinstance(value, str) or not value.endswith("Z"):
            raise UsageValidationError("Usage recorded_at is invalid.")
        try:
            parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
        except ValueError as error:
            raise UsageValidationError("Usage recorded_at is invalid.") from error
        if parsed <= cls._epoch:
            raise UsageValidationError("Usage recorded_at is invalid.")
        return value

    @classmethod
    def _to_record(cls, row: sqlite3.Row) -> UsageRecord:
        try:
            quota_reset_at = (
                None
                if row["quota_reset_at"] is None
                else cls._epoch_us_to_datetime(row["quota_reset_at"])
            )
            rate_limited_value = row["rate_limited"]
            if rate_limited_value not in (None, 0, 1):
                raise UsageValidationError("Usage rate_limited is invalid.")
            usage = UsageCreate(
                provider=row["provider"],
                job_id=row["job_id"],
                model=row["model"],
                input_tokens=row["input_tokens"],
                output_tokens=row["output_tokens"],
                cost_usd=cls._parse_decimal(row["cost_usd"], field="cost_usd"),
                latency_ms=row["latency_ms"],
                quota_limit=cls._parse_decimal(row["quota_limit"], field="quota_limit"),
                quota_remaining=cls._parse_decimal(row["quota_remaining"], field="quota_remaining"),
                quota_unit=row["quota_unit"],
                quota_reset_at=quota_reset_at,
                rate_limited=(None if rate_limited_value is None else bool(rate_limited_value)),
            )
            cls._validate_usage(usage)
            return UsageRecord(
                id=cls._validate_id(row["id"]),
                provider=usage.provider,
                job_id=usage.job_id,
                model=usage.model,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cost_usd=usage.cost_usd,
                latency_ms=usage.latency_ms,
                quota_limit=usage.quota_limit,
                quota_remaining=usage.quota_remaining,
                quota_unit=usage.quota_unit,
                quota_reset_at=usage.quota_reset_at,
                rate_limited=usage.rate_limited,
                recorded_at=cls._validate_timestamp(row["recorded_at"]),
            )
        except (
            IndexError,
            KeyError,
            TypeError,
            ValueError,
            UsageValidationError,
        ) as error:
            raise UsageRepositoryError("Stored usage data is invalid.") from error


class MemoryReferenceRepository(_Repository):
    error_type = MemoryReferenceRepositoryError
    storage_name = "Memory reference"
    _epoch = datetime(1970, 1, 1, tzinfo=UTC)
    _maximum_integer = 9_223_372_036_854_775_807

    def link(self, reference: MemoryReferenceCreate) -> MemoryReferenceRecord:
        self._validate_reference(reference)
        with self._write_connection() as connection:
            existing = self._select_exact(connection, reference)
            if existing is not None:
                return self._to_record(existing)
            try:
                result = connection.execute(
                    """
                    INSERT INTO memory_refs (projmem_record_id, job_id, event_id)
                    VALUES (?, ?, ?)
                    """,
                    (
                        reference.projmem_record_id,
                        reference.job_id,
                        reference.event_id,
                    ),
                )
                row = self._select_by_id(connection, result.lastrowid)
                if row is None:
                    raise MemoryReferenceRepositoryError("Memory reference could not be linked.")
                stored = self._to_record(row)
            except sqlite3.IntegrityError as error:
                error_name = getattr(error, "sqlite_errorname", "")
                if error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
                    raise MemoryReferenceValidationError(
                        "Memory reference job or event provenance does not exist."
                    ) from error
                raise MemoryReferenceRepositoryError(
                    "Memory reference could not be linked."
                ) from error
        return stored

    def get(self, reference_id: int) -> MemoryReferenceRecord:
        normalized_id = self._validate_id(reference_id)
        with self._connection() as connection:
            row = self._select_by_id(connection, normalized_id)
        if row is None:
            raise MemoryReferenceNotFoundError("Memory reference does not exist.")
        return self._to_record(row)

    def list(
        self,
        *,
        job_id: str | None = None,
        event_id: int | None = None,
        projmem_record_id: str | None = None,
    ) -> tuple[MemoryReferenceRecord, ...]:
        clauses: list[str] = []
        parameters: list[object] = []
        if job_id is not None:
            clauses.append("job_id = ?")
            parameters.append(self._validate_text(job_id, field="job_id", maximum_bytes=None))
        if event_id is not None:
            clauses.append("event_id = ?")
            parameters.append(self._validate_id(event_id))
        if projmem_record_id is not None:
            clauses.append("projmem_record_id = ?")
            parameters.append(
                self._validate_text(
                    projmem_record_id,
                    field="projmem_record_id",
                    maximum_bytes=512,
                )
            )
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connection() as connection:
            rows = connection.execute(
                f"""
                SELECT id, projmem_record_id, job_id, event_id, created_at
                FROM memory_refs
                {where}
                ORDER BY created_at, id
                """,
                tuple(parameters),
            ).fetchall()
        return tuple(self._to_record(row) for row in rows)

    @staticmethod
    def _select_by_id(
        connection: sqlite3.Connection,
        reference_id: object,
    ) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT id, projmem_record_id, job_id, event_id, created_at
            FROM memory_refs
            WHERE id = ?
            """,
            (reference_id,),
        ).fetchone()

    @staticmethod
    def _select_exact(
        connection: sqlite3.Connection,
        reference: MemoryReferenceCreate,
    ) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT id, projmem_record_id, job_id, event_id, created_at
            FROM memory_refs
            WHERE projmem_record_id = ? AND job_id = ? AND event_id IS ?
            """,
            (reference.projmem_record_id, reference.job_id, reference.event_id),
        ).fetchone()

    @classmethod
    def _validate_reference(cls, reference: MemoryReferenceCreate) -> None:
        if not isinstance(reference, MemoryReferenceCreate):
            raise MemoryReferenceValidationError(
                "Memory reference must use the supported creation contract."
            )
        cls._validate_text(
            reference.projmem_record_id,
            field="projmem_record_id",
            maximum_bytes=512,
        )
        cls._validate_text(reference.job_id, field="job_id", maximum_bytes=None)
        if reference.event_id is not None:
            cls._validate_id(reference.event_id)

    @classmethod
    def _validate_id(cls, value: object) -> int:
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value <= 0
            or value > cls._maximum_integer
        ):
            raise MemoryReferenceValidationError("Memory reference id is invalid.")
        return value

    @staticmethod
    def _validate_text(
        value: object,
        *,
        field: str,
        maximum_bytes: int | None,
    ) -> str:
        if not isinstance(value, str) or not value or value != value.strip() or "\x00" in value:
            raise MemoryReferenceValidationError(f"Memory reference {field} is invalid.")
        if maximum_bytes is not None and len(value.encode("utf-8")) > maximum_bytes:
            raise MemoryReferenceValidationError(f"Memory reference {field} is too large.")
        return value

    @classmethod
    def _validate_timestamp(cls, value: object) -> str:
        if not isinstance(value, str) or not value.endswith("Z"):
            raise MemoryReferenceValidationError("Memory reference created_at is invalid.")
        try:
            parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
        except ValueError as error:
            raise MemoryReferenceValidationError(
                "Memory reference created_at is invalid."
            ) from error
        if parsed <= cls._epoch:
            raise MemoryReferenceValidationError("Memory reference created_at is invalid.")
        return value

    @classmethod
    def _to_record(cls, row: sqlite3.Row) -> MemoryReferenceRecord:
        try:
            reference = MemoryReferenceCreate(
                projmem_record_id=row["projmem_record_id"],
                job_id=row["job_id"],
                event_id=row["event_id"],
            )
            cls._validate_reference(reference)
            return MemoryReferenceRecord(
                id=cls._validate_id(row["id"]),
                projmem_record_id=reference.projmem_record_id,
                job_id=reference.job_id,
                event_id=reference.event_id,
                created_at=cls._validate_timestamp(row["created_at"]),
            )
        except (
            IndexError,
            KeyError,
            TypeError,
            ValueError,
            MemoryReferenceValidationError,
        ) as error:
            raise MemoryReferenceRepositoryError(
                "Stored memory reference data is invalid."
            ) from error
