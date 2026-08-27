from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from engine import (
    ApprovalCreate,
    ApprovalDecision,
    ApprovalResolution,
    EventCreate,
    JobCreate,
    JobRuntime,
    JobState,
    JobUpdate,
    PermissionMode,
    PlannerItemCreate,
    PlannerItemKind,
    ProjectConfig,
    Sensitivity,
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
                row = self._select_by_id(connection, job.id)
                if row is None:
                    raise JobRepositoryError("Job could not be created.")
                stored = self._to_record(row)
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
            row = self._select_by_id(connection, job.id)
            if row is None:
                raise JobRepositoryError("Job could not be updated.")
            stored = self._to_record(row)
        return stored

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
                if idempotency_key is not None:
                    existing = self._select_by_idempotency_key(
                        connection, event.job_id, idempotency_key
                    )
                    if existing is not None:
                        stored = self._to_record(existing)
                        if (
                            stored.event_type != event.event_type
                            or stored.payload_hash != payload_hash
                        ):
                            raise EventIdempotencyConflictError(
                                "Event idempotency key conflicts with stored data."
                            )
                        return stored
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
                row = self._select_by_id(connection, event_id)
                if row is None:
                    raise EventRepositoryError("Event could not be appended.")
                stored = self._to_record(row)
            except sqlite3.IntegrityError as error:
                error_name = getattr(error, "sqlite_errorname", "")
                if error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
                    raise EventValidationError("Event job does not exist.") from error
                raise EventRepositoryError("Event could not be appended.") from error
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
