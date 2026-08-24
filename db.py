from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from engine import PermissionMode, ProjectConfig, Sensitivity


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


class ProjectRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

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

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if not self.database.path.is_file():
            raise ProjectRepositoryError("Project storage is unavailable.")
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                self.database.path,
                timeout=5.0,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
        except sqlite3.Error as error:
            if connection is not None:
                connection.close()
            raise ProjectRepositoryError("Project storage is unavailable.") from error

        try:
            yield connection
        except sqlite3.Error as error:
            raise ProjectRepositoryError("Project storage operation failed.") from error
        finally:
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
