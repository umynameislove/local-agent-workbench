from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


class DatabaseError(RuntimeError):
    """Base error for database initialization failures."""


class MigrationDefinitionError(DatabaseError):
    """Raised when embedded migrations are not a valid sequence."""


class DatabaseVersionError(DatabaseError):
    """Raised when stored schema metadata is invalid or unsupported."""


class MigrationApplyError(DatabaseError):
    """Raised when a migration cannot be applied atomically."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    statements: tuple[str, ...]


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
