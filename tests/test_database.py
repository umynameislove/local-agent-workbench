from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from db import (
    MIGRATIONS,
    Database,
    DatabaseError,
    DatabaseVersionError,
    Migration,
    MigrationApplyError,
    MigrationDefinitionError,
)


def migration(version: int, name: str, *statements: str) -> Migration:
    return Migration(version=version, name=name, statements=tuple(statements))


def schema_objects(path: Path) -> list[tuple[str, str, str]]:
    with sqlite3.connect(path) as connection:
        return connection.execute(
            """
            SELECT type, name, sql
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            ORDER BY type, name
            """
        ).fetchall()


def test_fresh_database_reaches_latest_schema_version(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    database = Database(path)

    version = database.initialize()

    assert version == 1
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT singleton, version FROM schema_version").fetchone()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
    assert row == (1, 1)
    assert integrity == ("ok",)


def test_initialize_is_idempotent(tmp_path: Path) -> None:
    database = Database(tmp_path / "state.db")

    assert database.initialize() == 1
    first_objects = schema_objects(database.path)
    assert database.initialize() == 1

    assert schema_objects(database.path) == first_objects


def test_staged_upgrade_matches_fresh_database(tmp_path: Path) -> None:
    migrations = (
        *MIGRATIONS,
        migration(2, "create_alpha", "CREATE TABLE alpha (id INTEGER PRIMARY KEY)"),
        migration(
            3,
            "add_alpha_name",
            "ALTER TABLE alpha ADD COLUMN name TEXT NOT NULL DEFAULT ''",
        ),
    )
    upgraded_path = tmp_path / "upgraded.db"
    fresh_path = tmp_path / "fresh.db"

    assert Database(upgraded_path, migrations=migrations[:1]).initialize() == 1
    assert Database(upgraded_path, migrations=migrations).initialize() == 3
    assert Database(fresh_path, migrations=migrations).initialize() == 3

    assert schema_objects(upgraded_path) == schema_objects(fresh_path)


def test_failed_migration_rolls_back_the_complete_run(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    migrations = (
        *MIGRATIONS,
        migration(
            2,
            "broken_change",
            "CREATE TABLE partial_change (id INTEGER PRIMARY KEY)",
            "THIS IS NOT VALID SQL",
        ),
    )

    with pytest.raises(MigrationApplyError, match="Migration 2 failed"):
        Database(path, migrations=migrations).initialize()

    assert schema_objects(path) == []


def test_failed_upgrade_preserves_previous_schema(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    assert Database(path).initialize() == 1
    original_objects = schema_objects(path)
    migrations = (
        *MIGRATIONS,
        migration(
            2,
            "broken_upgrade",
            "CREATE TABLE partial_change (id INTEGER PRIMARY KEY)",
            "THIS IS NOT VALID SQL",
        ),
    )

    with pytest.raises(MigrationApplyError):
        Database(path, migrations=migrations).initialize()

    assert schema_objects(path) == original_objects
    with sqlite3.connect(path) as connection:
        version = connection.execute("SELECT version FROM schema_version").fetchone()
    assert version == (1,)


def test_newer_database_version_fails_closed_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    assert Database(path).initialize() == 1
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE schema_version SET version = 2 WHERE singleton = 1")

    with pytest.raises(DatabaseVersionError, match="newer"):
        Database(path).initialize()

    with sqlite3.connect(path) as connection:
        version = connection.execute("SELECT version FROM schema_version").fetchone()
    assert version == (2,)


@pytest.mark.parametrize(
    "statement",
    [
        "DELETE FROM schema_version",
        "UPDATE schema_version SET singleton = 0",
    ],
)
def test_invalid_schema_metadata_fails_closed(tmp_path: Path, statement: str) -> None:
    path = tmp_path / "state.db"
    assert Database(path).initialize() == 1
    with sqlite3.connect(path) as connection:
        if statement.startswith("UPDATE"):
            connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(statement)

    with pytest.raises(DatabaseVersionError, match="metadata is invalid"):
        Database(path).initialize()


def test_invalid_schema_metadata_shape_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE schema_version (singleton INTEGER PRIMARY KEY, version INTEGER)"
        )
        connection.execute("INSERT INTO schema_version VALUES (1, 1)")

    with pytest.raises(DatabaseVersionError, match="metadata is invalid"):
        Database(path).initialize()


@pytest.mark.parametrize(
    "migrations",
    [
        (),
        (migration(2, "starts_late", "SELECT 1"),),
        (migration(True, "boolean_version", "SELECT 1"),),
        (
            migration(1, "duplicate", "SELECT 1"),
            migration(2, "duplicate", "SELECT 1"),
        ),
        (migration(1, "", "SELECT 1"),),
        (migration(1, "empty"),),
    ],
)
def test_invalid_migration_definitions_are_rejected(
    tmp_path: Path, migrations: tuple[Migration, ...]
) -> None:
    with pytest.raises(MigrationDefinitionError):
        Database(tmp_path / "state.db", migrations=migrations)


def test_database_parent_must_exist(tmp_path: Path) -> None:
    database = Database(tmp_path / "missing" / "state.db")

    with pytest.raises(DatabaseError, match="parent directory"):
        database.initialize()

    assert not database.path.exists()


def test_database_open_error_uses_safe_public_message(tmp_path: Path) -> None:
    database = Database(tmp_path)

    with pytest.raises(DatabaseError, match="Database could not be opened") as failure:
        database.initialize()

    assert str(tmp_path) not in str(failure.value)
