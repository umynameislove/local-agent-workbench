from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from db import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    Database,
    DatabaseError,
    DatabaseVersionError,
    Migration,
    MigrationApplyError,
    MigrationDefinitionError,
    ProjectAlreadyExistsError,
    ProjectNotFoundError,
    ProjectRepository,
    ProjectRepositoryError,
    ProjectValidationError,
)
from engine import PermissionMode, ProjectConfig, Sensitivity


def migration(version: int, name: str, *statements: str) -> Migration:
    return Migration(version=version, name=name, statements=tuple(statements))


def project(
    project_id: str = "alpha",
    *,
    root: str = "/workspace/alpha",
    sensitivity: Sensitivity = Sensitivity.PRIVATE,
    cloud_allowed: bool = False,
    permission_mode: PermissionMode = PermissionMode.SANDBOXED_WRITE,
) -> ProjectConfig:
    return ProjectConfig(
        id=project_id,
        root=root,
        sensitivity=sensitivity,
        cloud_allowed=cloud_allowed,
        permission_mode=permission_mode,
    )


def initialized_project_repository(tmp_path: Path) -> ProjectRepository:
    database = Database(tmp_path / "state.db")
    database.initialize()
    return ProjectRepository(database)


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

    assert version == LATEST_SCHEMA_VERSION
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT singleton, version FROM schema_version").fetchone()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
    assert row == (1, LATEST_SCHEMA_VERSION)
    assert integrity == ("ok",)


def test_initialize_is_idempotent(tmp_path: Path) -> None:
    database = Database(tmp_path / "state.db")

    assert database.initialize() == LATEST_SCHEMA_VERSION
    first_objects = schema_objects(database.path)
    assert database.initialize() == LATEST_SCHEMA_VERSION

    assert schema_objects(database.path) == first_objects


def test_staged_upgrade_matches_fresh_database(tmp_path: Path) -> None:
    alpha_version = LATEST_SCHEMA_VERSION + 1
    alpha_name_version = alpha_version + 1
    migrations = (
        *MIGRATIONS,
        migration(
            alpha_version,
            "create_alpha",
            "CREATE TABLE alpha (id INTEGER PRIMARY KEY)",
        ),
        migration(
            alpha_name_version,
            "add_alpha_name",
            "ALTER TABLE alpha ADD COLUMN name TEXT NOT NULL DEFAULT ''",
        ),
    )
    upgraded_path = tmp_path / "upgraded.db"
    fresh_path = tmp_path / "fresh.db"

    assert Database(upgraded_path, migrations=migrations[:1]).initialize() == 1
    assert Database(upgraded_path, migrations=migrations).initialize() == alpha_name_version
    assert Database(fresh_path, migrations=migrations).initialize() == alpha_name_version

    assert schema_objects(upgraded_path) == schema_objects(fresh_path)


def test_failed_migration_rolls_back_the_complete_run(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    broken_version = LATEST_SCHEMA_VERSION + 1
    migrations = (
        *MIGRATIONS,
        migration(
            broken_version,
            "broken_change",
            "CREATE TABLE partial_change (id INTEGER PRIMARY KEY)",
            "THIS IS NOT VALID SQL",
        ),
    )

    with pytest.raises(MigrationApplyError, match=f"Migration {broken_version} failed"):
        Database(path, migrations=migrations).initialize()

    assert schema_objects(path) == []


def test_failed_upgrade_preserves_previous_schema(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    assert Database(path).initialize() == LATEST_SCHEMA_VERSION
    original_objects = schema_objects(path)
    migrations = (
        *MIGRATIONS,
        migration(
            LATEST_SCHEMA_VERSION + 1,
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
    assert version == (LATEST_SCHEMA_VERSION,)


def test_newer_database_version_fails_closed_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    assert Database(path).initialize() == LATEST_SCHEMA_VERSION
    newer_version = LATEST_SCHEMA_VERSION + 1
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE schema_version SET version = ? WHERE singleton = 1",
            (newer_version,),
        )

    with pytest.raises(DatabaseVersionError, match="newer"):
        Database(path).initialize()

    with sqlite3.connect(path) as connection:
        version = connection.execute("SELECT version FROM schema_version").fetchone()
    assert version == (newer_version,)


@pytest.mark.parametrize(
    "statement",
    [
        "DELETE FROM schema_version",
        "UPDATE schema_version SET singleton = 0",
    ],
)
def test_invalid_schema_metadata_fails_closed(tmp_path: Path, statement: str) -> None:
    path = tmp_path / "state.db"
    assert Database(path).initialize() == LATEST_SCHEMA_VERSION
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


def test_project_crud_round_trip_preserves_identity_root_and_policy(tmp_path: Path) -> None:
    repository = initialized_project_repository(tmp_path)
    original = project(
        sensitivity=Sensitivity.PRIVATE,
        cloud_allowed=True,
        permission_mode=PermissionMode.READ_ONLY,
    )

    created = repository.create(original)

    assert created.to_config() == original
    assert created.created_at
    assert created.updated_at
    assert repository.get(original.id) == created

    changed = project(
        root="/workspace/alpha-renamed",
        sensitivity=Sensitivity.PUBLIC,
        cloud_allowed=True,
        permission_mode=PermissionMode.SANDBOXED_WRITE,
    )
    updated = repository.update(changed)

    assert updated.to_config() == changed
    assert updated.created_at == created.created_at
    assert repository.list() == (updated,)

    repository.delete(original.id)

    assert repository.list() == ()
    with pytest.raises(ProjectNotFoundError, match="does not exist"):
        repository.get(original.id)


def test_project_list_has_deterministic_id_order(tmp_path: Path) -> None:
    repository = initialized_project_repository(tmp_path)
    repository.create(project("zeta", root="/workspace/zeta"))
    repository.create(project("alpha", root="/workspace/alpha"))

    assert [stored.id for stored in repository.list()] == ["alpha", "zeta"]


@pytest.mark.parametrize(
    ("sensitivity", "cloud_allowed", "permission_mode"),
    [
        (Sensitivity.PUBLIC, False, PermissionMode.READ_ONLY),
        (Sensitivity.PUBLIC, True, PermissionMode.SANDBOXED_WRITE),
        (Sensitivity.PRIVATE, False, PermissionMode.NEVER),
        (Sensitivity.PRIVATE, True, PermissionMode.READ_ONLY),
        (Sensitivity.INTERNAL, False, PermissionMode.SANDBOXED_WRITE),
        (Sensitivity.RESTRICTED, False, PermissionMode.NEVER),
    ],
)
def test_supported_sensitivity_policy_matrix_round_trips(
    tmp_path: Path,
    sensitivity: Sensitivity,
    cloud_allowed: bool,
    permission_mode: PermissionMode,
) -> None:
    repository = initialized_project_repository(tmp_path)
    expected = project(
        sensitivity=sensitivity,
        cloud_allowed=cloud_allowed,
        permission_mode=permission_mode,
    )

    assert repository.create(expected).to_config() == expected


def test_project_survives_database_restart(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    database = Database(path)
    database.initialize()
    expected = project(
        sensitivity=Sensitivity.INTERNAL,
        permission_mode=PermissionMode.NEVER,
    )
    ProjectRepository(database).create(expected)

    restarted_database = Database(path)
    restarted_database.initialize()

    assert ProjectRepository(restarted_database).get(expected.id).to_config() == expected


@pytest.mark.parametrize(
    "duplicate",
    [
        project("alpha", root="/workspace/other"),
        project("other", root="/workspace/alpha"),
    ],
)
def test_duplicate_project_identity_or_root_is_rejected_atomically(
    tmp_path: Path,
    duplicate: ProjectConfig,
) -> None:
    repository = initialized_project_repository(tmp_path)
    original = repository.create(project())

    with pytest.raises(ProjectAlreadyExistsError, match="id or root"):
        repository.create(duplicate)

    assert repository.list() == (original,)


def test_conflicting_project_update_rolls_back(tmp_path: Path) -> None:
    repository = initialized_project_repository(tmp_path)
    alpha = repository.create(project())
    repository.create(project("beta", root="/workspace/beta"))

    with pytest.raises(ProjectAlreadyExistsError, match="id or root"):
        repository.update(project(root="/workspace/beta"))

    assert repository.get("alpha") == alpha


@pytest.mark.parametrize("operation", ["get", "update", "delete"])
def test_missing_project_operations_fail_explicitly(tmp_path: Path, operation: str) -> None:
    repository = initialized_project_repository(tmp_path)

    with pytest.raises(ProjectNotFoundError, match="does not exist"):
        if operation == "get":
            repository.get("missing")
        elif operation == "update":
            repository.update(project("missing", root="/workspace/missing"))
        else:
            repository.delete("missing")


@pytest.mark.parametrize(
    "invalid",
    [
        project(""),
        project(" alpha"),
        project("alpha\x00beta"),
        project(root=""),
        project(root=" /workspace/alpha"),
        ProjectConfig(
            id="alpha",
            root="/workspace/alpha",
            sensitivity="private",  # type: ignore[arg-type]
            cloud_allowed=False,
            permission_mode=PermissionMode.READ_ONLY,
        ),
        ProjectConfig(
            id="alpha",
            root="/workspace/alpha",
            sensitivity=Sensitivity.PRIVATE,
            cloud_allowed=1,  # type: ignore[arg-type]
            permission_mode=PermissionMode.READ_ONLY,
        ),
        ProjectConfig(
            id="alpha",
            root="/workspace/alpha",
            sensitivity=Sensitivity.PRIVATE,
            cloud_allowed=False,
            permission_mode="read-only",  # type: ignore[arg-type]
        ),
    ],
)
def test_invalid_project_contract_is_rejected_before_write(
    tmp_path: Path,
    invalid: ProjectConfig,
) -> None:
    repository = initialized_project_repository(tmp_path)

    with pytest.raises(ProjectValidationError):
        repository.create(invalid)

    assert repository.list() == ()


@pytest.mark.parametrize("sensitivity", [Sensitivity.INTERNAL, Sensitivity.RESTRICTED])
def test_sensitive_project_cannot_enable_cloud(
    tmp_path: Path,
    sensitivity: Sensitivity,
) -> None:
    repository = initialized_project_repository(tmp_path)

    with pytest.raises(ProjectValidationError, match="conflicts with sensitivity"):
        repository.create(project(sensitivity=sensitivity, cloud_allowed=True))

    assert repository.list() == ()


def test_database_constraint_rejects_sensitive_cloud_policy(tmp_path: Path) -> None:
    database = Database(tmp_path / "state.db")
    database.initialize()

    with sqlite3.connect(database.path) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO projects (
                id, root, sensitivity, cloud_allowed, permission_mode
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("alpha", "/workspace/alpha", "restricted", 1, "never"),
        )


@pytest.mark.parametrize(
    "values",
    [
        (" alpha", "/workspace/alpha", "private", 0, "read-only"),
        ("alpha", " /workspace/alpha", "private", 0, "read-only"),
        ("alpha", "/workspace/alpha", "unknown", 0, "read-only"),
        ("alpha", "/workspace/alpha", "private", 2, "read-only"),
        ("alpha", "/workspace/alpha", "private", 0, "unrestricted"),
    ],
)
def test_database_constraints_reject_invalid_project_rows(
    tmp_path: Path,
    values: tuple[object, ...],
) -> None:
    database = Database(tmp_path / "state.db")
    database.initialize()

    with sqlite3.connect(database.path) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO projects (
                id, root, sensitivity, cloud_allowed, permission_mode
            ) VALUES (?, ?, ?, ?, ?)
            """,
            values,
        )


def test_project_queries_treat_identity_as_data(tmp_path: Path) -> None:
    repository = initialized_project_repository(tmp_path)
    unusual_id = "alpha'); DROP TABLE projects; SELECT ('"
    stored = repository.create(project(unusual_id))

    assert repository.get(unusual_id) == stored
    assert repository.list() == (stored,)


def test_repository_fails_safely_before_schema_initialization(tmp_path: Path) -> None:
    repository = ProjectRepository(Database(tmp_path / "state.db"))

    with pytest.raises(ProjectRepositoryError, match="storage is unavailable") as failure:
        repository.list()

    assert str(tmp_path) not in str(failure.value)
    assert not repository.database.path.exists()


def test_corrupted_stored_project_is_not_returned(tmp_path: Path) -> None:
    repository = initialized_project_repository(tmp_path)
    repository.create(project())
    with sqlite3.connect(repository.database.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute("UPDATE projects SET cloud_allowed = 2 WHERE id = 'alpha'")

    with pytest.raises(ProjectRepositoryError, match="Stored project data is invalid"):
        repository.get("alpha")
