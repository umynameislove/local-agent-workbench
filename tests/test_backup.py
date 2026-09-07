from __future__ import annotations

import os
import shutil
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from db import (
    ApprovalRepository,
    BackupService,
    BackupServiceError,
    Database,
    JobRepository,
    ProjectRepository,
)
from engine import ApprovalCreate, JobCreate, PermissionMode, ProjectConfig, Sensitivity


def populated(tmp_path: Path) -> Database:
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(
        ProjectConfig(
            "sample",
            "/workspace/sample",
            Sensitivity.PRIVATE,
            False,
            PermissionMode.SANDBOXED_WRITE,
        )
    )
    JobRepository(database).create(JobCreate("job", "sample", "Preserve this request", {}))
    ApprovalRepository(database).create(
        ApprovalCreate(
            "approval", "job", {"operation": "review"}, datetime.now(UTC) + timedelta(hours=1)
        )
    )
    return database


def test_restore_preserves_records_and_is_independent(tmp_path: Path) -> None:
    database = populated(tmp_path)
    snapshot = tmp_path / "backup.db"
    assert BackupService(database).create(snapshot) == snapshot
    assert snapshot.stat().st_mode & 0o777 == 0o600
    restored_home = tmp_path / "restored"
    restored_home.mkdir()
    shutil.copyfile(snapshot, restored_home / "state.db")
    restored = Database(restored_home / "state.db")
    restored.initialize()
    assert ProjectRepository(restored).list() == ProjectRepository(database).list()
    assert JobRepository(restored).list() == JobRepository(database).list()
    assert ApprovalRepository(restored).list() == ApprovalRepository(database).list()
    JobRepository(restored).create(JobCreate("new", "sample", "Independent write", {}))
    assert len(JobRepository(database).list()) == 1
    with sqlite3.connect(snapshot) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)


def test_wal_snapshot_contains_commits_but_not_uncommitted_data(tmp_path: Path) -> None:
    database = populated(tmp_path)
    connection = sqlite3.connect(database.path, isolation_level=None)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("CREATE TABLE probe (value TEXT)")
        connection.execute("INSERT INTO probe VALUES ('committed')")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("INSERT INTO probe VALUES ('pending')")
        snapshot = BackupService(database).create(tmp_path / "wal-copy.db")
        with sqlite3.connect(snapshot) as restored:
            assert restored.execute("SELECT value FROM probe").fetchall() == [("committed",)]
        assert connection.in_transaction
        connection.execute("ROLLBACK")
    finally:
        connection.close()


@pytest.mark.parametrize("kind", ["existing", "source", "symlink", "hardlink", "sidecar"])
def test_conflicts_preserve_existing_data(tmp_path: Path, kind: str) -> None:
    database = populated(tmp_path)
    destination = tmp_path / "backup.db"
    before = database.path.read_bytes()
    if kind == "existing":
        destination.write_bytes(b"keep")
    elif kind == "source":
        destination = database.path
    elif kind == "symlink":
        destination.symlink_to(database.path)
    elif kind == "hardlink":
        os.link(database.path, destination)
    else:
        Path(f"{destination}-wal").write_bytes(b"keep")
    with pytest.raises(BackupServiceError):
        BackupService(database).create(destination)
    assert database.path.read_bytes() == before
    if kind == "existing":
        assert destination.read_bytes() == b"keep"
    if kind == "sidecar":
        assert Path(f"{destination}-wal").read_bytes() == b"keep"
    assert not list(tmp_path.glob(".*.tmp*"))


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_source_sidecar_names_are_reserved(tmp_path: Path, suffix: str) -> None:
    database = populated(tmp_path)
    with pytest.raises(BackupServiceError):
        BackupService(database).create(Path(f"{database.path}{suffix}"))


@pytest.mark.parametrize("kind", ["missing", "corrupt", "future", "foreign_key"])
def test_invalid_sources_never_publish(tmp_path: Path, kind: str) -> None:
    database = populated(tmp_path)
    if kind == "missing":
        database = Database(tmp_path / "missing.db")
    elif kind == "corrupt":
        database.path.write_bytes(b"invalid SQLite")
    else:
        with sqlite3.connect(database.path) as connection:
            if kind == "future":
                connection.execute("UPDATE schema_version SET version=999")
            else:
                connection.execute("UPDATE jobs SET project_id='missing'")
    destination = tmp_path / "backup.db"
    with pytest.raises(BackupServiceError):
        BackupService(database).create(destination)
    assert not destination.exists()
    assert not list(tmp_path.glob(".*.tmp*"))


def test_repository_and_missing_parent_are_rejected(tmp_path: Path) -> None:
    database = populated(tmp_path)
    repository = tmp_path / "repo"
    repository.mkdir()
    (repository / ".git").mkdir()
    for destination in [repository / "copy.db", tmp_path / "absent" / "copy.db"]:
        with pytest.raises(BackupServiceError):
            BackupService(database).create(destination)
        assert not destination.exists()


def test_publication_failure_cleans_temporary_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = populated(tmp_path)

    def fail(*args: object, **kwargs: object) -> None:
        raise OSError("Injected publication failure")

    monkeypatch.setattr(os, "link", fail)
    with pytest.raises(BackupServiceError):
        BackupService(database).create(tmp_path / "backup.db")
    assert not (tmp_path / "backup.db").exists()
    assert not list(tmp_path.glob(".*.tmp*"))


def test_backup_callback_timeout_aborts_lock_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = populated(tmp_path)
    service = BackupService(database, timeout=0.15)
    locker = sqlite3.connect(database.path, isolation_level=None)
    original = service._create_temporary

    def lock_after_schema_read(parent: Path, destination: Path) -> Path:
        temporary = original(parent, destination)
        locker.execute("BEGIN EXCLUSIVE")
        return temporary

    monkeypatch.setattr(service, "_create_temporary", lock_after_schema_read)
    started = time.monotonic()
    try:
        with pytest.raises(BackupServiceError, match="timed out"):
            service.create(tmp_path / "backup.db")
    finally:
        locker.close()
    assert time.monotonic() - started < 2
    assert not (tmp_path / "backup.db").exists()
    assert not list(tmp_path.glob(".*.tmp*"))
