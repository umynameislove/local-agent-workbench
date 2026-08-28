from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from db import (
    LATEST_SCHEMA_VERSION,
    MIGRATIONS,
    Database,
    JobRepository,
    ProjectRepository,
    UsageNotFoundError,
    UsageRepository,
    UsageRepositoryError,
    UsageValidationError,
)
from engine import (
    JobCreate,
    JobRuntime,
    JobState,
    PermissionMode,
    ProjectConfig,
    Sensitivity,
    UsageCreate,
)

NOW = datetime(2030, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)


def project() -> ProjectConfig:
    return ProjectConfig(
        id="alpha",
        root="/workspace/alpha",
        sensitivity=Sensitivity.PRIVATE,
        cloud_allowed=False,
        permission_mode=PermissionMode.SANDBOXED_WRITE,
    )


def job() -> JobCreate:
    return JobCreate(
        id="job-001",
        project_id="alpha",
        request="Measure provider usage truthfully.",
        request_snapshot={"policy_version": 1},
        state=JobState.COMPLETED,
        runtime=JobRuntime.CODEX,
        model="gpt-5.6-codex",
    )


def usage(**overrides: Any) -> UsageCreate:
    values: dict[str, Any] = {
        "provider": "openai",
        "job_id": "job-001",
        "model": "gpt-5.6-codex",
        "input_tokens": 1_250,
        "output_tokens": 375,
        "cost_usd": Decimal("0.00099"),
        "latency_ms": 2_401,
        "quota_limit": None,
        "quota_remaining": None,
        "quota_unit": None,
        "quota_reset_at": None,
        "rate_limited": None,
    }
    values.update(overrides)
    return UsageCreate(**values)


def initialized_repository(tmp_path: Path) -> tuple[Database, UsageRepository]:
    database = Database(tmp_path / "state.db")
    database.initialize()
    ProjectRepository(database).create(project())
    JobRepository(database).create(job())
    return database, UsageRepository(database)


def test_version_seven_database_adds_usage_without_losing_state(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    legacy = Database(path, migrations=MIGRATIONS[:7])
    assert legacy.initialize() == 7
    ProjectRepository(legacy).create(project())
    expected_job = JobRepository(legacy).create(job())

    upgraded = Database(path)
    assert upgraded.initialize() == LATEST_SCHEMA_VERSION
    assert JobRepository(upgraded).get(expected_job.id) == expected_job
    with sqlite3.connect(path) as connection:
        objects = connection.execute(
            """
            SELECT type, name
            FROM sqlite_master
            WHERE name LIKE 'usage%'
            ORDER BY type, name
            """
        ).fetchall()
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert objects == [
        ("index", "usage_job_recorded_idx"),
        ("index", "usage_provider_recorded_idx"),
        ("table", "usage"),
        ("trigger", "usage_prevent_delete"),
        ("trigger", "usage_prevent_update"),
    ]


def test_unknown_quota_stays_null_while_known_usage_survives_restart(tmp_path: Path) -> None:
    database, repository = initialized_repository(tmp_path)

    stored = repository.record(usage())

    assert stored.input_tokens == 1_250
    assert stored.output_tokens == 375
    assert stored.cost_usd == Decimal("0.00099")
    assert stored.latency_ms == 2_401
    assert stored.quota_limit is None
    assert stored.quota_remaining is None
    assert stored.quota_unit is None
    assert stored.quota_reset_at is None
    assert stored.rate_limited is None
    restarted = UsageRepository(Database(database.path))
    assert restarted.get(stored.id) == stored
    assert restarted.list(job_id="job-001", provider="openai") == (stored,)


def test_known_quota_and_reset_time_round_trip_exactly(tmp_path: Path) -> None:
    _, repository = initialized_repository(tmp_path)
    local_reset = datetime(
        2030,
        1,
        3,
        10,
        30,
        0,
        123456,
        tzinfo=timezone(timedelta(hours=7)),
    )

    stored = repository.record(
        usage(
            input_tokens=None,
            output_tokens=None,
            cost_usd=None,
            latency_ms=None,
            quota_limit=Decimal("1000000.000"),
            quota_remaining=Decimal("750000.50"),
            quota_unit="tokens",
            quota_reset_at=local_reset,
            rate_limited=False,
        )
    )

    assert stored.quota_limit == Decimal("1000000")
    assert stored.quota_remaining == Decimal("750000.5")
    assert stored.quota_unit == "tokens"
    assert stored.quota_reset_at == local_reset.astimezone(UTC)
    assert stored.rate_limited is False


def test_provider_level_observation_does_not_require_job_or_model(tmp_path: Path) -> None:
    _, repository = initialized_repository(tmp_path)

    stored = repository.record(
        usage(
            provider="openrouter",
            job_id=None,
            model=None,
            input_tokens=None,
            output_tokens=None,
            cost_usd=Decimal("0"),
            latency_ms=None,
            rate_limited=True,
        )
    )

    assert stored.job_id is None
    assert stored.model is None
    assert stored.cost_usd == Decimal("0")
    assert stored.rate_limited is True
    assert repository.list(provider="openrouter") == (stored,)


@pytest.mark.parametrize(
    "invalid",
    [
        usage(provider=" openai"),
        usage(provider=""),
        usage(job_id=" job-001"),
        usage(model=""),
        usage(input_tokens=-1),
        usage(input_tokens=True),
        usage(output_tokens=9_223_372_036_854_775_808),
        usage(latency_ms=-1),
        usage(cost_usd=0.1),
        usage(cost_usd=Decimal("NaN")),
        usage(cost_usd=Decimal("Infinity")),
        usage(cost_usd=Decimal("-0.01")),
        usage(cost_usd=Decimal("1e-101")),
        usage(quota_limit=Decimal("10"), quota_unit=None),
        usage(quota_unit="requests"),
        usage(
            quota_limit=Decimal("10"),
            quota_remaining=Decimal("11"),
            quota_unit="requests",
        ),
        usage(quota_reset_at=datetime(2030, 1, 2)),
        usage(quota_reset_at=datetime(1960, 1, 2, tzinfo=UTC)),
        usage(rate_limited=1),
        usage(
            input_tokens=None,
            output_tokens=None,
            cost_usd=None,
            latency_ms=None,
            rate_limited=None,
        ),
    ],
)
def test_invalid_usage_is_rejected_before_write(
    tmp_path: Path,
    invalid: UsageCreate,
) -> None:
    _, repository = initialized_repository(tmp_path)

    with pytest.raises(UsageValidationError):
        repository.record(invalid)

    assert repository.list() == ()


def test_missing_job_and_failed_insert_leave_no_partial_observation(tmp_path: Path) -> None:
    database, repository = initialized_repository(tmp_path)

    with pytest.raises(UsageValidationError, match="job does not exist"):
        repository.record(usage(job_id="missing"))
    with sqlite3.connect(database.path) as connection:
        connection.execute(
            """
            CREATE TRIGGER usage_reject_test_insert
            BEFORE INSERT ON usage
            BEGIN
                SELECT RAISE(ABORT, 'test failure');
            END
            """
        )
    with pytest.raises(UsageRepositoryError, match="could not be recorded"):
        repository.record(usage())

    assert repository.list() == ()


def test_concurrent_observations_are_distinct_and_ordered(tmp_path: Path) -> None:
    _, repository = initialized_repository(tmp_path)

    with ThreadPoolExecutor(max_workers=4) as executor:
        records = tuple(
            executor.map(
                lambda token_count: repository.record(
                    usage(input_tokens=token_count, output_tokens=0)
                ),
                range(1, 9),
            )
        )

    stored = repository.list()
    assert len({record.id for record in records}) == 8
    assert stored == tuple(sorted(stored, key=lambda record: record.id))
    assert {record.input_tokens for record in stored} == set(range(1, 9))


def test_usage_rows_are_immutable_at_the_database_boundary(tmp_path: Path) -> None:
    database, repository = initialized_repository(tmp_path)
    stored = repository.record(usage())

    with sqlite3.connect(database.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("UPDATE usage SET input_tokens = 1 WHERE id = ?", (stored.id,))
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("DELETE FROM usage WHERE id = ?", (stored.id,))

    assert repository.get(stored.id) == stored


@pytest.mark.parametrize(
    ("columns", "values"),
    [
        ("model", ("model-only",)),
        ("input_tokens", (-1,)),
        ("cost_usd", ("-0.01",)),
        ("rate_limited", (2,)),
        ("quota_limit", ("10",)),
        ("quota_limit, quota_remaining, quota_unit", ("10", "11", "requests")),
    ],
)
def test_database_constraints_reject_false_or_invalid_usage_truth(
    tmp_path: Path,
    columns: str,
    values: tuple[object, ...],
) -> None:
    database, repository = initialized_repository(tmp_path)
    placeholders = ", ".join("?" for _ in values)

    with sqlite3.connect(database.path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                f"INSERT INTO usage (provider, {columns}) VALUES (?, {placeholders})",
                ("openai", *values),
            )

    assert repository.list() == ()


def test_missing_invalid_and_corrupted_usage_fail_safely(tmp_path: Path) -> None:
    database, repository = initialized_repository(tmp_path)
    stored = repository.record(usage(cost_usd=Decimal("1")))

    with pytest.raises(UsageNotFoundError):
        repository.get(stored.id + 1)
    with pytest.raises(UsageValidationError):
        repository.get(True)
    with pytest.raises(UsageValidationError):
        repository.list(provider=" openai")
    with sqlite3.connect(database.path) as connection:
        connection.execute("DROP TRIGGER usage_prevent_update")
        connection.execute("UPDATE usage SET cost_usd = '1.0' WHERE id = ?", (stored.id,))
    with pytest.raises(UsageRepositoryError, match="Stored usage data is invalid"):
        repository.get(stored.id)


def test_usage_queries_treat_provider_and_model_as_data(tmp_path: Path) -> None:
    _, repository = initialized_repository(tmp_path)
    unusual_provider = "provider'); DROP TABLE usage; SELECT ('"
    unusual_model = "model'); DELETE FROM jobs; SELECT ('"

    stored = repository.record(usage(provider=unusual_provider, model=unusual_model, job_id=None))

    assert repository.list(provider=unusual_provider, model=unusual_model) == (stored,)
    assert repository.get(stored.id) == stored


def test_usage_storage_unavailable_uses_safe_public_error(tmp_path: Path) -> None:
    repository = UsageRepository(Database(tmp_path / "missing" / "state.db"))

    with pytest.raises(UsageRepositoryError) as failure:
        repository.list()

    assert str(tmp_path) not in str(failure.value)
    assert "unavailable" in str(failure.value)


def test_usage_create_is_immutable_input_data() -> None:
    original = usage()

    changed = replace(original, input_tokens=original.input_tokens + 1)

    assert original.input_tokens == 1_250
    assert changed.input_tokens == 1_251
