from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest

from engine import ProviderHealth, ProviderHealthState

OBSERVED_AT = datetime(2026, 9, 13, 8, tzinfo=UTC)


@pytest.mark.parametrize("state", list(ProviderHealthState))
def test_every_health_state_roundtrips_without_invented_quota(state) -> None:
    health = ProviderHealth(state, OBSERVED_AT)

    assert ProviderHealth.from_dict(health.to_dict()) == health
    assert health.to_dict() == {
        "state": state.value,
        "observed_at": "2026-09-13T08:00:00Z",
        "reset_at": None,
    }


@pytest.mark.parametrize("state", list(ProviderHealthState))
def test_health_preserves_a_known_future_reset_time(state) -> None:
    health = ProviderHealth(
        state,
        OBSERVED_AT,
        OBSERVED_AT + timedelta(minutes=15),
    )

    assert health.to_dict() == {
        "state": state.value,
        "observed_at": "2026-09-13T08:00:00Z",
        "reset_at": "2026-09-13T08:15:00Z",
    }
    assert ProviderHealth.from_dict(health.to_dict()) == health


def test_health_times_are_normalized_to_utc() -> None:
    local_zone = timezone(timedelta(hours=7))
    health = ProviderHealth(
        ProviderHealthState.RATE_LIMITED,
        datetime(2026, 9, 13, 15, tzinfo=local_zone),
        datetime(2026, 9, 13, 15, 30, tzinfo=local_zone),
    )

    assert health.observed_at == OBSERVED_AT
    assert health.reset_at == OBSERVED_AT + timedelta(minutes=30)
    assert health.observed_at.tzinfo is UTC
    assert health.reset_at is not None and health.reset_at.tzinfo is UTC


@pytest.mark.parametrize("state", ["available", None, True])
def test_health_requires_a_typed_state(state) -> None:
    with pytest.raises(ValueError, match="Provider health state"):
        ProviderHealth(state, OBSERVED_AT)


@pytest.mark.parametrize("observed_at", [None, "2026-09-13T08:00:00Z", datetime(2026, 9, 13)])
def test_health_requires_a_timezone_aware_observation(observed_at) -> None:
    with pytest.raises(ValueError, match="observation time"):
        ProviderHealth(ProviderHealthState.AVAILABLE, observed_at)


@pytest.mark.parametrize("reset_at", ["2026-09-13T08:15:00Z", datetime(2026, 9, 13, 8, 15)])
def test_reset_requires_a_timezone_aware_datetime(reset_at) -> None:
    with pytest.raises(ValueError, match="reset time"):
        ProviderHealth(ProviderHealthState.RATE_LIMITED, OBSERVED_AT, reset_at)


@pytest.mark.parametrize("offset", [timedelta(), timedelta(seconds=-1)])
def test_reset_must_follow_the_observation(offset) -> None:
    with pytest.raises(ValueError, match="must follow"):
        ProviderHealth(
            ProviderHealthState.RATE_LIMITED,
            OBSERVED_AT,
            OBSERVED_AT + offset,
        )


@pytest.mark.parametrize("field", ["state", "observed_at", "reset_at"])
def test_serialized_health_requires_every_field(field) -> None:
    value = ProviderHealth(ProviderHealthState.AVAILABLE, OBSERVED_AT).to_dict()
    del value[field]

    with pytest.raises(ValueError, match="fields are invalid"):
        ProviderHealth.from_dict(value)


def test_serialized_health_rejects_extra_fields() -> None:
    value = ProviderHealth(ProviderHealthState.AVAILABLE, OBSERVED_AT).to_dict()
    value["quota_remaining"] = "99"

    with pytest.raises(ValueError, match="fields are invalid"):
        ProviderHealth.from_dict(value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("state", "unknown"),
        ("observed_at", None),
        ("observed_at", "2026-09-13T08:00:00"),
        ("reset_at", 123),
        ("reset_at", "2026-09-13T07:59:59Z"),
    ],
)
def test_invalid_serialized_health_is_rejected(field, value) -> None:
    item = ProviderHealth(
        ProviderHealthState.RATE_LIMITED,
        OBSERVED_AT,
        OBSERVED_AT + timedelta(minutes=1),
    ).to_dict()
    item[field] = value

    with pytest.raises(ValueError, match="Provider health is invalid"):
        ProviderHealth.from_dict(item)


def test_health_observation_is_immutable() -> None:
    health = ProviderHealth(ProviderHealthState.DEGRADED, OBSERVED_AT)

    with pytest.raises(FrozenInstanceError):
        health.state = ProviderHealthState.AVAILABLE
