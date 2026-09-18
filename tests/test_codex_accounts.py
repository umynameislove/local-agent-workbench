from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from codex_accounts import CodexAccountPool, CodexAccountSlot, codex_account_slots
from engine import AdapterError


def test_runtime_slots_use_private_provider_layout(tmp_path: Path) -> None:
    slots = codex_account_slots(tmp_path, ("primary", "secondary", "tertiary"))

    assert [slot.name for slot in slots] == ["primary", "secondary", "tertiary"]
    assert [slot.home for slot in slots] == [
        tmp_path / "providers" / "codex" / name for name in ("primary", "secondary", "tertiary")
    ]
    with pytest.raises(FrozenInstanceError):
        slots[0].name = "changed"


@pytest.mark.parametrize(
    "name",
    ["", "UPPER", "has space", "../escape", ".hidden", "x" * 65, None],
)
def test_slot_names_reject_ambiguous_or_unsafe_values(tmp_path: Path, name) -> None:
    with pytest.raises(ValueError, match="slot name"):
        CodexAccountSlot(name, tmp_path)


@pytest.mark.parametrize("home", [Path("relative"), "not-a-path", None])
def test_slot_home_requires_an_absolute_path(home) -> None:
    with pytest.raises(ValueError, match="absolute Path"):
        CodexAccountSlot("primary", home)


def test_slot_factory_requires_absolute_runtime_and_nonempty_tuple(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Runtime root"):
        codex_account_slots(Path("runtime"), ("primary",))
    with pytest.raises(ValueError, match="nonempty tuple"):
        codex_account_slots(tmp_path, ())
    with pytest.raises(ValueError, match="nonempty tuple"):
        codex_account_slots(tmp_path, ["primary"])


@pytest.mark.parametrize("duplicate", ["name", "home"])
def test_pool_rejects_duplicate_identity(tmp_path: Path, duplicate: str) -> None:
    first = CodexAccountSlot("first", tmp_path / "first")
    second = (
        CodexAccountSlot("first", tmp_path / "second")
        if duplicate == "name"
        else CodexAccountSlot("second", tmp_path / "first")
    )

    with pytest.raises(ValueError, match="unique"):
        CodexAccountPool((first, second))


@pytest.mark.parametrize("slots", [(), [], (None,)])
def test_pool_requires_typed_nonempty_tuple(slots) -> None:
    with pytest.raises(ValueError, match="requires account slots"):
        CodexAccountPool(slots)


@pytest.mark.anyio
async def test_pool_rotates_deterministically_across_available_accounts(tmp_path: Path) -> None:
    slots = codex_account_slots(tmp_path, ("one", "two", "three"))
    pool = CodexAccountPool(slots)

    async def available(_slot: CodexAccountSlot) -> bool:
        return True

    selected = [await pool.acquire(available) for _ in range(7)]

    assert [slot.name for slot in selected] == ["one", "two", "three", "one", "two", "three", "one"]


@pytest.mark.anyio
async def test_pool_skips_unavailable_slots_without_losing_rotation(tmp_path: Path) -> None:
    slots = codex_account_slots(tmp_path, ("one", "two", "three"))
    pool = CodexAccountPool(slots)

    async def available(slot: CodexAccountSlot) -> bool:
        return slot.name != "two"

    selected = [await pool.acquire(available) for _ in range(4)]

    assert [slot.name for slot in selected] == ["one", "three", "one", "three"]


@pytest.mark.anyio
async def test_concurrent_acquisition_preserves_round_robin_order(tmp_path: Path) -> None:
    pool = CodexAccountPool(codex_account_slots(tmp_path, ("one", "two", "three")))

    async def available(_slot: CodexAccountSlot) -> bool:
        await asyncio.sleep(0)
        return True

    selected = await asyncio.gather(*(pool.acquire(available) for _ in range(6)))

    assert [slot.name for slot in selected] == ["one", "two", "three", "one", "two", "three"]


@pytest.mark.anyio
async def test_pool_failure_is_sanitized_when_every_slot_is_unavailable(tmp_path: Path) -> None:
    pool = CodexAccountPool(codex_account_slots(tmp_path, ("one", "two")))

    async def unavailable(_slot: CodexAccountSlot) -> bool:
        return False

    with pytest.raises(AdapterError, match="No authenticated Codex account") as error:
        await pool.acquire(unavailable)

    assert str(tmp_path) not in str(error.value)


@pytest.mark.anyio
async def test_pool_rejects_noncallable_probe(tmp_path: Path) -> None:
    pool = CodexAccountPool(codex_account_slots(tmp_path, ("one",)))

    with pytest.raises(TypeError, match="probe"):
        await pool.acquire(None)
