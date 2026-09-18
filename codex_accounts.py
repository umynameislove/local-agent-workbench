from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from engine import AdapterError

_SLOT_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")


@dataclass(frozen=True)
class CodexAccountSlot:
    """Identify one isolated Codex CLI state directory without exposing credentials."""

    name: str
    home: Path

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or _SLOT_NAME.fullmatch(self.name) is None:
            raise ValueError("Codex account slot name is invalid.")
        if (
            not isinstance(self.home, Path)
            or not self.home.is_absolute()
            or "\x00" in str(self.home)
        ):
            raise ValueError("Codex account home must be an absolute Path.")


def codex_account_slots(runtime_root: Path, names: tuple[str, ...]) -> tuple[CodexAccountSlot, ...]:
    """Place logical account slots below private runtime storage by convention."""

    if not isinstance(runtime_root, Path) or not runtime_root.is_absolute():
        raise ValueError("Runtime root must be an absolute Path.")
    if not isinstance(names, tuple) or not names:
        raise ValueError("Codex account names must be a nonempty tuple.")
    return tuple(
        CodexAccountSlot(name, runtime_root / "providers" / "codex" / name) for name in names
    )


class CodexAccountPool:
    """Select authenticated account slots in deterministic round robin order."""

    def __init__(self, slots: tuple[CodexAccountSlot, ...]) -> None:
        if (
            not isinstance(slots, tuple)
            or not slots
            or any(not isinstance(slot, CodexAccountSlot) for slot in slots)
        ):
            raise ValueError("Codex account pool requires account slots.")
        if len({slot.name for slot in slots}) != len(slots):
            raise ValueError("Codex account slot names must be unique.")
        if len({slot.home for slot in slots}) != len(slots):
            raise ValueError("Codex account homes must be unique.")
        self._slots = slots
        self._next_index = 0
        self._lock = asyncio.Lock()

    @property
    def slots(self) -> tuple[CodexAccountSlot, ...]:
        return self._slots

    async def acquire(
        self,
        available: Callable[[CodexAccountSlot], Awaitable[bool]],
    ) -> CodexAccountSlot:
        if not callable(available):
            raise TypeError("Codex account availability probe must be callable.")
        async with self._lock:
            for offset in range(len(self._slots)):
                index = (self._next_index + offset) % len(self._slots)
                slot = self._slots[index]
                if await available(slot):
                    self._next_index = (index + 1) % len(self._slots)
                    return slot
        raise AdapterError("No authenticated Codex account is available.")
