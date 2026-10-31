"""Local, additive model setup without activating providers or changing jobs."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
import tempfile
from dataclasses import asdict
from pathlib import Path

from .engine import ConfigurationError, WorkbenchConfig
from .team_config import AgentSystem, Connection, Model, TeamConfigError


class LibraryConflictError(ValueError):
    """Configuration changed or requires explicit setup before editing."""


class ModelLibrary:
    def __init__(self, path: Path, active: AgentSystem) -> None:
        self.path, self.active = path, active

    def _load(self) -> tuple[bytes, dict]:
        try:
            descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(descriptor, "rb") as handle:
                info = os.fstat(handle.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ConfigurationError("Use a regular private configuration file.")
                raw = handle.read(2_097_153)
            if len(raw) > 2_097_152:
                raise ConfigurationError("Configuration exceeds the setup size limit.")
            value = json.loads(raw)
            WorkbenchConfig.from_dict(value)
            return raw, value
        except FileNotFoundError:
            return b"", {}
        except (OSError, ValueError, TypeError, AttributeError) as error:
            raise ConfigurationError("Configuration cannot be read. Check local setup.") from error

    @staticmethod
    def _revision(raw: bytes) -> str:
        return hashlib.sha256(raw).hexdigest()

    def read(self) -> dict:
        raw, value = self._load()
        system = AgentSystem.from_dict(value.get("agent_system", {}))
        active_connections = {entry.id: entry for entry in self.active.connections}
        active_models = {entry.id: entry for entry in self.active.models}
        return {
            "revision": self._revision(raw),
            "editable": value.get("version") == 2,
            "connections": [
                {
                    "id": entry.id,
                    "kind": entry.kind,
                    "enabled": entry.enabled,
                    "credential_configured": bool(entry.credential_ref or entry.account_slot),
                    "status": "loaded"
                    if active_connections.get(entry.id) == entry
                    else "restart_required",
                }
                for entry in system.connections
            ],
            "models": [
                {
                    "id": entry.id,
                    "connection_id": entry.connection_id,
                    "model": entry.model,
                    "verified": entry.verified,
                    "capabilities": list(entry.capabilities),
                    "status": "loaded"
                    if active_models.get(entry.id) == entry
                    else "restart_required",
                }
                for entry in system.models
            ],
        }

    def add(self, payload: object) -> dict:
        if not isinstance(payload, dict) or set(payload) != {"revision", "kind", "entry"}:
            raise TeamConfigError("Setup requires revision, kind and entry.")
        kind = payload["kind"]
        fields = {"connection": {"id", "kind"}, "model": {"id", "connection_id", "model"}}
        entry = payload["entry"]
        if (
            not isinstance(kind, str)
            or kind not in fields
            or not isinstance(entry, dict)
            or set(entry) != fields[kind]
        ):
            raise TeamConfigError("Setup accepts only source and model identities, never secrets.")
        parsed = Connection.from_dict(entry) if kind == "connection" else Model.from_dict(entry)
        group = "connections" if kind == "connection" else "models"
        try:
            lock = os.open(
                self.path.parent / ".model-library.lock",
                os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW,
                0o600,
            )
            with os.fdopen(lock, "a") as guard:
                info = os.fstat(guard.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ConfigurationError("Setup lock must be a regular private file.")
                try:
                    fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as error:
                    raise LibraryConflictError(
                        "Another setup save is in progress. Reload to retry."
                    ) from error
                raw, value = self._load()
                if payload["revision"] != self._revision(raw):
                    raise LibraryConflictError(
                        "Configuration changed. Reload before adding an entry."
                    )
                if value.get("version") != 2:
                    raise LibraryConflictError(
                        "Set up version two configuration before adding entries."
                    )
                system = AgentSystem.from_dict(value.get("agent_system", {})).to_dict()
                if any(item["id"] == parsed.id for item in system[group]):
                    raise LibraryConflictError(
                        "This identity already exists. Existing entries are preserved."
                    )
                system[group].append(json.loads(json.dumps(asdict(parsed))))
                AgentSystem.from_dict(system)
                value["agent_system"] = system
                WorkbenchConfig.from_dict(value)
                content = (
                    json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
                ).encode()
                temporary = None
                try:
                    with tempfile.NamedTemporaryFile(dir=self.path.parent, delete=False) as handle:
                        temporary = Path(handle.name)
                        handle.write(content)
                        handle.flush()
                        os.fsync(handle.fileno())
                    if self._load()[0] != raw:
                        raise LibraryConflictError(
                            "Configuration changed. Reload before adding an entry."
                        )
                    os.replace(temporary, self.path)
                    parent = os.open(self.path.parent, os.O_RDONLY)
                    try:
                        os.fsync(parent)
                    finally:
                        os.close(parent)
                finally:
                    if temporary is not None:
                        temporary.unlink(missing_ok=True)
                return self.read()
        except OSError as error:
            raise ConfigurationError(
                "Setup could not be saved. Reload to check its outcome."
            ) from error
