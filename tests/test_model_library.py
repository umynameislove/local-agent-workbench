from __future__ import annotations

import copy
import fcntl
import json
import os
from concurrent.futures import ThreadPoolExecutor

import pytest
from test_workspace import workspace
from test_workspace_control import claim, control_app

from local_agent_workbench.engine import ConfigurationError, WorkbenchConfig
from local_agent_workbench.model_library import LibraryConflictError, ModelLibrary

URL = "/api/settings/library"


def addition(current, identity="research", **changes):
    return {
        "revision": current["revision"],
        "kind": "connection",
        "entry": {"id": identity, "kind": "openrouter"},
        **changes,
    }


@pytest.mark.anyio
async def test_complete_metadata_excludes_secrets_and_unassigned_entries_survive_restart(tmp_path):
    async with control_app(tmp_path) as (api, client):
        path, active = api.state.runtime.config, api.state.team_repository.system
        before = json.loads(path.read_text())
        attempt = claim(api)
        current = (await client.get(URL)).json()
        assert {c["id"] for c in current["connections"]} == {"native", "api"}
        for forbidden in ("private-reference", "credential_ref", "account_slot", str(tmp_path)):
            assert forbidden not in json.dumps(current)
        saved = await client.post(URL, json=addition(current))
        assert saved.status_code == 200
        current = saved.json()
        added = next(c for c in current["connections"] if c["id"] == "research")
        assert not added["enabled"] and not added["credential_configured"]
        assert added["status"] == "restart_required"
        model = {"id": "read", "connection_id": "research", "model": "vendor/reader"}
        result = await client.post(
            URL, json={"revision": current["revision"], "kind": "model", "entry": model}
        )
        assert result.status_code == 200
        assert result.json()["models"][-1]["status"] == "restart_required"
        after = json.loads(path.read_text())
        for key in before.keys() - {"agent_system"}:
            assert after[key] == before[key]
        original = WorkbenchConfig.from_dict(before).agent_system.to_dict()
        for key in original.keys() - {"connections", "models"}:
            assert after["agent_system"][key] == original[key]
        assert after["agent_system"]["connections"][:-1] == original["connections"]
        assert after["agent_system"]["models"][:-1] == original["models"]
        assert path.stat().st_mode & 0o777 == 0o600
        assert api.state.team_repository.system is active
        assert api.state.native_attempt_repository.get(attempt.job_id) == attempt
        assert not result.json()["models"][-1]["verified"]
        assert WorkbenchConfig.load(path).agent_system.models[-1].capabilities == ()
    async with api.router.lifespan_context(api):
        library = ModelLibrary(path, api.state.team_repository.system).read()
        assert all(c["status"] == "loaded" for c in library["connections"])
        assert not library["connections"][-1]["enabled"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "field", ["enabled", "verified", "capabilities", "credential_ref", "account_slot", "endpoint"]
)
async def test_setup_rejects_authority_and_secret_fields_without_a_write(tmp_path, field):
    async with control_app(tmp_path) as (api, client):
        before = api.state.runtime.config.read_bytes()
        value = (await client.get(URL)).json()
        payload = addition(value)
        payload["entry"][field] = "sensitive-fixture"
        response = await client.post(URL, json=payload)
        assert response.status_code == 422
        assert "sensitive-fixture" not in response.text
        assert api.state.runtime.config.read_bytes() == before


@pytest.mark.anyio
async def test_conflicts_foreign_origin_and_invalid_model_do_not_mutate(tmp_path):
    async with control_app(tmp_path) as (api, client):
        current = (await client.get(URL)).json()
        initial = api.state.runtime.config.read_bytes()
        for payload, status in (
            (addition(current, "native"), 409),
            (addition(current, revision="outdated"), 409),
            (
                addition(
                    current,
                    kind="model",
                    entry={"id": "new", "connection_id": "missing", "model": "a/b"},
                ),
                422,
            ),
            (addition(current, entry={"id": "bad", "kind": "unknown"}), 422),
        ):
            response = await client.post(URL, json=payload)
            assert response.status_code == status
            assert api.state.runtime.config.read_bytes() == initial
        assert (
            await client.post(
                URL, json=addition(current), headers={"Origin": "https://foreign.invalid"}
            )
        ).status_code == 403
        saved = await client.post(URL, json=addition(current))
        assert saved.status_code == 200
        assert (await client.post(URL, json=addition(current, "second"))).status_code == 409


@pytest.mark.anyio
@pytest.mark.parametrize("configured", [False, True])
async def test_legacy_and_empty_configuration_are_readable_without_migration(tmp_path, configured):
    async with workspace(tmp_path, configured=configured) as (api, client):
        before = api.state.runtime.config.read_bytes() if configured else None
        current = (await client.get(URL)).json()
        assert current["connections"] == current["models"] == [] and not current["editable"]
        assert (await client.post(URL, json=addition(current))).status_code == 409
        assert api.state.runtime.config.exists() == configured
        if configured:
            assert api.state.runtime.config.read_bytes() == before


@pytest.mark.anyio
async def test_replace_failure_leaves_original_and_cleans_temporary_file(tmp_path, monkeypatch):
    async with control_app(tmp_path) as (api, client):
        current = (await client.get(URL)).json()
        before = api.state.runtime.config.read_bytes()
        files = set(api.state.runtime.root.iterdir())
        monkeypatch.setattr(
            os, "replace", lambda *args: (_ for _ in ()).throw(OSError("private detail"))
        )
        result = await client.post(URL, json=addition(current))
        assert result.status_code == 503 and "private detail" not in result.text
        assert api.state.runtime.config.read_bytes() == before
        assert set(api.state.runtime.root.iterdir()) - files <= {
            api.state.runtime.root / ".model-library.lock"
        }


@pytest.mark.anyio
async def test_concurrent_writers_share_one_revision_and_symlink_is_rejected(tmp_path):
    async with control_app(tmp_path) as (api, _):
        store = ModelLibrary(api.state.runtime.config, api.state.team_repository.system)
        current = store.read()

        def write(identity):
            try:
                return store.add(addition(current, identity))
            except LibraryConflictError:
                return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(write, ["first", "second"]))
        assert sum(result is not None for result in results) == 1
        original = copy.deepcopy(store.read())
        target = tmp_path / "target.json"
        api.state.runtime.config.rename(target)
        api.state.runtime.config.symlink_to(target)
        with pytest.raises(ConfigurationError):
            store.add(addition(original))
        assert len(json.loads(target.read_text())["agent_system"]["connections"]) == 3


@pytest.mark.anyio
async def test_held_setup_lock_returns_conflict_and_keeps_backend_available(tmp_path):
    async with control_app(tmp_path) as (api, client):
        current = (await client.get(URL)).json()
        before = api.state.runtime.config.read_bytes()
        with (api.state.runtime.root / ".model-library.lock").open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = await client.post(URL, json=addition(current))
            assert result.status_code == 409
            assert (await client.get("/api/health")).status_code == 200
        assert api.state.runtime.config.read_bytes() == before


@pytest.mark.anyio
@pytest.mark.parametrize("field", ["verified", "capabilities", "enabled", "credential_ref"])
async def test_model_setup_cannot_activate_an_existing_native_connection(tmp_path, field):
    async with control_app(tmp_path) as (api, client):
        active = api.state.team_repository.system
        current = (await client.get(URL)).json()
        entry = {"id": "new-native", "connection_id": "native", "model": "example/model"}
        result = await client.post(
            URL,
            json={
                "revision": current["revision"],
                "kind": "model",
                "entry": {**entry, field: True},
            },
        )
        assert result.status_code == 422
        assert (await client.get(URL)).json()["revision"] == current["revision"]
        result = await client.post(
            URL, json={"revision": current["revision"], "kind": "model", "entry": entry}
        )
        assert result.status_code == 200
        model = result.json()["models"][-1]
        assert not model["verified"] and model["capabilities"] == []
        assert api.state.team_repository.system is active
