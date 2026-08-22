from __future__ import annotations

import io
import json
import logging
from collections.abc import Iterator
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

from app import create_app
from logging_setup import LOGGER_NAME, REDACTED, configure_logging, redact

SYNTHETIC_SECRET = "synthetic" + "_secret_value_123456"


@pytest.fixture
def isolated_workbench_logger() -> Iterator[logging.Logger]:
    logger = logging.getLogger(LOGGER_NAME)
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    logger.handlers.clear()
    try:
        yield logger
    finally:
        logger.handlers.clear()
        logger.handlers.extend(original_handlers)
        logger.setLevel(original_level)
        logger.propagate = original_propagate


def parse_lines(stream: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_recursive_redaction_masks_sensitive_fields_without_mutation() -> None:
    original = {
        "Token": SYNTHETIC_SECRET,
        "nested": {
            "apiKey": SYNTHETIC_SECRET,
            "items": [{"PASSWORD": SYNTHETIC_SECRET}, "safe"],
        },
        "tuple": ({"authorization": SYNTHETIC_SECRET},),
        "safe": "visible",
    }

    result = redact(original)

    assert result == {
        "Token": REDACTED,
        "nested": {
            "apiKey": REDACTED,
            "items": [{"PASSWORD": REDACTED}, "safe"],
        },
        "tuple": [{"authorization": REDACTED}],
        "safe": "visible",
    }
    assert original["Token"] == SYNTHETIC_SECRET
    assert original["nested"]["apiKey"] == SYNTHETIC_SECRET  # type: ignore[index]


def test_custom_sensitive_field_and_protected_payload_fields_are_masked() -> None:
    value = {
        "tenant_reference": SYNTHETIC_SECRET,
        "prompt": "private user text",
        "task_payload": {"safe": "value"},
        "environment": {"HOME": "private location"},
    }

    result = redact(value, sensitive_fields={"tenant_reference"})

    assert result == {
        "tenant_reference": REDACTED,
        "prompt": REDACTED,
        "task_payload": REDACTED,
        "environment": REDACTED,
    }


def test_all_default_sensitive_fields_are_masked() -> None:
    fields = [
        "token",
        "api_key",
        "password",
        "authorization",
        "cookie",
        "credential",
        "secret",
        "private_key",
        "access_token",
        "refresh_token",
    ]

    result = redact({field: SYNTHETIC_SECRET for field in fields})

    assert result == {field: REDACTED for field in fields}


def test_embedded_credentials_and_personal_paths_are_masked() -> None:
    authorization = "Authorization" + f": Bearer {SYNTHETIC_SECRET}"
    basic_authorization = "Authorization" + f": Basic {SYNTHETIC_SECRET}"
    cookie = "Cookie" + f": session={SYNTHETIC_SECRET}; theme=private"
    assignment = "token=" + SYNTHETIC_SECRET
    prompt = "prompt=" + SYNTHETIC_SECRET
    personal_path = str(Path("/") / "Users" / "synthetic_user" / "private" / "file.txt")

    result = redact([authorization, basic_authorization, cookie, assignment, prompt, personal_path])
    serialized = json.dumps(result)

    assert SYNTHETIC_SECRET not in serialized
    assert "synthetic_user" not in serialized
    assert "private/file.txt" not in serialized
    assert "theme=private" not in serialized
    assert serialized.count(REDACTED) == 5
    assert "[REDACTED_PATH]" in serialized


def test_circular_context_is_safe() -> None:
    context: dict[str, object] = {}
    context["self"] = context

    assert redact(context) == {"self": "[CIRCULAR]"}


def test_json_contract_and_exception_are_redacted(
    isolated_workbench_logger: logging.Logger,
) -> None:
    stream = io.StringIO()
    logger = configure_logging(handler=logging.StreamHandler(stream))

    try:
        raise ValueError("credential=" + SYNTHETIC_SECRET)
    except ValueError:
        logger.exception(
            "Provider request failed with token=" + SYNTHETIC_SECRET,
            extra={
                "event": "provider.request.failed",
                "context": {"cookie": SYNTHETIC_SECRET, "provider": "synthetic"},
            },
        )

    records = parse_lines(stream)
    assert len(records) == 1
    record = records[0]
    assert set(record) == {
        "context",
        "event",
        "exception",
        "level",
        "logger",
        "message",
        "timestamp",
    }
    assert record["event"] == "provider.request.failed"
    assert record["level"] == "ERROR"
    assert record["logger"] == LOGGER_NAME
    assert record["context"] == {"cookie": REDACTED, "provider": "synthetic"}
    assert str(record["timestamp"]).endswith("Z")
    assert SYNTHETIC_SECRET not in stream.getvalue()
    assert "/Users/" not in stream.getvalue()


def test_configuration_is_idempotent_and_supports_handler_injection(
    isolated_workbench_logger: logging.Logger,
) -> None:
    first_stream = io.StringIO()
    first_handler = logging.StreamHandler(first_stream)

    first = configure_logging(
        handler=first_handler,
        sensitive_fields={"tenant_reference"},
    )
    second = configure_logging()
    second.info(
        "One record.",
        extra={
            "event": "test.record",
            "context": {"tenant_reference": SYNTHETIC_SECRET},
        },
    )

    assert first is second
    assert second.handlers == [first_handler]
    assert parse_lines(first_stream)[0]["context"] == {"tenant_reference": REDACTED}

    replacement_stream = io.StringIO()
    replacement_handler = logging.StreamHandler(replacement_stream)
    configure_logging(handler=replacement_handler)

    assert second.handlers == [replacement_handler]


def test_rotation_capable_handler_can_be_injected(
    tmp_path: Path, isolated_workbench_logger: logging.Logger
) -> None:
    log_path = tmp_path / "runtime.log"
    handler = RotatingFileHandler(log_path, maxBytes=1024, backupCount=1, delay=True)
    logger = configure_logging(handler=handler)

    logger.info("Rotation ready record.", extra={"event": "test.rotation.ready"})
    handler.close()

    record = json.loads(log_path.read_text(encoding="utf-8"))
    assert record["event"] == "test.rotation.ready"
    assert logger.handlers == [handler]


def test_default_handler_writes_json_to_stderr(
    isolated_workbench_logger: logging.Logger, capsys: pytest.CaptureFixture[str]
) -> None:
    logger = configure_logging()

    logger.info("Default output.", extra={"event": "test.stderr"})

    captured = capsys.readouterr()
    assert json.loads(captured.err)["event"] == "test.stderr"
    assert captured.out == ""


@pytest.mark.anyio
async def test_application_lifecycle_emits_safe_bootstrap_event(
    tmp_path: Path, isolated_workbench_logger: logging.Logger
) -> None:
    stream = io.StringIO()
    configure_logging(handler=logging.StreamHandler(stream))
    runtime_home = tmp_path / "private_runtime"
    api = create_app({"AGENT_WORKBENCH_HOME": str(runtime_home)})

    async with api.router.lifespan_context(api):
        pass

    records = parse_lines(stream)
    assert records == [
        {
            "context": {"runtime_home_configured": True},
            "event": "runtime.bootstrap.completed",
            "level": "INFO",
            "logger": LOGGER_NAME,
            "message": "Runtime storage is ready.",
            "timestamp": records[0]["timestamp"],
        }
    ]
    assert str(runtime_home) not in stream.getvalue()
