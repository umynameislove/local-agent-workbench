from __future__ import annotations

import asyncio
import json
import math
from collections.abc import AsyncIterator

from db import (
    EventNotFoundError,
    EventRecord,
    EventRepository,
    EventRepositoryError,
    JobRepository,
)


class EventStreamCursorError(ValueError):
    """Raised when an SSE cursor cannot resume the requested job stream."""


class EventStreamService:
    """Replay the durable event ledger and follow newly persisted events."""

    def __init__(
        self,
        jobs: JobRepository,
        events: EventRepository,
        *,
        poll_interval: float = 0.1,
        heartbeat_interval: float = 15.0,
        batch_size: int = 128,
    ) -> None:
        if not isinstance(jobs, JobRepository) or not isinstance(events, EventRepository):
            raise TypeError("Event streaming requires job and event repositories.")
        if (
            type(poll_interval) not in (int, float)
            or not math.isfinite(poll_interval)
            or poll_interval <= 0
        ):
            raise ValueError("Event stream polling interval must be finite and positive.")
        if (
            type(heartbeat_interval) not in (int, float)
            or not math.isfinite(heartbeat_interval)
            or heartbeat_interval <= 0
        ):
            raise ValueError("Event stream heartbeat interval must be finite and positive.")
        if type(batch_size) is not int or not 1 <= batch_size <= 1_000:
            raise ValueError("Event stream batch size must be between 1 and 1000.")
        self._jobs = jobs
        self._events = events
        self._poll_interval = float(poll_interval)
        self._heartbeat_interval = float(heartbeat_interval)
        self._batch_size = batch_size

    def subscribe(
        self,
        job_id: str,
        last_event_id: str | None = None,
    ) -> AsyncIterator[bytes]:
        """Validate a subscription before response headers are committed."""
        self._jobs.get(job_id)
        sequence = 0
        if last_event_id not in (None, ""):
            event_id = self._parse_event_id(last_event_id)
            try:
                cursor = self._events.get(event_id)
            except EventNotFoundError as error:
                raise EventStreamCursorError(
                    "Last-Event-ID does not identify an event for this job."
                ) from error
            if cursor.job_id != job_id:
                raise EventStreamCursorError(
                    "Last-Event-ID does not identify an event for this job."
                )
            sequence = cursor.sequence
        return self._follow(job_id, sequence)

    @staticmethod
    def _parse_event_id(value: str) -> int:
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 19
            or not value.isascii()
            or not value.isdigit()
        ):
            raise EventStreamCursorError("Last-Event-ID must be a positive event identifier.")
        event_id = int(value)
        if not 1 <= event_id <= 9_223_372_036_854_775_807:
            raise EventStreamCursorError("Last-Event-ID must be a positive event identifier.")
        return event_id

    async def _follow(self, job_id: str, sequence: int) -> AsyncIterator[bytes]:
        loop = asyncio.get_running_loop()
        next_heartbeat = loop.time() + self._heartbeat_interval
        while True:
            try:
                records = await asyncio.to_thread(
                    self._events.list_after,
                    job_id,
                    sequence,
                    limit=self._batch_size,
                )
            except EventRepositoryError:
                yield self._unavailable_frame()
                return
            if records:
                for record in records:
                    yield self._event_frame(record)
                    sequence = record.sequence
                next_heartbeat = loop.time() + self._heartbeat_interval
                continue
            delay = min(self._poll_interval, max(0.0, next_heartbeat - loop.time()))
            if delay:
                await asyncio.sleep(delay)
            if loop.time() >= next_heartbeat:
                yield b": keep-alive\n\n"
                next_heartbeat = loop.time() + self._heartbeat_interval

    @staticmethod
    def _event_frame(record: EventRecord) -> bytes:
        data = json.dumps(
            {
                "created_at": record.created_at,
                "event_type": record.event_type,
                "id": record.id,
                "job_id": record.job_id,
                "payload": record.payload,
                "sequence": record.sequence,
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return f"id: {record.id}\nevent: workbench.event\ndata: {data}\n\n".encode()

    @staticmethod
    def _unavailable_frame() -> bytes:
        return b'event: workbench.error\ndata: {"code":"event_stream_unavailable"}\n\n'
