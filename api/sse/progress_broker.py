"""
api/sse/progress_broker.py — Per-run SSE progress broker.

ProgressBroker manages a dict of run_id → asyncio.Queue.
InstrumentedEventStore wraps any EventStore and pushes relevant
events (AgentNodeExecuted, AgentSessionCompleted, AgentSessionFailed)
to the broker without modifying any ledger/ code.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncGenerator


_SENTINEL = object()


class ProgressBroker:
    """Manages SSE queues for in-progress pipeline runs."""

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue] = {}

    def create_run(self, run_id: str) -> None:
        self._queues[run_id] = asyncio.Queue()

    def push(self, run_id: str, event_name: str, data: dict[str, Any]) -> None:
        q = self._queues.get(run_id)
        if q is not None:
            q.put_nowait((event_name, data))

    def close(self, run_id: str) -> None:
        q = self._queues.get(run_id)
        if q is not None:
            q.put_nowait(_SENTINEL)

    async def stream(self, run_id: str) -> AsyncGenerator[str, None]:
        q = self._queues.get(run_id)
        if q is None:
            return
        while True:
            item = await q.get()
            if item is _SENTINEL:
                break
            event_name, data = item
            yield f"event: {event_name}\ndata: {json.dumps(data)}\n\n"
        # Clean up
        self._queues.pop(run_id, None)


# Map event_type → SSE event name.
# AgentSessionFailed uses "run_error" (not "error") to avoid colliding
# with the browser EventSource's built-in connection-error event, which
# also fires as "error" and cannot be distinguished by type alone.
_EVENT_MAP = {
    "AgentNodeExecuted": "node",
    "AgentSessionCompleted": "done",
    "AgentSessionFailed": "run_error",
    "AgentSessionStarted": "started",
}


class InstrumentedEventStore:
    """
    Wraps an EventStore (PostgreSQL or InMemory) and pushes agent lifecycle
    events to a ProgressBroker queue without modifying any ledger/ code.
    """

    def __init__(self, inner, broker: ProgressBroker, run_id: str) -> None:
        self._inner = inner
        self._broker = broker
        self._run_id = run_id

    async def append(self, stream_id: str, events: list[dict], expected_version: int, **kw):
        result = await self._inner.append(stream_id, events, expected_version, **kw)
        for e in events:
            etype = e.get("event_type", "")
            if etype in _EVENT_MAP:
                payload = e.get("payload", {})
                sse_name = _EVENT_MAP[etype]
                self._broker.push(self._run_id, sse_name, _sanitise(payload))
        return result

    # Delegate all other methods to the inner store
    async def load_stream(self, *args, **kw):
        return await self._inner.load_stream(*args, **kw)

    async def load_all(self, *args, **kw):
        # load_all is an async generator — must yield
        async for event in self._inner.load_all(*args, **kw):
            yield event

    async def stream_version(self, *args, **kw):
        return await self._inner.stream_version(*args, **kw)

    async def get_event(self, *args, **kw):
        return await self._inner.get_event(*args, **kw)

    async def connect(self, *args, **kw):
        return await self._inner.connect(*args, **kw)

    async def close(self, *args, **kw):
        return await self._inner.close(*args, **kw)

    async def save_checkpoint(self, *args, **kw):
        if hasattr(self._inner, "save_checkpoint"):
            return await self._inner.save_checkpoint(*args, **kw)

    async def load_checkpoint(self, *args, **kw):
        if hasattr(self._inner, "load_checkpoint"):
            return await self._inner.load_checkpoint(*args, **kw)
        return 0


def _sanitise(payload: dict) -> dict:
    """Convert non-JSON-serialisable types to strings."""
    result = {}
    for k, v in payload.items():
        try:
            json.dumps(v)
            result[k] = v
        except (TypeError, ValueError):
            result[k] = str(v)
    return result
