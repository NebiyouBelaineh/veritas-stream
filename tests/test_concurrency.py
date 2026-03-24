"""
tests/test_concurrency.py
=========================
OCC double-decision test. Two agents attempt to append at expected_version=3.
Exactly one must succeed; the other must raise OptimisticConcurrencyError.
Stream must have exactly 4 events total.

Run: pytest tests/test_concurrency.py -v
"""
import asyncio
import pytest

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError


def _ev(event_type: str) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": {}}


async def test_concurrent_double_append_exactly_one_succeeds():
    """
    Two agents simultaneously attempt to append to the same stream at version 3.
    Exactly one must succeed. The other must raise OptimisticConcurrencyError.
    Stream must have exactly 4 events total (3 pre-existing + 1 winner).
    """
    store = InMemoryEventStore()

    # Setup: stream at version 3 (three pre-existing events, 1-based versioning)
    await store.append("loan-occ-test", [_ev("E1")], expected_version=-1)
    await store.append("loan-occ-test", [_ev("E2")], expected_version=1)
    await store.append("loan-occ-test", [_ev("E3")], expected_version=2)

    # Two concurrent appends at expected_version=3 (the race condition)
    results = await asyncio.gather(
        store.append("loan-occ-test", [_ev("A")], expected_version=3),
        store.append("loan-occ-test", [_ev("B")], expected_version=3),
        return_exceptions=True,
    )

    successes = [r for r in results if isinstance(r, list)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]

    assert len(successes) == 1, \
        f"Expected exactly 1 success, got {len(successes)}: {results}"
    assert len(errors) == 1, \
        f"Expected exactly 1 OptimisticConcurrencyError, got {len(errors)}: {results}"
    assert successes[0] == [4], \
        f"Winner's event must be at stream_position 4, got {successes[0]}"
    assert await store.stream_version("loan-occ-test") == 4, \
        "Stream must have exactly 4 events (3 pre-existing + 1 winner)"
