import pytest
"""
tests/test_event_store.py
=========================
Phase 1 tests: EventStore implementation.
These tests FAIL until you implement EventStore. That is expected.
When all pass, your event store is correct.

Run: pytest tests/test_event_store.py -v
"""
import asyncio, os, pytest, sys
from pathlib import Path; sys.path.insert(0, str(Path(__file__).parent.parent))
from ledger.event_store import (
    EventStore,
    InMemoryEventStore,
    OptimisticConcurrencyError,
    StreamArchivedError,
    StreamMetadata,
)

DB_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:apex@localhost/apex_ledger")

@pytest.fixture
async def store():
    s = EventStore(DB_URL); await s.connect()
    assert s._pool is not None
    async with s._pool.acquire() as conn:
        await conn.execute("DELETE FROM events WHERE stream_id LIKE 'test-%'")
        await conn.execute("DELETE FROM event_streams WHERE stream_id LIKE 'test-%'")
    yield s
    await s.close()

def _event(etype, n=1):
    return [{"event_type":etype,"event_version":1,"payload":{"seq":i,"test":True}} for i in range(n)]

@pytest.mark.asyncio
async def test_append_new_stream(store):
    positions = await store.append("test-new-001", _event("TestEvent"), expected_version=-1)
    assert positions == [1]

@pytest.mark.asyncio
async def test_append_existing_stream(store):
    await store.append("test-exist-001", _event("TestEvent"), expected_version=-1)
    positions = await store.append("test-exist-001", _event("TestEvent2"), expected_version=1)
    assert positions == [2]

@pytest.mark.asyncio
async def test_occ_wrong_version_raises(store):
    await store.append("test-occ-001", _event("E"), expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append("test-occ-001", _event("E"), expected_version=99)
    assert exc.value.expected == 99; assert exc.value.actual == 1

@pytest.mark.asyncio
async def test_concurrent_double_append_exactly_one_succeeds(store):
    """The critical OCC test: two concurrent appends, exactly one wins."""
    await store.append("test-concurrent-001", _event("Init"), expected_version=-1)
    results = await asyncio.gather(
        store.append("test-concurrent-001", _event("A"), expected_version=1),
        store.append("test-concurrent-001", _event("B"), expected_version=1),
        return_exceptions=True,
    )
    successes = [r for r in results if isinstance(r, list)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}"
    assert len(errors) == 1

@pytest.mark.asyncio
async def test_load_stream_ordered(store):
    await store.append("test-load-001", _event("E",3), expected_version=-1)
    events = await store.load_stream("test-load-001")
    assert len(events) == 3
    positions = [e["stream_position"] for e in events]
    assert positions == sorted(positions)

@pytest.mark.asyncio
async def test_stream_version(store):
    await store.append("test-ver-001", _event("E",4), expected_version=-1)
    assert await store.stream_version("test-ver-001") == 4

@pytest.mark.asyncio
async def test_stream_version_nonexistent(store):
    assert await store.stream_version("test-does-not-exist") == -1

@pytest.mark.asyncio
async def test_load_all_yields_in_global_order(store):
    await store.append("test-global-A", _event("E",2), expected_version=-1)
    await store.append("test-global-B", _event("E",2), expected_version=-1)
    all_events = [e async for e in store.load_all(from_position=0)]
    positions = [e["global_position"] for e in all_events]
    assert positions == sorted(positions)


# ── archive_stream / get_stream_metadata (in-memory, no DB required) ──────────

@pytest.fixture
def mem():
    return InMemoryEventStore()


async def test_get_stream_metadata_returns_correct_version(mem):
    """get_stream_metadata.current_version agrees with stream_version()."""
    await mem.append("loan-meta-001", _event("E", 3), expected_version=-1)
    expected_v = await mem.stream_version("loan-meta-001")
    meta = await mem.get_stream_metadata("loan-meta-001")
    assert isinstance(meta, StreamMetadata)
    assert meta.stream_id == "loan-meta-001"
    assert meta.current_version == expected_v
    assert meta.is_archived is False


async def test_get_stream_metadata_raises_for_unknown_stream(mem):
    """get_stream_metadata raises KeyError for a stream that has never been written to."""
    with pytest.raises(KeyError, match="not found"):
        await mem.get_stream_metadata("loan-does-not-exist")


async def test_archive_stream_prevents_further_appends(mem):
    """archive_stream marks a stream closed; subsequent appends raise StreamArchivedError."""
    await mem.append("loan-arch-001", _event("E"), expected_version=-1)
    await mem.archive_stream("loan-arch-001")
    with pytest.raises(StreamArchivedError):
        await mem.append("loan-arch-001", _event("E2"), expected_version=1)


async def test_archive_stream_reflects_in_metadata(mem):
    """get_stream_metadata.is_archived returns True after archive_stream is called."""
    await mem.append("loan-arch-002", _event("E"), expected_version=-1)
    meta_before = await mem.get_stream_metadata("loan-arch-002")
    assert meta_before.is_archived is False

    await mem.archive_stream("loan-arch-002")
    meta_after = await mem.get_stream_metadata("loan-arch-002")
    assert meta_after.is_archived is True


async def test_archive_stream_raises_for_unknown_stream(mem):
    """archive_stream raises KeyError when the stream does not exist."""
    with pytest.raises(KeyError):
        await mem.archive_stream("loan-never-written")
