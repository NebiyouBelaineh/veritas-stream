"""
tests/test_schema.py
====================
Proves structural guarantees required by the TRP spec.

Each test is named after the requirement it verifies, not the table column
it happens to touch.  Run against a real PostgreSQL database:

    DATABASE_URL=postgresql://postgres:apex@localhost/apex_ledger_test \
        pytest tests/test_schema.py -v

The fixtures apply schema.sql fresh on each session.
"""
from __future__ import annotations
import os
import pytest
import pytest_asyncio
import asyncpg

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:apex@localhost/apex_ledger",
)
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schema.sql")


# ─── FIXTURES ─────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture()
async def conn():
    """Per-test connection; applies schema before each test."""
    c = await asyncpg.connect(DB_URL)
    with open(SCHEMA_PATH) as f:
        await c.execute(f.read())
    yield c
    await c.close()


async def _columns(conn, table: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = $1 AND table_schema = 'public'",
        table,
    )
    return {r["column_name"] for r in rows}


async def _constraints(conn, table: str) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT tc.constraint_name, tc.constraint_type,
               kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        WHERE tc.table_name   = $1
          AND tc.table_schema = 'public'
        """,
        table,
    )
    return [dict(r) for r in rows]


# ─── REQUIREMENT: All 4 required tables exist ────────────────────────────────

@pytest.mark.asyncio
async def test_all_required_tables_exist(conn):
    """The TRP spec mandates exactly these 4 tables."""
    rows = await conn.fetch(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public'"
    )
    tables = {r["table_name"] for r in rows}
    required = {"events", "event_streams", "projection_checkpoints", "outbox"}
    assert required.issubset(tables), (
        f"Missing tables: {required - tables}"
    )


# ─── REQUIREMENT: Append-only guarantee enforced at DB level ─────────────────

@pytest.mark.asyncio
async def test_unique_stream_position_constraint_exists(conn):
    """
    UNIQUE(stream_id, stream_position) must be present so that two concurrent
    writes at the same position are rejected by the database even if
    application-level OCC is bypassed.  This is the last line of defence for
    the append-only invariant.
    """
    constraints = await _constraints(conn, "events")
    unique_cols = {
        c["column_name"]
        for c in constraints
        if c["constraint_type"] == "UNIQUE"
    }
    assert "stream_id" in unique_cols and "stream_position" in unique_cols, (
        "UNIQUE(stream_id, stream_position) constraint not found on events table"
    )


@pytest.mark.asyncio
async def test_db_rejects_duplicate_stream_position(conn):
    """
    Attempting to insert two events at the same (stream_id, stream_position)
    must raise a DB-level unique violation — independent of application code.
    """
    sid = "test-append-only-guarantee"
    await conn.execute(
        "DELETE FROM events WHERE stream_id = $1", sid
    )
    await conn.execute(
        "INSERT INTO events(stream_id, stream_position, event_type, payload) "
        "VALUES($1, 1, 'TestEvent', '{}'::jsonb)",
        sid,
    )
    with pytest.raises(asyncpg.UniqueViolationError):
        await conn.execute(
            "INSERT INTO events(stream_id, stream_position, event_type, payload) "
            "VALUES($1, 1, 'TestEvent', '{}'::jsonb)",
            sid,
        )
    # cleanup
    await conn.execute("DELETE FROM events WHERE stream_id = $1", sid)


# ─── REQUIREMENT: Global ordering is gap-free and non-reusable ───────────────

@pytest.mark.asyncio
async def test_global_position_is_generated_always_as_identity(conn):
    """
    global_position must be GENERATED ALWAYS AS IDENTITY.
    This guarantees no gaps from manual inserts and no reuse,
    which ProjectionDaemon relies on for reliable checkpoint-based replay.
    """
    row = await conn.fetchrow(
        """
        SELECT is_identity, identity_generation
        FROM information_schema.columns
        WHERE table_name = 'events'
          AND column_name = 'global_position'
          AND table_schema = 'public'
        """
    )
    assert row is not None, "global_position column not found"
    assert row["is_identity"] == "YES", "global_position must be an identity column"
    assert row["identity_generation"] == "ALWAYS", (
        "global_position must be GENERATED ALWAYS (not BY DEFAULT) "
        "to prevent manual overrides"
    )


# ─── REQUIREMENT: Outbox entry cannot exist without its event ────────────────

@pytest.mark.asyncio
async def test_outbox_event_id_has_foreign_key_to_events(conn):
    """
    outbox.event_id must reference events(event_id) with a foreign key.
    This guarantees that a message cannot be queued for delivery unless
    its event was successfully written — the transactional delivery guarantee.
    """
    rows = await conn.fetch(
        """
        SELECT kcu.column_name, ccu.table_name AS foreign_table,
               ccu.column_name AS foreign_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema    = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_name      = 'outbox'
          AND tc.table_schema    = 'public'
        """
    )
    fks = {(r["column_name"], r["foreign_table"], r["foreign_column"]) for r in rows}
    assert ("event_id", "events", "event_id") in fks, (
        "outbox.event_id must have a FOREIGN KEY referencing events(event_id)"
    )


@pytest.mark.asyncio
async def test_outbox_rejects_orphan_entry(conn):
    """
    Inserting an outbox row with a non-existent event_id must fail at the
    DB level — proving the transactional delivery guarantee holds.
    """
    import uuid
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await conn.execute(
            "INSERT INTO outbox(event_id, destination, payload) "
            "VALUES($1, 'test', '{}'::jsonb)",
            uuid.uuid4(),
        )


# ─── REQUIREMENT: OCC support — stream registry has current_version ──────────

@pytest.mark.asyncio
async def test_event_streams_has_current_version_column(conn):
    """
    event_streams.current_version enables optimistic concurrency control:
    EventStore.append() does SELECT FOR UPDATE on this row to serialise
    concurrent appends without requiring distributed application locks.
    """
    cols = await _columns(conn, "event_streams")
    assert "current_version" in cols, (
        "event_streams must have current_version column for OCC"
    )


# ─── REQUIREMENT: EventStore columns match what EventStore.append() writes ───

@pytest.mark.asyncio
async def test_events_table_has_all_required_columns(conn):
    cols = await _columns(conn, "events")
    required = {
        "event_id", "stream_id", "stream_position", "global_position",
        "event_type", "event_version", "payload", "metadata", "recorded_at",
    }
    assert required.issubset(cols), f"Missing columns: {required - cols}"


@pytest.mark.asyncio
async def test_projection_checkpoints_schema(conn):
    cols = await _columns(conn, "projection_checkpoints")
    assert {"projection_name", "last_position", "updated_at"}.issubset(cols)


@pytest.mark.asyncio
async def test_outbox_schema(conn):
    cols = await _columns(conn, "outbox")
    assert {"id", "event_id", "destination", "payload",
            "created_at", "published_at", "attempts"}.issubset(cols)


# ─── REQUIREMENT: recorded_at uses clock_timestamp(), not NOW() ──────────────

@pytest.mark.asyncio
async def test_recorded_at_column_default_uses_clock_timestamp(conn):
    """
    recorded_at must default to clock_timestamp(), not NOW().
    NOW() freezes at transaction start: every event in a single transaction
    gets the same timestamp. clock_timestamp() gives each row its actual
    write time, which ProjectionDaemon and time-range queries depend on.
    """
    row = await conn.fetchrow(
        """
        SELECT column_default
        FROM information_schema.columns
        WHERE table_name  = 'events'
          AND column_name = 'recorded_at'
          AND table_schema = 'public'
        """
    )
    assert row is not None, "recorded_at column not found on events table"
    assert "clock_timestamp" in (row["column_default"] or ""), (
        f"recorded_at default must use clock_timestamp(), got: {row['column_default']!r}"
    )


# ─── REQUIREMENT: event_streams supports stream archival ─────────────────────

@pytest.mark.asyncio
async def test_event_streams_has_archived_at_column(conn):
    """
    event_streams.archived_at enables stream lifecycle management.
    EventStore.archive_stream() sets this column; StreamMetadata.is_archived
    is derived from it. Without this column the archive feature cannot exist.
    """
    cols = await _columns(conn, "event_streams")
    assert "archived_at" in cols, (
        "event_streams must have an archived_at column for stream archival support"
    )


# ─── REQUIREMENT: Indexes cover all four read patterns ───────────────────────

@pytest.mark.asyncio
async def test_indexes_cover_four_read_patterns(conn):
    """
    The events table must be indexed for all four read patterns the store serves:
      1. Stream-ordered reads     — filters by stream_id, orders by stream_position
      2. Global position reads    — orders by global_position (ProjectionDaemon)
      3. Event type filtering     — WHERE event_type = $1 (load_all filters)
      4. Time-range queries       — WHERE recorded_at BETWEEN ... (audit, compliance)
    Missing any index means one of these patterns degrades to a full table scan.
    """
    rows = await conn.fetch(
        "SELECT indexdef FROM pg_indexes "
        "WHERE tablename = 'events' AND schemaname = 'public'"
    )
    index_defs = " ".join(r["indexdef"].lower() for r in rows)

    assert "stream_id" in index_defs, (
        "No index on stream_id — stream-ordered reads will full-scan"
    )
    assert "global_position" in index_defs, (
        "No index on global_position — ProjectionDaemon checkpoint resume will full-scan"
    )
    assert "event_type" in index_defs, (
        "No index on event_type — load_all(event_type=...) filters will full-scan"
    )
    assert "recorded_at" in index_defs, (
        "No index on recorded_at — time-range audit queries will full-scan"
    )
