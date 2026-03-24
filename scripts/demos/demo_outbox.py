"""
scripts/demos/demo_outbox.py
==============================
Demonstrates the transactional outbox pattern.

Every event.append() writes an outbox row IN THE SAME TRANSACTION as the event
insert.  This guarantees that an outbox entry can never exist without its event,
and an event can never exist without an outbox entry -- even if the process
crashes between the two writes.

What you will see:
  1. Submit an application -- one event appended, one outbox row created.
  2. Inspect the outbox table: event_id matches, published_at is NULL (undelivered).
  3. Simulate a relay publishing the outbox entries (mark published_at).
  4. Show that re-running append creates NEW outbox rows for new events.
  5. Show that the outbox row count always equals the event count for this stream.

Run:
    uv run python scripts/demos/demo_outbox.py
"""
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from ledger.event_store import EventStore

DB_URL = os.environ["DATABASE_URL"]
SEP = "-" * 60
APP_ID = "DEMO-OUTBOX-001"
STREAM_ID = f"loan-{APP_ID}"


async def fetch_outbox(conn, stream_id: str) -> list[dict]:
    rows = await conn.fetch(
        "SELECT o.id, o.event_id, o.destination, o.published_at, o.attempts, "
        "       e.event_type, e.stream_position "
        "FROM outbox o "
        "JOIN events e ON e.event_id = o.event_id "
        "WHERE e.stream_id = $1 "
        "ORDER BY e.stream_position ASC",
        stream_id,
    )
    return [dict(r) for r in rows]


def _print_outbox(rows: list[dict]):
    if not rows:
        print("  (empty)")
        return
    for r in rows:
        status = f"published at {r['published_at']}" if r["published_at"] else "PENDING"
        print(f"  pos={r['stream_position']}  {r['event_type']:<35} outbox: {status}")


async def main():
    store = EventStore(DB_URL)
    await store.connect()

    # clean up prior run
    async with store._pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM outbox WHERE event_id IN "
            "(SELECT event_id FROM events WHERE stream_id=$1)", STREAM_ID
        )
        await conn.execute("DELETE FROM events        WHERE stream_id=$1", STREAM_ID)
        await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", STREAM_ID)

    # ── Step 1: append one event ──────────────────────────────────────────────
    print(SEP)
    print("STEP 1 — append ApplicationSubmitted")
    print(SEP)

    await store.append(STREAM_ID, [{
        "event_type": "ApplicationSubmitted",
        "event_version": 1,
        "payload": {
            "application_id": APP_ID,
            "applicant_id": "COMP-001",
            "requested_amount_usd": "500000",
            "loan_purpose": "working_capital",
            "loan_term_months": 36,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        },
    }], expected_version=-1)

    # ── Step 2: inspect outbox ────────────────────────────────────────────────
    print()
    print(SEP)
    print("STEP 2 — inspect outbox table (event written, outbox row PENDING)")
    print(SEP)

    async with store._pool.acquire() as conn:
        rows = await fetch_outbox(conn, STREAM_ID)

    _print_outbox(rows)
    assert len(rows) == 1, "one event must produce exactly one outbox row"
    assert rows[0]["published_at"] is None, "new outbox row must be unpublished"
    assert rows[0]["destination"] == "ApplicationSubmitted"

    outbox_event_id = rows[0]["event_id"]
    print(f"\n  event_id in outbox matches events table: {outbox_event_id}")

    # verify same event_id is in events table
    async with store._pool.acquire() as conn:
        ev_row = await conn.fetchrow(
            "SELECT event_id FROM events WHERE stream_id=$1", STREAM_ID
        )
    assert str(ev_row["event_id"]) == str(outbox_event_id)
    print("  Confirmed: event_id is identical in both tables (same transaction).")

    # ── Step 3: simulate relay publishing ────────────────────────────────────
    print()
    print(SEP)
    print("STEP 3 — simulate outbox relay marking entry as published")
    print(SEP)

    now = datetime.now(timezone.utc)
    async with store._pool.acquire() as conn:
        await conn.execute(
            "UPDATE outbox SET published_at=$1, attempts=1 WHERE event_id=$2",
            now, outbox_event_id,
        )

    async with store._pool.acquire() as conn:
        rows = await fetch_outbox(conn, STREAM_ID)
    _print_outbox(rows)
    assert rows[0]["published_at"] is not None, "outbox row must now be published"

    # ── Step 4: append two more events (each gets its own outbox row) ─────────
    print()
    print(SEP)
    print("STEP 4 — append two more events (batch append)")
    print(SEP)

    ver = await store.stream_version(STREAM_ID)
    await store.append(STREAM_ID, [
        {"event_type": "DocumentUploaded",        "event_version": 1, "payload": {}},
        {"event_type": "CreditAnalysisRequested", "event_version": 1, "payload": {}},
    ], expected_version=ver)

    async with store._pool.acquire() as conn:
        rows = await fetch_outbox(conn, STREAM_ID)

    print("  Full outbox for this stream:")
    _print_outbox(rows)

    pending = [r for r in rows if r["published_at"] is None]
    print(f"\n  Total outbox rows : {len(rows)}  (must equal event count = 3)")
    print(f"  Pending (undelivered): {len(pending)}  (the 2 new events)")
    assert len(rows) == 3
    assert len(pending) == 2

    # ── Step 5: prove atomicity -- count must always match ────────────────────
    print()
    print(SEP)
    print("STEP 5 — atomicity check: outbox row count == event count")
    print(SEP)

    async with store._pool.acquire() as conn:
        ev_count = await conn.fetchval(
            "SELECT COUNT(*) FROM events WHERE stream_id=$1", STREAM_ID
        )
        ob_count = await conn.fetchval(
            "SELECT COUNT(*) FROM outbox WHERE event_id IN "
            "(SELECT event_id FROM events WHERE stream_id=$1)", STREAM_ID
        )
    print(f"  events table : {ev_count} rows")
    print(f"  outbox table : {ob_count} rows")
    assert ev_count == ob_count, "FAIL: outbox/event count mismatch!"
    print("  Counts match: transactional guarantee holds.")

    print()
    print("PASSED — outbox entries written atomically with events.")

    await store.close()


if __name__ == "__main__":
    asyncio.run(main())
