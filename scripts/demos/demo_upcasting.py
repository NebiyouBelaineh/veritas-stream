"""
scripts/demos/demo_upcasting.py
================================
Demonstrates event upcasting: a v1 CreditAnalysisCompleted event stored in the
DB is transparently read back as v2, gaining the fields added in the migration
(confidence_score, regulatory_basis, model_version) -- without touching the
stored bytes.

What you will see:
  1. A v1 event is written (simulating old data before the schema evolved).
  2. The raw row in the DB still shows event_version=1.
  3. load_stream() with an UpcasterRegistry returns the same event at v2.
  4. The stored row in the DB is confirmed unchanged (immutability guarantee).

Run:
    uv run python scripts/demos/demo_upcasting.py
"""
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from ledger.event_store import EventStore
from ledger.upcasting.upcasters import create_registry

DB_URL = os.environ["DATABASE_URL"]

SEP = "-" * 60


def _fmt(event: dict) -> str:
    p = event["payload"]
    return (
        f"  event_type    : {event['event_type']}\n"
        f"  event_version : {event['event_version']}\n"
        f"  stream_pos    : {event['stream_position']}\n"
        f"  payload keys  : {sorted(p.keys())}\n"
        f"  confidence_score : {p.get('confidence_score', '<missing>')}\n"
        f"  regulatory_basis : {p.get('regulatory_basis', '<missing>')}\n"
        f"  model_version    : {p.get('model_version', '<missing>')}"
    )


async def main():
    registry = create_registry()
    store_raw  = EventStore(DB_URL)                       # no upcasting
    store_up   = EventStore(DB_URL, upcaster_registry=registry)  # upcasting on

    await store_raw.connect()
    await store_up.connect()

    app_id    = "DEMO-UPCAST-001"
    stream_id = f"credit-{app_id}"

    # -- clean up any prior run
    async with store_raw._pool.acquire() as conn:
        await conn.execute("DELETE FROM outbox WHERE event_id IN "
                           "(SELECT event_id FROM events WHERE stream_id=$1)", stream_id)
        await conn.execute("DELETE FROM events        WHERE stream_id=$1", stream_id)
        await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", stream_id)

    print(SEP)
    print("STEP 1 — write a v1 CreditAnalysisCompleted event (old schema)")
    print(SEP)

    # v1 payload has NO confidence_score, regulatory_basis, or model_version
    v1_payload = {
        "application_id": app_id,
        "session_id":     "sess-legacy-001",
        "decision": {
            "risk_tier":             "MEDIUM",
            "recommended_limit_usd": 175000,
        },
        "model_deployment_id":  "credit-model-v1.0",
        "input_data_hash":      "abc123",
        "analysis_duration_ms": 820,
        "completed_at":         "2025-06-15T09:00:00+00:00",
    }

    await store_raw.append(stream_id, [{
        "event_type":    "CreditAnalysisCompleted",
        "event_version": 1,                              # intentionally v1
        "payload":       v1_payload,
    }], expected_version=-1)

    print("Written. Payload keys:", sorted(v1_payload.keys()))

    print()
    print(SEP)
    print("STEP 2 — read back WITHOUT upcasting (raw store)")
    print(SEP)

    raw_events = await store_raw.load_stream(stream_id)
    raw = raw_events[0]
    print(_fmt(raw))
    assert raw["event_version"] == 1, "raw store must return v1"

    print()
    print(SEP)
    print("STEP 3 — read back WITH upcasting (v1 → v2 applied in memory)")
    print(SEP)

    up_events = await store_up.load_stream(stream_id)
    up = up_events[0]
    print(_fmt(up))
    assert up["event_version"] == 2, "upcasting store must return v2"
    assert up["payload"]["confidence_score"] is None,  "confidence_score must be None (not fabricated)"
    assert isinstance(up["payload"]["regulatory_basis"], list), "regulatory_basis must be a list"
    assert isinstance(up["payload"]["model_version"], str),     "model_version must be inferred"

    print()
    print(SEP)
    print("STEP 4 — confirm DB row is still v1 (immutability guarantee)")
    print(SEP)

    async with store_raw._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT event_version, payload FROM events WHERE stream_id=$1", stream_id
        )
    stored_version = row["event_version"]
    raw_payload = row["payload"]
    stored_payload = raw_payload if isinstance(raw_payload, dict) else __import__("json").loads(raw_payload)

    print(f"  DB event_version : {stored_version}  (must still be 1)")
    print(f"  DB payload keys  : {sorted(stored_payload.keys())}")
    assert stored_version == 1, "FAIL: DB row was mutated!"
    assert "confidence_score" not in stored_payload, "FAIL: DB payload was modified!"

    print()
    print("PASSED — v1 event upcasted to v2 at read time; DB row unchanged.")

    await store_raw.close()
    await store_up.close()


if __name__ == "__main__":
    asyncio.run(main())
