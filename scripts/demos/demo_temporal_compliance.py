"""
scripts/demos/demo_temporal_compliance.py
==========================================
Demonstrates temporal compliance queries: ask what the compliance state of an
application was at any past point in time, not just right now.

This is the Python equivalent of the MCP resource:
    ledger://applications/{id}/compliance/{as_of}

Temporal ordering uses `evaluated_at` in the event payload (the time the
compliance rule was actually evaluated), not `recorded_at` (the DB write time).
This gives a stable, replay-safe timeline.

What you will see:
  1. Two compliance rules are evaluated 80 seconds apart:
       AML-001  evaluated at T - 100s
       KYC-001  evaluated at T - 20s
  2. A query as of T - 60s returns only AML-001 (KYC-001 had not been
     evaluated yet at that point in time).
  3. A query at current time returns both rules and the CLEAR verdict.

Run:
    uv run python scripts/demos/demo_temporal_compliance.py
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from ledger.event_store import EventStore
from ledger.projections.compliance_audit import ComplianceAuditViewProjection

DB_URL = os.environ["DATABASE_URL"]
SEP = "-" * 60

APP_ID     = "DEMO-TEMPORAL-001"
SESSION_ID = "sess-temporal-001"


async def main():
    store = EventStore(DB_URL)
    await store.connect()

    compliance_stream = f"compliance-{APP_ID}"

    # -- clean up any prior run
    async with store._pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM outbox WHERE event_id IN "
            "(SELECT event_id FROM events WHERE stream_id=$1)", compliance_stream
        )
        await conn.execute("DELETE FROM events        WHERE stream_id=$1", compliance_stream)
        await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", compliance_stream)

    now = datetime.now(timezone.utc)
    t_minus_120 = now - timedelta(seconds=120)
    t_minus_100 = now - timedelta(seconds=100)  # AML-001 evaluated here
    t_minus_20  = now - timedelta(seconds=20)   # KYC-001 evaluated here
    t_minus_10  = now - timedelta(seconds=10)   # verdict issued here
    t_minus_60  = now - timedelta(seconds=60)   # our "as_of" query point

    # ── STEP 1: Write compliance events to the store ──────────────────────────
    print(SEP)
    print("STEP 1 — Seed compliance events with explicit evaluated_at timestamps")
    print(SEP)
    print(f"  T - 120s : ComplianceCheckInitiated")
    print(f"  T - 100s : ComplianceRulePassed  (AML-001)")
    print(f"  T -  20s : ComplianceRulePassed  (KYC-001)")
    print(f"  T -  10s : ComplianceCheckCompleted  verdict=CLEAR")
    print()
    print(f"  Query point A: T - 60s  (between AML-001 and KYC-001 evaluations)")
    print(f"  Query point B: now      (after all rules evaluated)")

    compliance_events = [
        {
            "event_type": "ComplianceCheckInitiated",
            "event_version": 1,
            "payload": {
                "application_id": APP_ID,
                "session_id": SESSION_ID,
                "regulation_set_version": "2026-Q1",
                "rules_to_evaluate": ["AML-001", "KYC-001"],
                "initiated_at": t_minus_120.isoformat(),
            },
        },
        {
            "event_type": "ComplianceRulePassed",
            "event_version": 1,
            "payload": {
                "application_id": APP_ID,
                "session_id": SESSION_ID,
                "rule_id": "AML-001",
                "rule_name": "Anti-Money Laundering Screening",
                "rule_version": "v3.2",
                "evidence_hash": "aml-hash-001",
                "evaluation_notes": "No AML flags found in screening database.",
                "evaluated_at": t_minus_100.isoformat(),
            },
        },
        {
            "event_type": "ComplianceRulePassed",
            "event_version": 1,
            "payload": {
                "application_id": APP_ID,
                "session_id": SESSION_ID,
                "rule_id": "KYC-001",
                "rule_name": "Know Your Customer Identity Verification",
                "rule_version": "v2.1",
                "evidence_hash": "kyc-hash-001",
                "evaluation_notes": "Identity documents verified against registry.",
                "evaluated_at": t_minus_20.isoformat(),
            },
        },
        {
            "event_type": "ComplianceCheckCompleted",
            "event_version": 1,
            "payload": {
                "application_id": APP_ID,
                "session_id": SESSION_ID,
                "rules_evaluated": 2,
                "rules_passed": 2,
                "rules_failed": 0,
                "rules_noted": 0,
                "has_hard_block": False,
                "overall_verdict": "CLEAR",
                "completed_at": t_minus_10.isoformat(),
            },
        },
    ]

    await store.append(compliance_stream, compliance_events, expected_version=-1)
    print(f"\n  Written {len(compliance_events)} events to {compliance_stream!r}")

    # ── STEP 2: Load events and feed into projection ───────────────────────────
    print()
    print(SEP)
    print("STEP 2 — Load events from store and materialise compliance projection")
    print(SEP)

    proj = ComplianceAuditViewProjection()
    stored_events = await store.load_stream(compliance_stream)
    for ev in stored_events:
        await proj.handle(ev)
    print(f"  Projection materialised from {len(stored_events)} stored events.")

    # ── STEP 3: Temporal query — as of T - 60s ────────────────────────────────
    print()
    print(SEP)
    print(f"STEP 3 — Temporal query: compliance state as of T - 60s")
    print(f"         (AML-001 was evaluated at T-100s, KYC-001 at T-20s)")
    print(SEP)

    snapshot_past = proj.get_compliance_at(APP_ID, t_minus_60)
    print(f"  rules_passed  : {sorted(snapshot_past.rules_passed)}")
    print(f"  rules_failed  : {sorted(snapshot_past.rules_failed)}")
    print(f"  hard_blocked  : {snapshot_past.hard_blocked}")
    print(f"  verdict       : {snapshot_past.verdict}")

    assert "AML-001" in snapshot_past.rules_passed, "AML-001 must be visible at T-60s"
    assert "KYC-001" not in snapshot_past.rules_passed, (
        "KYC-001 must NOT be visible at T-60s (it was evaluated at T-20s)"
    )
    assert snapshot_past.verdict is None, "Verdict must be None at T-60s (check not completed yet)"
    print("  CORRECT — only AML-001 visible; KYC-001 had not been evaluated yet.")

    # ── STEP 4: Current query — both rules present ────────────────────────────
    print()
    print(SEP)
    print("STEP 4 — Current state query (both rules evaluated, verdict issued)")
    print(SEP)

    snapshot_now = proj.get_compliance_at(APP_ID, now)
    print(f"  rules_passed  : {sorted(snapshot_now.rules_passed)}")
    print(f"  rules_failed  : {sorted(snapshot_now.rules_failed)}")
    print(f"  hard_blocked  : {snapshot_now.hard_blocked}")
    print(f"  verdict       : {snapshot_now.verdict}")

    assert "AML-001" in snapshot_now.rules_passed, "AML-001 must be present now"
    assert "KYC-001" in snapshot_now.rules_passed, "KYC-001 must be present now"
    assert snapshot_now.verdict == "CLEAR", f"Expected CLEAR, got {snapshot_now.verdict!r}"
    print("  CORRECT — both rules present, verdict CLEAR.")

    print()
    print("PASSED — temporal query returns different compliance state at different timestamps.")

    await store.close()


if __name__ == "__main__":
    asyncio.run(main())
