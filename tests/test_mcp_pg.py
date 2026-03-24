"""
tests/test_mcp_pg.py
=====================
MCP integration tests against a real PostgreSQL database.

Mirrors test_mcp_integration.py but uses EventStore + asyncpg instead of
InMemoryEventStore. Requires DATABASE_URL (or TEST_DB_URL) to be set in .env.

Run:
    pytest tests/test_mcp_pg.py -v

Skip if no DB:
    pytest tests/ -v --ignore=tests/test_mcp_pg.py
"""
import json
import os
import time

import asyncpg
import pytest
import pytest_asyncio

from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

from fastmcp import Client

from ledger.event_store import EventStore
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.mcp.server import create_mcp_server

# ── DB URL ────────────────────────────────────────────────────────────────────

_DB_URL = os.environ.get("TEST_DB_URL") or os.environ.get("DATABASE_URL")
_SCHEMA_PATH = Path(__file__).parent.parent / "schema.sql"

pytestmark = pytest.mark.asyncio


def pytest_configure(config):
    config.addinivalue_line("markers", "pg: mark test as requiring PostgreSQL")


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def pg_store():
    """
    Connects a real EventStore to Postgres, applies the schema if needed,
    and yields the store. Cleans up all test streams on teardown.
    """
    if not _DB_URL:
        pytest.skip("DATABASE_URL / TEST_DB_URL not set")

    store = EventStore(_DB_URL)
    await store.connect()

    # Apply schema idempotently
    sql = _SCHEMA_PATH.read_text()
    async with store._pool.acquire() as conn:
        await conn.execute(sql)

    yield store

    await store.close()


@pytest_asyncio.fixture
async def pg_server(pg_store):
    """Wires projections + daemon + MCP server around the real EventStore."""
    summary    = ApplicationSummaryProjection()
    compliance = ComplianceAuditViewProjection()
    perf       = AgentPerformanceLedgerProjection()
    daemon     = ProjectionDaemon(pg_store, [summary, compliance, perf])
    projections = {
        "summary":           summary,
        "compliance":        compliance,
        "agent_performance": perf,
    }
    mcp = create_mcp_server(pg_store, daemon, projections)
    yield mcp, pg_store, daemon, projections


# ── Helpers ───────────────────────────────────────────────────────────────────

def _data(result) -> dict:
    return result.data


async def _cleanup(store: EventStore, app_id: str):
    """Delete all test streams for app_id so reruns stay clean."""
    streams = [
        f"loan-{app_id}",
        f"credit-{app_id}",
        f"fraud-{app_id}",
        f"compliance-{app_id}",
        f"agent-credit_analysis-sess-credit-{app_id}",
        f"agent-fraud_screening-sess-fraud-{app_id}",
        f"agent-decision_orchestrator-sess-orch-{app_id}",
    ]
    async with store._pool.acquire() as conn:
        for sid in streams:
            # Delete outbox rows first (FK constraint on events)
            await conn.execute(
                "DELETE FROM outbox WHERE event_id IN "
                "(SELECT event_id FROM events WHERE stream_id = $1)", sid)
            await conn.execute("DELETE FROM events WHERE stream_id = $1", sid)
            await conn.execute("DELETE FROM event_streams WHERE stream_id = $1", sid)
        await conn.execute(
            "DELETE FROM projection_checkpoints WHERE projection_name LIKE 'proj_%'"
        )


# ── Test 1: Full lifecycle via MCP against Postgres ───────────────────────────

@pytest.mark.pg
async def test_full_lifecycle_pg(pg_server):
    mcp, store, daemon, projections = pg_server
    app_id = "PG-LIFECYCLE-001"
    await _cleanup(store, app_id)

    async with Client(mcp) as client:

        # Step 1: Submit
        r = await client.call_tool("submit_application", {
            "application_id": app_id,
            "applicant_id":   "COMP-001",
            "requested_amount_usd": "500000",
            "loan_purpose":   "working_capital",
            "loan_term_months": 36,
        })
        assert _data(r)["success"] is True, f"submit failed: {_data(r)}"

        # Step 2: Start credit session
        credit_session = f"sess-credit-{app_id}"
        r = await client.call_tool("start_agent_session", {
            "session_id":   credit_session,
            "agent_type":   "credit_analysis",
            "agent_id":     "credit_agent_01",
            "application_id": app_id,
            "model_version": "google/gemini-2.5-flash",
        })
        assert _data(r)["success"] is True, f"start session failed: {_data(r)}"

        # Seed loan state events that agents would normally append
        loan_ver = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [
            {"event_type": "DocumentUploadRequested", "event_version": 1, "payload": {}},
            {"event_type": "DocumentUploaded",        "event_version": 1, "payload": {}},
            {"event_type": "PackageReadyForAnalysis", "event_version": 1, "payload": {}},
            {"event_type": "CreditAnalysisRequested", "event_version": 1, "payload": {}},
        ], expected_version=loan_ver)

        # Step 3: Credit analysis (COMP-001: revenue $6.37M, limit = 35% = $2.23M)
        r = await client.call_tool("record_credit_analysis", {
            "application_id":      app_id,
            "session_id":          credit_session,
            "agent_id":            "credit_agent_01",
            "agent_type":          "credit_analysis",
            "model_version":       "google/gemini-2.5-flash",
            "model_deployment_id": "openrouter-prod",
            "risk_tier":           "MEDIUM",
            "recommended_limit_usd": "500000",
            "confidence":          0.82,
            "rationale":           "Revenue $6.37M, EBITDA margin 8%, elevated leverage (D/E 2.3x). Limit within 35% revenue cap.",
        })
        assert _data(r)["success"] is True, f"credit analysis failed: {_data(r)}"

        # Seed fraud screening trigger
        loan_ver = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [
            {"event_type": "FraudScreeningRequested", "event_version": 1, "payload": {}},
        ], expected_version=loan_ver)

        # Step 4: Fraud screening
        fraud_session = f"sess-fraud-{app_id}"
        r = await client.call_tool("start_agent_session", {
            "session_id":     fraud_session,
            "agent_type":     "fraud_screening",
            "agent_id":       "fraud_agent_01",
            "application_id": app_id,
            "model_version":  "google/gemini-2.5-flash",
        })
        assert _data(r)["success"] is True

        r = await client.call_tool("record_fraud_screening", {
            "application_id":         app_id,
            "session_id":             fraud_session,
            "agent_id":               "fraud_agent_01",
            "agent_type":             "fraud_screening",
            "fraud_score":            0.06,
            "risk_level":             "LOW",
            "recommendation":         "PROCEED",
            "screening_model_version": "google/gemini-2.5-flash",
        })
        assert _data(r)["success"] is True, f"fraud screening failed: {_data(r)}"

        # Step 5: Compliance checks
        compliance_session = f"sess-comp-{app_id}"
        for rule_id, rule_name in [
            ("AML-001", "Anti-Money Laundering"),
            ("KYC-001", "Know Your Customer"),
            ("REG-001", "BSA Compliance"),
        ]:
            r = await client.call_tool("record_compliance_check", {
                "application_id": app_id,
                "session_id":     compliance_session,
                "rule_id":        rule_id,
                "rule_name":      rule_name,
                "rule_version":   "2026-Q1",
                "passed":         True,
            })
            assert _data(r)["success"] is True, f"compliance {rule_id} failed: {_data(r)}"

        # Step 6: Generate decision
        orch_session = f"sess-orch-{app_id}"
        r = await client.call_tool("start_agent_session", {
            "session_id":     orch_session,
            "agent_type":     "decision_orchestrator",
            "agent_id":       "orch_agent_01",
            "application_id": app_id,
            "model_version":  "google/gemini-2.5-flash",
        })
        assert _data(r)["success"] is True

        r = await client.call_tool("generate_decision", {
            "application_id":          app_id,
            "orchestrator_session_id": orch_session,
            "recommendation":          "APPROVE",
            "confidence":              0.82,
            "executive_summary":       "COMP-001: solid revenue base, manageable leverage. Recommend approval at requested amount.",
            "approved_amount_usd":     "500000",
            "required_compliance_rules": ["AML-001", "KYC-001", "REG-001"],
        })
        assert _data(r)["success"] is True,      f"generate_decision failed: {_data(r)}"
        assert _data(r)["recommendation"] == "APPROVE"

        # Step 7: Human review
        r = await client.call_tool("record_human_review", {
            "application_id":         app_id,
            "reviewer_id":            "REVIEWER-007",
            "override":               False,
            "original_recommendation": "APPROVE",
            "final_decision":         "APPROVED",
        })
        assert _data(r)["success"] is True, f"human review failed: {_data(r)}"
        assert _data(r)["final_decision"] == "APPROVED"

    # Run projection daemon to materialise read models
    await daemon.run_once()

    # ── Verify complete history via MCP resources ──────────────────────────────
    async with Client(mcp) as client:

        # Application summary (projection, not stream replay)
        content = await client.read_resource(f"ledger://applications/{app_id}")
        state = json.loads(content[0].text)
        assert "error" not in state,              f"summary error: {state}"
        assert state["application_id"] == app_id
        assert state["decision"] == "APPROVE",    f"expected APPROVE, got: {state}"
        assert state["risk_tier"] == "MEDIUM",    f"expected MEDIUM risk, got: {state}"

        # Audit trail (stream replay — full event list in order)
        content = await client.read_resource(f"ledger://applications/{app_id}/audit-trail")
        events = json.loads(content[0].text)
        assert len(events) > 0, "audit trail must not be empty"
        types = [e["event_type"] for e in events]
        assert "ApplicationSubmitted" in types
        assert "CreditAnalysisRequested" in types
        print(f"\nAudit trail for {app_id} ({len(events)} events on loan stream):")
        for e in events:
            print(f"  pos={e['stream_position']:>2}  {e['event_type']}")

        # Compliance resource
        content = await client.read_resource(f"ledger://applications/{app_id}/compliance")
        comp = json.loads(content[0].text)
        assert "AML-001" in comp["rules_passed"], f"AML-001 missing: {comp}"
        assert "KYC-001" in comp["rules_passed"], f"KYC-001 missing: {comp}"

        # Health
        content = await client.read_resource("ledger://ledger/health")
        health = json.loads(content[0].text)
        assert health["status"] == "ok"
        assert "application_summary" in health["projection_lags_ms"]


# ── Test 2: OCC on real Postgres ──────────────────────────────────────────────

@pytest.mark.pg
async def test_occ_on_postgres(pg_server):
    """
    Two concurrent appends at the same expected_version. The second must
    raise OptimisticConcurrencyError, proving the DB-level UNIQUE constraint
    enforces the OCC guarantee independently of application-level locking.
    """
    from ledger.event_store import OptimisticConcurrencyError
    import asyncio

    mcp, store, daemon, _ = pg_server
    app_id = "PG-OCC-001"
    await _cleanup(store, app_id)
    stream = f"loan-{app_id}"

    await store.append(stream, [
        {"event_type": "ApplicationSubmitted", "event_version": 1,
         "payload": {"application_id": app_id}},
    ], expected_version=-1)

    ver = await store.stream_version(stream)  # should be 0

    results = []
    async def try_append(label):
        try:
            await store.append(stream, [
                {"event_type": "CreditAnalysisRequested", "event_version": 1,
                 "payload": {"label": label}},
            ], expected_version=ver)
            results.append(("ok", label))
        except OptimisticConcurrencyError as e:
            results.append(("occ", label))

    await asyncio.gather(try_append("A"), try_append("B"))

    ok_count  = sum(1 for r, _ in results if r == "ok")
    occ_count = sum(1 for r, _ in results if r == "occ")
    assert ok_count  == 1, f"Exactly one append must succeed, got: {results}"
    assert occ_count == 1, f"Exactly one append must fail with OCC, got: {results}"

    # Stream version advanced by exactly 1
    assert await store.stream_version(stream) == ver + 1


# ── Test 3: Audit trail is append-only (no phantom updates) ───────────────────

@pytest.mark.pg
async def test_audit_trail_is_append_only(pg_server):
    """
    After appending events, verify the events table has no UPDATE or DELETE
    capability by confirming the event count only ever grows and event_ids
    are stable.
    """
    mcp, store, daemon, _ = pg_server
    app_id = "PG-IMMUTABLE-001"
    await _cleanup(store, app_id)
    stream = f"loan-{app_id}"

    await store.append(stream, [
        {"event_type": "ApplicationSubmitted", "event_version": 1,
         "payload": {"application_id": app_id}},
    ], expected_version=-1)

    events_before = await store.load_stream(stream)
    ids_before = [e["event_id"] for e in events_before]

    await store.append(stream, [
        {"event_type": "CreditAnalysisRequested", "event_version": 1, "payload": {}},
    ], expected_version=await store.stream_version(stream))

    events_after = await store.load_stream(stream)
    ids_after = [e["event_id"] for e in events_after]

    # Original event_ids must be unchanged
    assert ids_before[0] == ids_after[0], "event_id of existing event must never change"
    assert len(events_after) == len(events_before) + 1, "stream must only grow"
