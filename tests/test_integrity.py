"""
tests/test_integrity.py
=========================
Branch 5 integrity and Gas Town tests.

Proves:
  - Cryptographic hash chain detects tampered events.
  - Gas Town: agent context can be reconstructed after a crash.
  - Gas Town: partial decision in progress is flagged as NEEDS_RECONCILIATION.

Run: pytest tests/test_integrity.py -v
"""
import pytest

from ledger.event_store import InMemoryEventStore
from ledger.integrity.audit_chain import run_integrity_check
from ledger.integrity.gas_town import (
    reconstruct_agent_context,
    HEALTHY,
    NEEDS_RECONCILIATION,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ev(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


async def _seed_loan_stream(store, app_id: str, n_events: int = 3) -> None:
    events = [
        _ev("ApplicationSubmitted", application_id=app_id, applicant_id="a1",
            requested_amount_usd="100000"),
    ]
    for i in range(n_events - 1):
        events.append(_ev(f"Event{i}", application_id=app_id, seq=i))
    await store.append(f"loan-{app_id}", events, expected_version=-1)


# ── Test 1: Hash chain verifies on clean stream ───────────────────────────────

@pytest.mark.asyncio
async def test_hash_chain_verifies_on_clean_stream():
    """
    run_integrity_check() returns chain_valid=True, tamper_detected=False
    on an unmodified stream.
    """
    store = InMemoryEventStore()
    app_id = "APEX-AUDIT-001"
    await _seed_loan_stream(store, app_id, n_events=5)

    result = await run_integrity_check(store, f"loan-{app_id}")

    assert result["chain_valid"] is True, \
        f"chain_valid must be True on clean stream, got {result}"
    assert result["tamper_detected"] is False, \
        f"tamper_detected must be False on clean stream, got {result}"
    assert result["events_verified"] == 5
    assert result["integrity_hash"] is not None
    assert result["previous_hash"] is None  # first ever check

    # Running a second check should also pass (no tampering between checks)
    result2 = await run_integrity_check(store, f"loan-{app_id}")
    assert result2["chain_valid"] is True
    assert result2["tamper_detected"] is False
    assert result2["previous_hash"] == result["integrity_hash"]


# ── Test 2: Tampered event breaks hash chain ──────────────────────────────────

@pytest.mark.asyncio
async def test_tampered_event_breaks_hash_chain():
    """
    Directly update a payload in the store (simulating DB-level tampering);
    run_integrity_check() then returns tamper_detected=True.

    This proves the cryptographic guarantee: any modification to a stored
    event payload is detectable.
    """
    store = InMemoryEventStore()
    app_id = "APEX-AUDIT-002"
    await _seed_loan_stream(store, app_id, n_events=4)

    # First integrity check — establishes the baseline hash
    result1 = await run_integrity_check(store, f"loan-{app_id}")
    assert result1["chain_valid"] is True
    assert result1["tamper_detected"] is False

    # ── TAMPER: directly modify the second event's payload ────────────────────
    # In production this would be a direct UPDATE on the DB events table.
    # With InMemoryEventStore we reach into _streams directly.
    stream_events = store._streams[f"loan-{app_id}"]
    assert len(stream_events) >= 2, "Need at least 2 events to tamper with"
    # Modify a field in the second event's payload
    stream_events[1]["payload"]["__tampered__"] = True
    stream_events[1]["payload"]["applicant_id"] = "FRAUDSTER"

    # ── Second check — must detect the tampering ──────────────────────────────
    result2 = await run_integrity_check(store, f"loan-{app_id}")

    assert result2["tamper_detected"] is True, (
        "tamper_detected must be True after payload was modified directly in the store"
    )
    assert result2["chain_valid"] is False, (
        "chain_valid must be False when tamper is detected"
    )


# ── Test 3: Gas Town — agent reconstructs after crash ────────────────────────

@pytest.mark.asyncio
async def test_gas_town_agent_reconstructs_after_crash():
    """
    TRP Gas Town test (verbatim requirement):
      Append 5 agent session events, discard the in-memory agent object, call
      reconstruct_agent_context(). Assert the returned context contains enough
      information for the agent to continue.
    """
    store = InMemoryEventStore()
    agent_id = "credit_analysis"
    session_id = "sess-crash-001"
    app_id = "APEX-GT-CRASH-001"

    # Seed 5 agent events (session started, context loaded, 3 nodes executed)
    await store.append(f"agent-{agent_id}-{session_id}", [
        _ev("AgentSessionStarted",
            session_id=session_id, agent_type="credit_analysis",
            agent_id=agent_id, application_id=app_id,
            model_version="v2.3", langgraph_graph_version="1.0",
            context_source="event_store", context_token_count=500,
            started_at="2026-01-15T09:00:00+00:00"),
        _ev("AgentInputValidated",
            session_id=session_id, agent_type="credit_analysis",
            application_id=app_id, inputs_validated=["docs", "history"],
            validation_duration_ms=15,
            validated_at="2026-01-15T09:00:01+00:00"),
        _ev("AgentNodeExecuted",
            session_id=session_id, agent_type="credit_analysis",
            node_name="load_financial_facts", node_sequence=1,
            input_keys=["application_id"], output_keys=["facts"],
            llm_called=False, duration_ms=120,
            executed_at="2026-01-15T09:00:02+00:00"),
        _ev("AgentNodeExecuted",
            session_id=session_id, agent_type="credit_analysis",
            node_name="compute_ratios", node_sequence=2,
            input_keys=["facts"], output_keys=["ratios"],
            llm_called=False, duration_ms=80,
            executed_at="2026-01-15T09:00:03+00:00"),
        _ev("AgentNodeExecuted",
            session_id=session_id, agent_type="credit_analysis",
            node_name="llm_analysis", node_sequence=3,
            input_keys=["ratios", "history"], output_keys=["analysis"],
            llm_called=True, llm_tokens_input=1200, llm_tokens_output=350,
            llm_cost_usd=0.002, duration_ms=3500,
            executed_at="2026-01-15T09:00:07+00:00"),
    ], expected_version=-1)

    # ── Discard the in-memory agent object (simulating a crash) ───────────────
    # (There's no object to discard in this test — we never created one.
    #  The point is that reconstruct_agent_context() works from the store alone.)

    ctx = await reconstruct_agent_context(
        store, agent_id=agent_id, session_id=session_id,
        agent_type="credit_analysis",
    )

    # Assert the context has enough to continue
    assert ctx.session_id == session_id
    assert ctx.application_id == app_id, "Must recover application_id"
    assert ctx.model_version == "v2.3", "Must recover model_version"
    assert len(ctx.nodes_completed) == 3, "Must recover list of completed nodes"
    assert "load_financial_facts" in ctx.nodes_completed
    assert "llm_analysis" in ctx.nodes_completed
    assert ctx.context_text, "context_text must be non-empty"
    assert ctx.last_position == 4, "last_position must be the final stream_position"

    # Session started but not completed → NEEDS_RECONCILIATION
    assert ctx.health_status == NEEDS_RECONCILIATION, (
        f"Session that never completed must be NEEDS_RECONCILIATION, "
        f"got {ctx.health_status!r}"
    )


# ── Test 4: Partial decision flagged as NEEDS_RECONCILIATION ─────────────────

@pytest.mark.asyncio
async def test_gas_town_partial_decision_flagged():
    """
    If the last event is a decision-generating node with no corresponding
    AgentSessionCompleted, reconstruct_agent_context() returns
    health_status = NEEDS_RECONCILIATION.

    This signals to the orchestrator that the session may have written
    partial output that needs reconciliation before proceeding.
    """
    store = InMemoryEventStore()
    agent_id = "decision_orchestrator"
    session_id = "sess-partial-001"
    app_id = "APEX-GT-PARTIAL-001"

    await store.append(f"agent-{agent_id}-{session_id}", [
        _ev("AgentSessionStarted",
            session_id=session_id, agent_type="decision_orchestrator",
            agent_id=agent_id, application_id=app_id,
            model_version="v2.3", langgraph_graph_version="1.0",
            context_source="event_store", context_token_count=800,
            started_at="2026-01-15T10:00:00+00:00"),
        _ev("AgentInputValidated",
            session_id=session_id, agent_type="decision_orchestrator",
            application_id=app_id, inputs_validated=["credit", "fraud", "compliance"],
            validation_duration_ms=5,
            validated_at="2026-01-15T10:00:01+00:00"),
        _ev("AgentNodeExecuted",
            session_id=session_id, agent_type="decision_orchestrator",
            node_name="aggregate_analyses", node_sequence=1,
            input_keys=["credit_result", "fraud_result"], output_keys=["summary"],
            llm_called=False, duration_ms=50,
            executed_at="2026-01-15T10:00:02+00:00"),
        # Last node is "generate_decision" — no AgentOutputWritten or
        # AgentSessionCompleted follows → partial decision in progress
        _ev("AgentNodeExecuted",
            session_id=session_id, agent_type="decision_orchestrator",
            node_name="generate_decision", node_sequence=2,
            input_keys=["summary"], output_keys=["decision"],
            llm_called=True, llm_tokens_input=2000, llm_tokens_output=500,
            duration_ms=4200,
            executed_at="2026-01-15T10:00:07+00:00"),
        # ← NO AgentOutputWritten, NO AgentSessionCompleted (crash here)
    ], expected_version=-1)

    ctx = await reconstruct_agent_context(
        store, agent_id=agent_id, session_id=session_id,
        agent_type="decision_orchestrator",
    )

    assert ctx.health_status == NEEDS_RECONCILIATION, (
        f"Expected NEEDS_RECONCILIATION for partial decision, "
        f"got {ctx.health_status!r}"
    )
    # Must flag the in-progress decision node
    assert any("generate_decision" in w or "decision" in w.lower()
               for w in ctx.pending_work), (
        f"pending_work must mention the in-progress decision node, "
        f"got {ctx.pending_work}"
    )
    assert ctx.context_text, "context_text must be non-empty for recovery"
