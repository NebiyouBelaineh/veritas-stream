"""
tests/test_projections.py
==========================
Branch 4 gate tests — projection daemon and read-model projections.

Each test maps to a named TRP requirement or SLO. All tests use
InMemoryEventStore — no database required.

Run: pytest tests/test_projections.py -v
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from ledger.event_store import InMemoryEventStore
from ledger.domain.aggregates.loan_application import ApplicationState
from ledger.projections.daemon import ProjectionDaemon
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.commands.handlers import (
    SubmitApplicationCommand,
    StartAgentSessionCommand,
    CreditAnalysisCompletedCommand,
    FraudScreeningCompletedCommand,
    ComplianceCheckCommand,
    GenerateDecisionCommand,
    HumanReviewCompletedCommand,
    handle_submit_application,
    handle_start_agent_session,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_compliance_check,
    handle_generate_decision,
    handle_human_review_completed,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ev(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


async def _run_full_lifecycle(
    store,
    app_id: str = "APEX-PROJ-001",
    session_id: str = "sess-c001",
    agent_id: str = "credit_analysis",
    model_version: str = "v2.3",
    final_decision: str = "APPROVED",
    fraud_session_id: str = "sess-f001",
) -> None:
    """Seed a complete lifecycle into the store via command handlers."""
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id, applicant_id="app-42",
            requested_amount_usd=Decimal("200000"), loan_purpose="working_capital",
            loan_term_months=24,
        ), store,
    )
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id=session_id, agent_type="credit_analysis",
            agent_id=agent_id, application_id=app_id, model_version=model_version,
        ), store,
    )
    await handle_credit_analysis_completed(
        CreditAnalysisCompletedCommand(
            application_id=app_id, session_id=session_id,
            agent_id=agent_id, agent_type="credit_analysis",
            model_version=model_version, model_deployment_id="dep-001",
            decision={"risk_tier": "MEDIUM", "recommended_limit_usd": "180000",
                      "confidence": 0.82, "rationale": "Good cash flow"},
        ), store,
    )
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id=fraud_session_id, agent_type="fraud_detection",
            agent_id="fraud_detection", application_id=app_id, model_version="v1.5",
        ), store,
    )
    await handle_fraud_screening_completed(
        FraudScreeningCompletedCommand(
            application_id=app_id, session_id=fraud_session_id,
            agent_id="fraud_detection", agent_type="fraud_detection",
            fraud_score=0.08, risk_level="LOW", recommendation="APPROVE",
            screening_model_version="v1.5",
        ), store,
    )
    for rule_id, rule_name in [
        ("rule-kyc", "KYC"), ("rule-aml", "AML"), ("rule-sanctions", "Sanctions"),
    ]:
        await handle_compliance_check(
            ComplianceCheckCommand(
                application_id=app_id, session_id="sess-compliance",
                rule_id=rule_id, rule_name=rule_name, rule_version="2024.1",
                passed=True,
            ), store,
        )
    await handle_generate_decision(
        GenerateDecisionCommand(
            application_id=app_id, orchestrator_session_id="orch-sess-001",
            recommendation="REFER", confidence=0.80,
            executive_summary="Referred for review.",
            required_compliance_rules={"rule-kyc", "rule-aml", "rule-sanctions"},
        ), store,
    )
    await handle_human_review_completed(
        HumanReviewCompletedCommand(
            application_id=app_id, reviewer_id="reviewer-jane",
            override=False, original_recommendation="REFER",
            final_decision=final_decision,
        ), store,
    )


def _make_daemon(store) -> tuple[ProjectionDaemon, ApplicationSummaryProjection,
                                  AgentPerformanceLedgerProjection,
                                  ComplianceAuditViewProjection]:
    summary = ApplicationSummaryProjection()
    performance = AgentPerformanceLedgerProjection()
    compliance = ComplianceAuditViewProjection()
    daemon = ProjectionDaemon(store, [summary, performance, compliance])
    return daemon, summary, performance, compliance


# ── Test 1: ApplicationSummary reflects final state ───────────────────────────

@pytest.mark.asyncio
async def test_application_summary_reflects_final_state():
    """
    After a full lifecycle of events, ApplicationSummary shows the correct
    terminal state, decision, and reviewer — proving the projection is a
    complete read model.
    """
    store = InMemoryEventStore()
    await _run_full_lifecycle(store, final_decision="APPROVED")

    daemon, summary, _, _ = _make_daemon(store)
    await daemon.run_once()

    row = summary.get("APEX-PROJ-001")
    assert row is not None, "ApplicationSummary row must exist after full lifecycle"
    assert row.state == "APPROVED", f"Expected APPROVED, got {row.state!r}"
    assert row.risk_tier == "MEDIUM"
    assert row.fraud_score == pytest.approx(0.08)
    assert row.human_reviewer_id == "reviewer-jane"
    assert row.decision == "REFER"     # orchestrator recommendation
    assert row.final_decision_at is not None


# ── Test 2: ComplianceAudit temporal query ────────────────────────────────────

@pytest.mark.asyncio
async def test_compliance_audit_temporal_query():
    """
    get_compliance_at(application_id, timestamp) returns the compliance state
    *as it existed at that moment*, not the current state — proves regulatory
    time-travel works.
    """
    store = InMemoryEventStore()
    app_id = "APEX-TEMPORAL-001"

    t0 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 1, 10, 5, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 10, 10, 0, tzinfo=timezone.utc)
    t3 = datetime(2026, 1, 1, 10, 15, 0, tzinfo=timezone.utc)

    # Three compliance events at t1, t2, t3
    await store.append(f"compliance-{app_id}", [
        _ev("ComplianceRulePassed", application_id=app_id, session_id="s",
            rule_id="rule-kyc", rule_name="KYC", rule_version="1.0",
            evidence_hash="x", evaluation_notes="ok",
            evaluated_at=t1.isoformat()),
        _ev("ComplianceRulePassed", application_id=app_id, session_id="s",
            rule_id="rule-aml", rule_name="AML", rule_version="1.0",
            evidence_hash="y", evaluation_notes="ok",
            evaluated_at=t2.isoformat()),
        _ev("ComplianceRulePassed", application_id=app_id, session_id="s",
            rule_id="rule-sanctions", rule_name="Sanctions", rule_version="1.0",
            evidence_hash="z", evaluation_notes="ok",
            evaluated_at=t3.isoformat()),
    ], expected_version=-1)

    _, _, _, compliance = _make_daemon(store)
    daemon = ProjectionDaemon(store, [compliance])
    await daemon.run_once()

    # Query between t1 and t2: only rule-kyc should be visible
    snapshot = compliance.get_compliance_at(app_id, t1 + timedelta(seconds=1))
    assert "rule-kyc" in snapshot.rules_passed
    assert "rule-aml" not in snapshot.rules_passed
    assert "rule-sanctions" not in snapshot.rules_passed

    # Query after t3: all three rules visible
    snapshot_full = compliance.get_compliance_at(app_id, t3 + timedelta(seconds=1))
    assert snapshot_full.rules_passed == {"rule-kyc", "rule-aml", "rule-sanctions"}


# ── Test 3: Temporal query before any checks ──────────────────────────────────

@pytest.mark.asyncio
async def test_compliance_audit_temporal_query_before_any_checks():
    """
    Querying before any compliance events returns an empty record — proves the
    temporal query doesn't bleed future state backwards.
    """
    store = InMemoryEventStore()
    app_id = "APEX-TEMPORAL-002"

    t_event = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    t_before = t_event - timedelta(hours=1)

    await store.append(f"compliance-{app_id}", [
        _ev("ComplianceRulePassed", application_id=app_id, session_id="s",
            rule_id="rule-kyc", rule_name="KYC", rule_version="1.0",
            evidence_hash="x", evaluation_notes="ok",
            evaluated_at=t_event.isoformat()),
    ], expected_version=-1)

    compliance = ComplianceAuditViewProjection()
    daemon = ProjectionDaemon(store, [compliance])
    await daemon.run_once()

    # Query one hour before the event — must return empty
    snapshot = compliance.get_compliance_at(app_id, t_before)
    assert len(snapshot.rules_passed) == 0, "No rules should be visible before the event"
    assert len(snapshot.rules_failed) == 0
    assert snapshot.verdict is None


# ── Test 4: AgentPerformance tracks model version separately ─────────────────

@pytest.mark.asyncio
async def test_agent_performance_tracks_model_version_separately():
    """
    Two agents with different model_version values produce separate rows in
    AgentPerformanceLedger — enables "has v2.3 behaved differently than v2.2?"
    """
    store = InMemoryEventStore()

    # Agent A on model v2.2, Agent B on model v2.3
    await store.append("agent-credit_analysis-sess-v22", [
        _ev("AgentSessionStarted",
            session_id="sess-v22", agent_type="credit_analysis",
            agent_id="agent-A", application_id="APP-01",
            model_version="v2.2", langgraph_graph_version="1.0",
            context_source="store", context_token_count=100,
            started_at="2026-01-01T00:00:00+00:00"),
        _ev("CreditAnalysisCompleted",
            application_id="APP-01", session_id="sess-v22",
            decision={"risk_tier": "LOW", "recommended_limit_usd": "100000",
                      "confidence": 0.90, "rationale": "Good"},
            model_version="v2.2", model_deployment_id="d", input_data_hash="h",
            analysis_duration_ms=500, regulatory_basis=[],
            completed_at="2026-01-01T00:01:00+00:00"),
    ], expected_version=-1)

    await store.append("agent-credit_analysis-sess-v23", [
        _ev("AgentSessionStarted",
            session_id="sess-v23", agent_type="credit_analysis",
            agent_id="agent-B", application_id="APP-02",
            model_version="v2.3", langgraph_graph_version="1.0",
            context_source="store", context_token_count=100,
            started_at="2026-01-01T00:00:00+00:00"),
        _ev("CreditAnalysisCompleted",
            application_id="APP-02", session_id="sess-v23",
            decision={"risk_tier": "MEDIUM", "recommended_limit_usd": "80000",
                      "confidence": 0.72, "rationale": "Moderate"},
            model_version="v2.3", model_deployment_id="d", input_data_hash="h",
            analysis_duration_ms=480, regulatory_basis=[],
            completed_at="2026-01-01T00:01:00+00:00"),
    ], expected_version=-1)

    performance = AgentPerformanceLedgerProjection()
    daemon = ProjectionDaemon(store, [performance])
    await daemon.run_once()

    row_v22 = performance.get("agent-A", "v2.2")
    row_v23 = performance.get("agent-B", "v2.3")

    assert row_v22 is not None, "v2.2 row must exist"
    assert row_v23 is not None, "v2.3 row must exist"
    assert row_v22.analyses_completed == 1
    assert row_v23.analyses_completed == 1
    assert row_v22.avg_confidence_score == pytest.approx(0.90)
    assert row_v23.avg_confidence_score == pytest.approx(0.72)

    # They must be separate rows (different keys)
    assert (row_v22.agent_id, row_v22.model_version) != (row_v23.agent_id, row_v23.model_version)


# ── Test 5: ApplicationSummary SLO < 500ms ────────────────────────────────────

@pytest.mark.asyncio
async def test_projection_lag_slo_application_summary():
    """
    Under 50 concurrent event appends, ApplicationSummary lag stays under
    500ms — the TRP-specified SLO.
    """
    store = InMemoryEventStore()

    # Append 50 ApplicationSubmitted events concurrently
    async def seed_app(i: int) -> None:
        app_id = f"APEX-SLO-{i:03d}"
        await store.append(f"loan-{app_id}", [
            _ev("ApplicationSubmitted",
                application_id=app_id, applicant_id=f"app-{i}",
                requested_amount_usd="100000", loan_purpose="working_capital",
                loan_term_months=12, submission_channel="online",
                contact_email="", contact_name="",
                application_reference=app_id,
                submitted_at="2026-01-01T00:00:00+00:00"),
        ], expected_version=-1)

    await asyncio.gather(*[seed_app(i) for i in range(50)])

    summary = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store, [summary])

    start = time.perf_counter()
    await daemon.run_once()
    elapsed_ms = (time.perf_counter() - start) * 1000

    lag_ms = await daemon.get_lag("application_summary")

    assert elapsed_ms < 500, (
        f"run_once() took {elapsed_ms:.1f}ms — must be < 500ms SLO"
    )
    assert lag_ms < 500, (
        f"ApplicationSummary lag is {lag_ms:.1f}ms — must be < 500ms SLO"
    )
    assert len(summary.all()) == 50


# ── Test 6: ComplianceAuditView SLO < 2000ms ─────────────────────────────────

@pytest.mark.asyncio
async def test_projection_lag_slo_compliance_audit():
    """
    Under 50 concurrent compliance event appends, ComplianceAuditView lag
    stays under 2000ms — the TRP-specified SLO.
    """
    store = InMemoryEventStore()

    async def seed_compliance(i: int) -> None:
        app_id = f"APEX-CSLO-{i:03d}"
        await store.append(f"compliance-{app_id}", [
            _ev("ComplianceRulePassed",
                application_id=app_id, session_id=f"sess-{i}",
                rule_id="rule-kyc", rule_name="KYC", rule_version="1.0",
                evidence_hash="h", evaluation_notes="ok",
                evaluated_at="2026-01-01T00:00:00+00:00"),
        ], expected_version=-1)

    await asyncio.gather(*[seed_compliance(i) for i in range(50)])

    compliance = ComplianceAuditViewProjection()
    daemon = ProjectionDaemon(store, [compliance])

    start = time.perf_counter()
    await daemon.run_once()
    elapsed_ms = (time.perf_counter() - start) * 1000

    lag_ms = await daemon.get_lag("compliance_audit_view")

    assert elapsed_ms < 2000, (
        f"run_once() took {elapsed_ms:.1f}ms — must be < 2000ms SLO"
    )
    assert lag_ms < 2000, (
        f"ComplianceAuditView lag is {lag_ms:.1f}ms — must be < 2000ms SLO"
    )


# ── Test 7: Rebuild from scratch produces identical result ────────────────────

@pytest.mark.asyncio
async def test_rebuild_from_scratch_produces_identical_result():
    """
    Truncate + replay produces identical projection state — proves projections
    are deterministic and recoverable.
    """
    store = InMemoryEventStore()
    await _run_full_lifecycle(store, app_id="APEX-REBUILD-001")

    summary = ApplicationSummaryProjection()
    daemon = ProjectionDaemon(store, [summary])
    await daemon.run_once()

    # Capture state after first build
    row_before = summary.get("APEX-REBUILD-001")
    assert row_before is not None

    state_before = row_before.state
    risk_before = row_before.risk_tier
    fraud_before = row_before.fraud_score
    reviewer_before = row_before.human_reviewer_id

    # Rebuild from scratch
    await daemon.rebuild_from_scratch("application_summary")

    row_after = summary.get("APEX-REBUILD-001")
    assert row_after is not None, "Row must still exist after rebuild"

    assert row_after.state == state_before, "State must be identical after rebuild"
    assert row_after.risk_tier == risk_before, "risk_tier must be identical after rebuild"
    assert row_after.fraud_score == fraud_before, "fraud_score must be identical after rebuild"
    assert row_after.human_reviewer_id == reviewer_before, "reviewer must be identical after rebuild"


# ── Test 8: Daemon continues after bad event ──────────────────────────────────

@pytest.mark.asyncio
async def test_daemon_continues_after_bad_event():
    """
    Injecting a malformed event does not crash the daemon; subsequent valid
    events are still processed — proves the fault tolerance requirement.
    """
    store = InMemoryEventStore()
    app_id = "APEX-FAULT-001"

    # First: a valid ApplicationSubmitted
    await store.append(f"loan-{app_id}", [
        _ev("ApplicationSubmitted",
            application_id=app_id, applicant_id="fault-tester",
            requested_amount_usd="50000", loan_purpose="bridge",
            loan_term_months=6, submission_channel="online",
            contact_email="", contact_name="", application_reference=app_id,
            submitted_at="2026-01-01T00:00:00+00:00"),
    ], expected_version=-1)

    # Inject a "ComplianceRulePassed" on the compliance stream but with a
    # broken application_id structure that the projection can handle
    # by raising an exception. We'll monkey-patch handle to fail once.
    app_id2 = "APEX-FAULT-002"
    await store.append(f"compliance-{app_id2}", [
        _ev("ComplianceRulePassed",
            # Missing required fields to trigger exception inside projection
            # rule_id is missing; we'll get None, but that won't raise.
            # Instead we append an event with no event_type to cause a KeyError.
            application_id=app_id2),
    ], expected_version=-1)

    # Second valid application after the bad event
    app_id3 = "APEX-FAULT-003"
    await store.append(f"loan-{app_id3}", [
        _ev("ApplicationSubmitted",
            application_id=app_id3, applicant_id="ok-tester",
            requested_amount_usd="75000", loan_purpose="expansion",
            loan_term_months=12, submission_channel="online",
            contact_email="", contact_name="", application_reference=app_id3,
            submitted_at="2026-01-01T01:00:00+00:00"),
    ], expected_version=-1)

    # Use a projection that raises on the compliance event
    class FailOnComplianceProjection(ApplicationSummaryProjection):
        name = "application_summary"
        async def handle(self, event: dict) -> None:
            if event["event_type"] == "ComplianceRulePassed":
                raise RuntimeError("Simulated bad event handler failure")
            await super().handle(event)

    summary = FailOnComplianceProjection()
    daemon = ProjectionDaemon(store, [summary], max_retry=0)

    # Must not raise
    await daemon.run_once()

    # Valid events before and after the bad event must be processed
    assert summary.get(app_id) is not None, "APEX-FAULT-001 must be in summary"
    assert summary.get(app_id3) is not None, "APEX-FAULT-003 must be in summary after bad event"
