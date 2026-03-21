"""
tests/test_aggregates.py
========================
Branch 2 gate tests — domain aggregates.

Each test maps to a named TRP business rule. All tests use InMemoryEventStore
so no database is required.

Run: pytest tests/test_aggregates.py -v
"""
import pytest

from ledger.event_store import InMemoryEventStore
from ledger.domain.aggregates.loan_application import (
    LoanApplicationAggregate,
    ApplicationState,
    DomainError,
)
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate
from ledger.domain.aggregates.audit_ledger import AuditLedgerAggregate


# ── Helpers ──────────────────────────────────────────────────────────────────

def _ev(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


async def _seed_loan(store, app_id: str, events: list[dict]) -> None:
    """Append events to the loan stream."""
    ver = await store.stream_version(f"loan-{app_id}")
    await store.append(f"loan-{app_id}", events, expected_version=ver)


async def _seed_agent(store, agent_id: str, session_id: str, events: list[dict]) -> None:
    """Append events to an agent session stream."""
    stream = f"agent-{agent_id}-{session_id}"
    ver = await store.stream_version(stream)
    await store.append(stream, events, expected_version=ver)


async def _seed_compliance(store, app_id: str, events: list[dict]) -> None:
    """Append events to the compliance stream."""
    ver = await store.stream_version(f"compliance-{app_id}")
    await store.append(f"compliance-{app_id}", events, expected_version=ver)


# ── Rule 1: State machine ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rule1_approved_cannot_revert_to_under_review():
    """
    Rule 1: "Cannot transition from Approved to UnderReview (or any non-terminal state)."
    Once APPROVED, no valid transitions remain.
    """
    store = InMemoryEventStore()
    app_id = "APEX-R1A"

    await _seed_loan(store, app_id, [
        _ev("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=100000,
            loan_purpose="working_capital"),
        _ev("ApplicationApproved", approved_amount_usd=100000, approved_by="system"),
    ])

    agg = await LoanApplicationAggregate.load(store, app_id)
    assert agg.state == ApplicationState.APPROVED

    # APPROVED has no entry in VALID_TRANSITIONS → any target raises DomainError
    with pytest.raises(DomainError):
        agg.assert_valid_transition(ApplicationState.SUBMITTED)

    with pytest.raises(DomainError):
        agg.assert_valid_transition(ApplicationState.PENDING_DECISION)


@pytest.mark.asyncio
async def test_rule1_out_of_order_transition_raises():
    """
    Rule 1: "Any state skipped in the defined machine raises DomainError."
    Cannot jump from SUBMITTED directly to CREDIT_ANALYSIS_REQUESTED.
    """
    store = InMemoryEventStore()
    app_id = "APEX-R1B"

    await _seed_loan(store, app_id, [
        _ev("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=50000,
            loan_purpose="expansion"),
    ])

    agg = await LoanApplicationAggregate.load(store, app_id)
    assert agg.state == ApplicationState.SUBMITTED

    # Jumping from SUBMITTED to CREDIT_ANALYSIS_REQUESTED skips steps
    with pytest.raises(DomainError):
        agg.assert_valid_transition(ApplicationState.CREDIT_ANALYSIS_REQUESTED)

    # Jumping from SUBMITTED to APPROVED skips steps
    with pytest.raises(DomainError):
        agg.assert_valid_transition(ApplicationState.APPROVED)

    # Valid next step is DOCUMENTS_PENDING
    agg.assert_valid_transition(ApplicationState.DOCUMENTS_PENDING)  # must not raise


# ── Rule 2: Gas Town ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rule2_agent_decision_without_context_raises():
    """
    Rule 2 (Gas Town): "An AgentSession MUST have AgentContextLoaded as its first
    event before any decision event."
    """
    store = InMemoryEventStore()
    session_id = "sess-001"
    agent_id = "credit_analysis"

    # Session started but context never loaded
    await _seed_agent(store, agent_id, session_id, [
        _ev("AgentSessionStarted", session_id=session_id, agent_type="credit_analysis",
            agent_id=agent_id, application_id="APEX-R2", model_version="v2.3",
            langgraph_graph_version="1.0", context_source="event_store",
            context_token_count=0),
    ])

    agg = await AgentSessionAggregate.load(store, agent_id, session_id)
    assert agg.context_loaded is False

    with pytest.raises(DomainError):
        agg.assert_context_loaded()


@pytest.mark.asyncio
async def test_rule2_agent_with_context_does_not_raise():
    """Complement to rule2: context loaded → assert passes."""
    store = InMemoryEventStore()
    session_id = "sess-002"
    agent_id = "credit_analysis"

    await _seed_agent(store, agent_id, session_id, [
        _ev("AgentSessionStarted", session_id=session_id, agent_type="credit_analysis",
            agent_id=agent_id, application_id="APEX-R2B", model_version="v2.3",
            langgraph_graph_version="1.0", context_source="event_store",
            context_token_count=100),
        _ev("AgentInputValidated", session_id=session_id, agent_type="credit_analysis",
            application_id="APEX-R2B", inputs_validated=["docs"], validation_duration_ms=10),
    ])

    agg = await AgentSessionAggregate.load(store, agent_id, session_id)
    assert agg.context_loaded is True
    agg.assert_context_loaded()  # must not raise


# ── Rule 3: Credit analysis lock ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rule3_second_credit_analysis_rejected():
    """
    Rule 3: "No further CreditAnalysisCompleted events may be appended once one
    exists unless superseded by HumanReviewOverride."
    """
    store = InMemoryEventStore()
    app_id = "APEX-R3"

    await _seed_loan(store, app_id, [
        _ev("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=200000,
            loan_purpose="equipment_financing"),
        _ev("CreditAnalysisCompleted", session_id="sess-1", application_id=app_id,
            model_version="v2.3", confidence_score=0.75),
    ])

    agg = await LoanApplicationAggregate.load(store, app_id)
    assert agg.credit_analysis_done is True

    with pytest.raises(DomainError):
        agg.assert_credit_analysis_not_locked()


@pytest.mark.asyncio
async def test_rule3_first_credit_analysis_allowed():
    """Complement: first CreditAnalysisCompleted is accepted."""
    store = InMemoryEventStore()
    app_id = "APEX-R3B"

    await _seed_loan(store, app_id, [
        _ev("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=75000,
            loan_purpose="working_capital"),
    ])

    agg = await LoanApplicationAggregate.load(store, app_id)
    assert agg.credit_analysis_done is False
    agg.assert_credit_analysis_not_locked()  # must not raise


# ── Rule 4: Confidence floor ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rule4_low_confidence_forces_refer():
    """
    Rule 4: "confidence_score < 0.6 must set recommendation = REFER regardless
    of orchestrator's analysis."
    """
    store = InMemoryEventStore()
    app_id = "APEX-R4"

    # DecisionGenerated carries confidence 0.45 and recommendation "APPROVE" —
    # the aggregate must override to "REFER"
    await _seed_loan(store, app_id, [
        _ev("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=150000,
            loan_purpose="real_estate"),
        _ev("DecisionGenerated", application_id=app_id,
            orchestrator_session_id="orch-sess-1",
            recommendation="APPROVE",     # orchestrator says APPROVE
            confidence=0.45,              # but confidence is below floor
            contributing_sessions=[],
            model_versions={},
            executive_summary="Borderline case"),
    ])

    agg = await LoanApplicationAggregate.load(store, app_id)
    # Aggregate must have overridden the recommendation to REFER
    assert agg.recommendation == "REFER", (
        f"Expected recommendation='REFER' for confidence=0.45, got {agg.recommendation!r}"
    )
    assert agg.confidence == 0.45


@pytest.mark.asyncio
async def test_rule4_high_confidence_keeps_original():
    """Complement: confidence >= 0.6 preserves the orchestrator's recommendation."""
    store = InMemoryEventStore()
    app_id = "APEX-R4B"

    await _seed_loan(store, app_id, [
        _ev("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=80000,
            loan_purpose="working_capital"),
        _ev("DecisionGenerated", application_id=app_id,
            orchestrator_session_id="orch-sess-2",
            recommendation="APPROVE",
            confidence=0.82,
            contributing_sessions=[],
            model_versions={},
            executive_summary="Strong case"),
    ])

    agg = await LoanApplicationAggregate.load(store, app_id)
    assert agg.recommendation == "APPROVE"
    assert agg.confidence == 0.82


# ── Rule 5: Compliance dependency ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rule5_approval_blocked_without_compliance_clearance():
    """
    Rule 5: "ApplicationApproved cannot be appended unless all ComplianceRulePassed
    events are present."
    """
    store = InMemoryEventStore()
    app_id = "APEX-R5"

    # Compliance check partially done — rule-kyc passed, rule-aml not yet evaluated
    await _seed_compliance(store, app_id, [
        _ev("ComplianceRulePassed", application_id=app_id, session_id="sess-c1",
            rule_id="rule-kyc", rule_name="KYC", rule_version="1.0",
            evidence_hash="abc", evaluation_notes="OK"),
    ])

    agg = await ComplianceRecordAggregate.load(store, app_id)
    required = {"rule-kyc", "rule-aml", "rule-sanctions"}

    with pytest.raises(DomainError):
        agg.assert_all_checks_complete(required)


@pytest.mark.asyncio
async def test_rule5_approval_allowed_with_full_clearance():
    """Complement: all required rules evaluated → assert passes."""
    store = InMemoryEventStore()
    app_id = "APEX-R5B"

    await _seed_compliance(store, app_id, [
        _ev("ComplianceRulePassed", application_id=app_id, session_id="sess-c2",
            rule_id="rule-kyc", rule_name="KYC", rule_version="1.0",
            evidence_hash="abc", evaluation_notes="OK"),
        _ev("ComplianceRulePassed", application_id=app_id, session_id="sess-c2",
            rule_id="rule-aml", rule_name="AML", rule_version="1.0",
            evidence_hash="def", evaluation_notes="OK"),
        _ev("ComplianceRulePassed", application_id=app_id, session_id="sess-c2",
            rule_id="rule-sanctions", rule_name="Sanctions", rule_version="1.0",
            evidence_hash="ghi", evaluation_notes="OK"),
    ])

    agg = await ComplianceRecordAggregate.load(store, app_id)
    required = {"rule-kyc", "rule-aml", "rule-sanctions"}
    agg.assert_all_checks_complete(required)  # must not raise


# ── Rule 6: Causal chain ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rule6_decision_rejects_unrelated_agent_sessions():
    """
    Rule 6: "Orchestrator that references sessions that never processed this
    application must be rejected."
    """
    store = InMemoryEventStore()
    app_id = "APEX-R6"

    # Only sess-known wrote output to this application
    await _seed_loan(store, app_id, [
        _ev("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=300000,
            loan_purpose="acquisition"),
        _ev("AgentOutputWritten", session_id="sess-known",
            agent_type="credit_analysis", application_id=app_id,
            events_written=[], output_summary="credit done"),
    ])

    agg = await LoanApplicationAggregate.load(store, app_id)
    assert "sess-known" in agg.known_sessions

    # Decision cites a session that never touched this application
    with pytest.raises(DomainError):
        agg.assert_contributing_sessions_known(["sess-known", "sess-unrelated"])


@pytest.mark.asyncio
async def test_rule6_decision_accepts_known_sessions():
    """Complement: all cited sessions are known → assert passes."""
    store = InMemoryEventStore()
    app_id = "APEX-R6B"

    await _seed_loan(store, app_id, [
        _ev("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=120000,
            loan_purpose="bridge"),
        _ev("AgentOutputWritten", session_id="sess-credit",
            agent_type="credit_analysis", application_id=app_id,
            events_written=[], output_summary="done"),
        _ev("AgentOutputWritten", session_id="sess-fraud",
            agent_type="fraud_detection", application_id=app_id,
            events_written=[], output_summary="done"),
    ])

    agg = await LoanApplicationAggregate.load(store, app_id)
    agg.assert_contributing_sessions_known(["sess-credit", "sess-fraud"])  # must not raise


# ── Aggregate version tracking ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_aggregate_version_matches_stream_version_after_replay():
    """
    After replaying N events, aggregate.version must equal stream_version().
    Command handlers pass aggregate.version as expected_version to store.append().
    If the two values diverge, every subsequent append fails with OCC.
    """
    store = InMemoryEventStore()
    app_id = "APEX-VER-001"

    await _seed_loan(store, app_id, [
        _ev("ApplicationSubmitted", applicant_id="a1", requested_amount_usd=100000,
            loan_purpose="working_capital"),
        _ev("DocumentUploadRequested"),
        _ev("DocumentUploaded"),
    ])

    agg = await LoanApplicationAggregate.load(store, app_id)
    stream_ver = await store.stream_version(f"loan-{app_id}")

    assert agg.version == stream_ver, (
        f"aggregate.version ({agg.version}) != stream_version ({stream_ver}). "
        "Handlers will pass the wrong expected_version on the next append."
    )


# ── Agent session model version guard ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_agent_session_model_version_guard_raises_on_mismatch():
    """
    Rule 3: assert_model_version_current() raises DomainError when the model
    version in a subsequent command differs from what the session declared at
    AgentSessionStarted. Prevents silent model drift within a single session.
    """
    store = InMemoryEventStore()
    session_id = "sess-mvg-001"
    agent_id = "credit_analysis"

    await _seed_agent(store, agent_id, session_id, [
        _ev("AgentSessionStarted", session_id=session_id, agent_type=agent_id,
            agent_id=agent_id, application_id="APEX-MVG",
            model_version="v2.3",
            langgraph_graph_version="1.0", context_source="event_store",
            context_token_count=0),
        _ev("AgentInputValidated", session_id=session_id, agent_type=agent_id,
            application_id="APEX-MVG", inputs_validated=[], validation_duration_ms=5),
    ])

    agg = await AgentSessionAggregate.load(store, agent_id, session_id)
    assert agg.model_version == "v2.3"

    # Same version — must not raise
    agg.assert_model_version_current("v2.3")

    # Different version — must raise DomainError
    with pytest.raises(DomainError, match="version"):
        agg.assert_model_version_current("v3.0")


# ── Aggregate load integration ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_audit_ledger_aggregate_loads_and_tracks_hash():
    """AuditLedgerAggregate tracks the last integrity hash and events verified."""
    store = InMemoryEventStore()
    entity_id = "loan-APEX-AUDIT"

    await store.append(
        f"audit-loan-{entity_id}", [
            _ev("AuditIntegrityCheckRun",
                entity_type="loan", entity_id=entity_id,
                events_verified_count=12,
                integrity_hash="sha256-abc",
                previous_hash=None,
                chain_valid=True,
                tamper_detected=False),
        ],
        expected_version=-1,
    )

    agg = await AuditLedgerAggregate.load(store, "loan", entity_id)
    assert agg.last_integrity_hash == "sha256-abc"
    assert agg.events_verified == 12
    assert agg.last_chain_valid is True
    assert agg.last_tamper_detected is False
