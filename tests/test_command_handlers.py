"""
tests/test_command_handlers.py
================================
Branch 3 gate tests — command handlers.

Each test maps to a named TRP system-level requirement. All tests use
InMemoryEventStore — no database required.

Run: pytest tests/test_command_handlers.py -v
"""
import asyncio
from decimal import Decimal

import pytest

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError
from ledger.domain.aggregates.loan_application import (
    LoanApplicationAggregate,
    ApplicationState,
    DomainError,
)
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


# ── Shared helpers ────────────────────────────────────────────────────────────

APP_ID = "APEX-TEST-001"
SESSION_ID = "sess-credit-001"
AGENT_ID = "credit_analysis"
AGENT_TYPE = "credit_analysis"
FRAUD_SESSION_ID = "sess-fraud-001"
FRAUD_AGENT_ID = "fraud_detection"
FRAUD_AGENT_TYPE = "fraud_detection"
MODEL_VERSION = "v2.3"
REQUIRED_RULES = {"rule-kyc", "rule-aml", "rule-sanctions"}


async def _run_lifecycle(store, app_id: str = APP_ID, final_decision: str = "APPROVED"):
    """
    Drive a full loan lifecycle through command handlers:
      submit → agent session → credit → fraud → 3× compliance → decision → human review
    Returns the final LoanApplicationAggregate.
    """
    # 1. Submit
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="applicant-42",
            requested_amount_usd=Decimal("250000"),
            loan_purpose="working_capital",
            loan_term_months=24,
            application_reference=f"REF-{app_id}",
        ),
        store,
    )

    # 2. Start credit agent session (Gas Town: AgentInputValidated emitted by handler)
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id=SESSION_ID,
            agent_type=AGENT_TYPE,
            agent_id=AGENT_ID,
            application_id=app_id,
            model_version=MODEL_VERSION,
        ),
        store,
    )

    # 3. Credit analysis
    await handle_credit_analysis_completed(
        CreditAnalysisCompletedCommand(
            application_id=app_id,
            session_id=SESSION_ID,
            agent_id=AGENT_ID,
            agent_type=AGENT_TYPE,
            model_version=MODEL_VERSION,
            model_deployment_id="deploy-001",
            decision={
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": "200000",
                "confidence": 0.78,
                "rationale": "Solid cash flow",
            },
        ),
        store,
    )

    # 4. Start fraud agent session
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id=FRAUD_SESSION_ID,
            agent_type=FRAUD_AGENT_TYPE,
            agent_id=FRAUD_AGENT_ID,
            application_id=app_id,
            model_version="v1.5",
        ),
        store,
    )

    # 5. Fraud screening
    await handle_fraud_screening_completed(
        FraudScreeningCompletedCommand(
            application_id=app_id,
            session_id=FRAUD_SESSION_ID,
            agent_id=FRAUD_AGENT_ID,
            agent_type=FRAUD_AGENT_TYPE,
            fraud_score=0.12,
            risk_level="LOW",
            recommendation="APPROVE",
            screening_model_version="v1.5",
        ),
        store,
    )

    # 6. Three compliance checks (all pass)
    compliance_session = "sess-compliance-001"
    for rule_id, rule_name in [
        ("rule-kyc", "Know Your Customer"),
        ("rule-aml", "Anti-Money Laundering"),
        ("rule-sanctions", "Sanctions Screening"),
    ]:
        await handle_compliance_check(
            ComplianceCheckCommand(
                application_id=app_id,
                session_id=compliance_session,
                rule_id=rule_id,
                rule_name=rule_name,
                rule_version="2024.1",
                passed=True,
            ),
            store,
        )

    # 7. Generate decision (REFER → goes to human review)
    await handle_generate_decision(
        GenerateDecisionCommand(
            application_id=app_id,
            orchestrator_session_id="orch-sess-001",
            recommendation="REFER",
            confidence=0.75,
            executive_summary="Referred for human review.",
            required_compliance_rules=REQUIRED_RULES,
        ),
        store,
    )

    # 8. Human review
    await handle_human_review_completed(
        HumanReviewCompletedCommand(
            application_id=app_id,
            reviewer_id="reviewer-jane",
            override=False,
            original_recommendation="REFER",
            final_decision=final_decision,
        ),
        store,
    )

    return await LoanApplicationAggregate.load(store, app_id)


# ── Test 1: Full happy path — approved ───────────────────────────────────────

@pytest.mark.asyncio
async def test_complete_loan_lifecycle_approved():
    """
    Full happy path: submit → agent session → credit → fraud → compliance →
    decision → human review → FinalApproved.
    Proves the command chain is complete and produces the right terminal state.
    """
    store = InMemoryEventStore()
    agg = await _run_lifecycle(store, final_decision="APPROVED")
    assert agg.state == ApplicationState.APPROVED, (
        f"Expected APPROVED, got {agg.state}"
    )
    assert agg.credit_analysis_done is True
    assert agg.fraud_done is True


# ── Test 2: Full lifecycle — declined ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_complete_loan_lifecycle_declined():
    """
    Same flow terminating in FinalDeclined.
    Proves both terminal states are reachable through the command chain.
    """
    store = InMemoryEventStore()
    agg = await _run_lifecycle(store, app_id="APEX-TEST-002", final_decision="DECLINED")
    assert agg.state == ApplicationState.DECLINED, (
        f"Expected DECLINED, got {agg.state}"
    )


# ── Test 3: Double-Decision (TRP Phase 1 OCC test) ───────────────────────────

@pytest.mark.asyncio
async def test_double_decision_exactly_one_succeeds():
    """
    The Double-Decision Test (TRP Phase 1):
      Two concurrent asyncio tasks append CreditAnalysisCompleted at the same
      expected_version.
    Asserts:
      (a) total events in loan stream = N+1, not N+2
      (b) winner's event has the expected stream position
      (c) loser receives OptimisticConcurrencyError, not silent failure
    """
    store = InMemoryEventStore()
    app_id = "APEX-OCC-001"

    # Set up: submit application and open agent session
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="occ-tester",
            requested_amount_usd=Decimal("100000"),
            loan_purpose="expansion",
            loan_term_months=12,
        ),
        store,
    )
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id="occ-sess-1",
            agent_type=AGENT_TYPE,
            agent_id=AGENT_ID,
            application_id=app_id,
            model_version=MODEL_VERSION,
        ),
        store,
    )

    # Loan stream is now at version=0 (ApplicationSubmitted).
    loan_stream = f"loan-{app_id}"
    expected_version = await store.stream_version(loan_stream)  # 0

    credit_event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 2,
        "payload": {
            "application_id": app_id,
            "session_id": "occ-sess-1",
            "decision": {"risk_tier": "LOW", "recommended_limit_usd": "100000",
                         "confidence": 0.9, "rationale": "Strong"},
            "model_version": MODEL_VERSION,
            "model_deployment_id": "deploy-occ",
            "input_data_hash": "n/a",
            "analysis_duration_ms": 0,
            "regulatory_basis": [],
            "completed_at": "2026-01-01T00:00:00+00:00",
        },
    }

    # Two concurrent tasks both try to append at the SAME expected_version.
    # This is the TRP Double-Decision test: OCC at the store level.
    results = []
    errors = []

    async def attempt():
        try:
            positions = await store.append(
                loan_stream, [credit_event], expected_version=expected_version
            )
            results.append(positions)
        except OptimisticConcurrencyError as e:
            errors.append(e)

    await asyncio.gather(attempt(), attempt())

    # (a) Exactly one success, one OCC error
    assert len(results) == 1, f"Expected 1 success, got {len(results)}"
    assert len(errors) == 1, f"Expected 1 OCC error, got {len(errors)}"

    # (b) Winner's event is at the expected stream position
    winner_positions = results[0]
    assert winner_positions == [1], f"Winner should be at position 1, got {winner_positions}"

    # (c) Loan stream has exactly 2 events (ApplicationSubmitted + one CreditAnalysisCompleted)
    events = await store.load_stream(loan_stream)
    assert len(events) == 2, f"Expected 2 events, got {len(events)}"

    # OCC loser has a typed, structured error — not silent
    occ_err = errors[0]
    assert occ_err.stream_id == loan_stream
    assert occ_err.expected == expected_version
    assert occ_err.actual == expected_version + 1


# ── Test 4: Gas Town — no session → DomainError ──────────────────────────────

@pytest.mark.asyncio
async def test_agent_cannot_act_without_session():
    """
    Gas Town: calling handle_credit_analysis_completed without a prior
    handle_start_agent_session raises DomainError.
    """
    store = InMemoryEventStore()
    app_id = "APEX-GT-001"

    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="gt-tester",
            requested_amount_usd=Decimal("50000"),
            loan_purpose="bridge",
            loan_term_months=6,
        ),
        store,
    )

    # Attempt credit analysis without ever calling handle_start_agent_session
    with pytest.raises(DomainError, match="context"):
        await handle_credit_analysis_completed(
            CreditAnalysisCompletedCommand(
                application_id=app_id,
                session_id="nonexistent-sess",
                agent_id=AGENT_ID,
                agent_type=AGENT_TYPE,
                model_version=MODEL_VERSION,
                model_deployment_id="deploy-gt",
                decision={"risk_tier": "HIGH", "recommended_limit_usd": "0",
                           "confidence": 0.5, "rationale": "No context"},
            ),
            store,
        )


# ── Test 5: Override requires reason ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_human_override_requires_reason():
    """
    "If override=True, override_reason is required."
    Omitting it raises DomainError.
    """
    store = InMemoryEventStore()
    app_id = "APEX-OVR-001"

    # Seed a loan stream so the handler can load the aggregate
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="ovr-tester",
            requested_amount_usd=Decimal("75000"),
            loan_purpose="refinancing",
            loan_term_months=36,
        ),
        store,
    )

    with pytest.raises(DomainError, match="override_reason"):
        await handle_human_review_completed(
            HumanReviewCompletedCommand(
                application_id=app_id,
                reviewer_id="reviewer-bob",
                override=True,
                original_recommendation="DECLINE",
                final_decision="APPROVED",
                override_reason=None,   # missing — must raise
            ),
            store,
        )

    # Non-override without reason is allowed
    await handle_human_review_completed(
        HumanReviewCompletedCommand(
            application_id=app_id,
            reviewer_id="reviewer-bob",
            override=False,
            original_recommendation="REFER",
            final_decision="APPROVED",
        ),
        store,
    )


# ── Test 6: Decision without all analyses ─────────────────────────────────────

@pytest.mark.asyncio
async def test_decision_without_all_analyses_raises():
    """
    "All required analyses must be present" before generate_decision succeeds.
    Skipping credit and/or fraud screening raises DomainError.
    """
    store = InMemoryEventStore()
    app_id = "APEX-NOANALYSIS-001"

    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="skip-tester",
            requested_amount_usd=Decimal("120000"),
            loan_purpose="equipment_financing",
            loan_term_months=48,
        ),
        store,
    )

    # No credit or fraud analysis done — generate_decision must raise
    with pytest.raises(DomainError, match="credit"):
        await handle_generate_decision(
            GenerateDecisionCommand(
                application_id=app_id,
                orchestrator_session_id="orch-incomplete",
                recommendation="APPROVE",
                confidence=0.8,
                executive_summary="Skipped analyses",
            ),
            store,
        )

    # Now do credit analysis but skip fraud — still must raise
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id="sess-partial",
            agent_type=AGENT_TYPE,
            agent_id=AGENT_ID,
            application_id=app_id,
            model_version=MODEL_VERSION,
        ),
        store,
    )
    await handle_credit_analysis_completed(
        CreditAnalysisCompletedCommand(
            application_id=app_id,
            session_id="sess-partial",
            agent_id=AGENT_ID,
            agent_type=AGENT_TYPE,
            model_version=MODEL_VERSION,
            model_deployment_id="deploy-partial",
            decision={"risk_tier": "LOW", "recommended_limit_usd": "120000",
                       "confidence": 0.9, "rationale": "Fine"},
        ),
        store,
    )

    with pytest.raises(DomainError, match="fraud"):
        await handle_generate_decision(
            GenerateDecisionCommand(
                application_id=app_id,
                orchestrator_session_id="orch-no-fraud",
                recommendation="APPROVE",
                confidence=0.8,
                executive_summary="Fraud missing",
            ),
            store,
        )


# ── Test 7: correlation_id and causation_id threaded through handler ──────────

@pytest.mark.asyncio
async def test_correlation_id_threaded_through_command_handler():
    """
    correlation_id and causation_id on a command must appear in the stored
    event's metadata. Proves the handler passes causal metadata through to
    store.append() rather than discarding it — required for distributed
    tracing and causal chain audits.
    """
    store = InMemoryEventStore()
    corr_id = "trace-12345"
    cause_id = "cmd-67890"

    await handle_submit_application(
        SubmitApplicationCommand(
            application_id="APEX-CORR-001",
            applicant_id="corr-tester",
            requested_amount_usd=Decimal("100000"),
            loan_purpose="working_capital",
            loan_term_months=12,
            correlation_id=corr_id,
            causation_id=cause_id,
        ),
        store,
    )

    events = await store.load_stream("loan-APEX-CORR-001")
    assert len(events) == 1
    meta = events[0]["metadata"]
    assert meta.get("correlation_id") == corr_id, (
        f"correlation_id not threaded through handler to event metadata: {meta}"
    )
    assert meta.get("causation_id") == cause_id, (
        f"causation_id not threaded through handler to event metadata: {meta}"
    )
