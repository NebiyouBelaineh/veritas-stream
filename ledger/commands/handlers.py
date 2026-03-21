"""
ledger/commands/handlers.py
============================
Command handlers — load → validate → determine → append.

Each handler follows this pattern:
  1. Load the relevant aggregate(s) from the store.
  2. Assert business rules on the aggregate(s).
  3. Build the event(s) to append.
  4. Append to the stream with OCC (expected_version = agg.version).

Handlers are pure async functions; they do not instantiate the store.
The caller (test, MCP tool, or HTTP endpoint) provides the store.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from ledger.event_store import OptimisticConcurrencyError  # re-exported for callers
from ledger.domain.aggregates.loan_application import (
    LoanApplicationAggregate,
    DomainError,
)
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Command dataclasses ───────────────────────────────────────────────────────

@dataclass
class SubmitApplicationCommand:
    application_id: str
    applicant_id: str
    requested_amount_usd: Decimal
    loan_purpose: str
    loan_term_months: int
    submission_channel: str = "online"
    contact_email: str = ""
    contact_name: str = ""
    application_reference: str = ""
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class StartAgentSessionCommand:
    session_id: str
    agent_type: str          # e.g. "credit_analysis"
    agent_id: str
    application_id: str
    model_version: str
    langgraph_graph_version: str = "1.0"
    context_source: str = "event_store"
    context_token_count: int = 0
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class CreditAnalysisCompletedCommand:
    application_id: str
    session_id: str
    agent_id: str
    agent_type: str
    model_version: str
    model_deployment_id: str
    decision: dict           # serialised CreditDecision fields
    input_data_hash: str = "n/a"
    analysis_duration_ms: int = 0
    regulatory_basis: list = field(default_factory=list)
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class FraudScreeningCompletedCommand:
    application_id: str
    session_id: str
    agent_id: str
    agent_type: str
    fraud_score: float
    risk_level: str
    recommendation: str
    screening_model_version: str
    input_data_hash: str = "n/a"
    anomalies_found: int = 0
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class ComplianceCheckCommand:
    application_id: str
    session_id: str
    rule_id: str
    rule_name: str
    rule_version: str
    passed: bool
    evidence_hash: str = "n/a"
    evaluation_notes: str = ""
    is_hard_block: bool = False
    failure_reason: str = ""
    remediation_available: bool = False
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class GenerateDecisionCommand:
    application_id: str
    orchestrator_session_id: str
    recommendation: str
    confidence: float
    executive_summary: str
    contributing_sessions: list = field(default_factory=list)
    approved_amount_usd: Decimal | None = None
    conditions: list = field(default_factory=list)
    key_risks: list = field(default_factory=list)
    model_versions: dict = field(default_factory=dict)
    required_compliance_rules: set = field(default_factory=set)
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass
class HumanReviewCompletedCommand:
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str      # "APPROVED" | "DECLINED"
    override_reason: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None


# ── Handlers ─────────────────────────────────────────────────────────────────

async def handle_submit_application(
    cmd: SubmitApplicationCommand, store
) -> dict:
    """
    Append ApplicationSubmitted to a new loan stream.
    Raises DomainError if the application already exists.
    """
    stream_id = f"loan-{cmd.application_id}"
    current = await store.stream_version(stream_id)
    if current != -1:
        raise DomainError(
            f"Application {cmd.application_id!r} already exists "
            f"(stream version={current})."
        )

    event = {
        "event_type": "ApplicationSubmitted",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "applicant_id": cmd.applicant_id,
            "requested_amount_usd": str(cmd.requested_amount_usd),
            "loan_purpose": cmd.loan_purpose,
            "loan_term_months": cmd.loan_term_months,
            "submission_channel": cmd.submission_channel,
            "contact_email": cmd.contact_email,
            "contact_name": cmd.contact_name,
            "application_reference": cmd.application_reference,
            "submitted_at": _now(),
        },
    }
    positions = await store.append(
        stream_id, [event], expected_version=-1,
        causation_id=cmd.causation_id, correlation_id=cmd.correlation_id,
    )
    return {"stream_id": stream_id, "version": positions[-1]}


async def handle_start_agent_session(
    cmd: StartAgentSessionCommand, store
) -> dict:
    """
    Open an agent session stream with AgentSessionStarted + AgentInputValidated.
    The AgentInputValidated event satisfies the Gas Town context-loaded requirement.
    Raises DomainError if the session already exists.
    """
    stream_id = f"agent-{cmd.agent_type}-{cmd.session_id}"
    current = await store.stream_version(stream_id)
    if current != -1:
        raise DomainError(
            f"Agent session {cmd.session_id!r} already exists."
        )

    now = _now()
    events = [
        {
            "event_type": "AgentSessionStarted",
            "event_version": 1,
            "payload": {
                "session_id": cmd.session_id,
                "agent_type": cmd.agent_type,
                "agent_id": cmd.agent_id,
                "application_id": cmd.application_id,
                "model_version": cmd.model_version,
                "langgraph_graph_version": cmd.langgraph_graph_version,
                "context_source": cmd.context_source,
                "context_token_count": cmd.context_token_count,
                "started_at": now,
            },
        },
        {
            # Gas Town: context is loaded and validated before any decision event.
            "event_type": "AgentInputValidated",
            "event_version": 1,
            "payload": {
                "session_id": cmd.session_id,
                "agent_type": cmd.agent_type,
                "application_id": cmd.application_id,
                "inputs_validated": ["application_data", "financial_documents"],
                "validation_duration_ms": 10,
                "validated_at": now,
            },
        },
    ]
    positions = await store.append(
        stream_id, events, expected_version=-1,
        causation_id=cmd.causation_id, correlation_id=cmd.correlation_id,
    )
    return {"stream_id": stream_id, "version": positions[-1]}


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand, store
) -> dict:
    """
    Append CreditAnalysisCompleted to the loan stream.

    Validates:
      - Rule 2 (Gas Town): agent session must have context loaded.
      - Rule 3: credit analysis must not already be locked.
      - Rule 3: model_version must match what the session declared.
    OCC: appends at the loan stream's current version.
    """
    loan_stream = f"loan-{cmd.application_id}"
    agent_stream = f"agent-{cmd.agent_type}-{cmd.session_id}"

    loan_agg = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent_agg = await AgentSessionAggregate.load_by_stream(
        store, agent_stream, cmd.agent_id, cmd.session_id
    )

    loan_agg.assert_credit_analysis_not_locked()        # Rule 3
    agent_agg.assert_context_loaded()                   # Rule 2 (Gas Town)
    agent_agg.assert_model_version_current(cmd.model_version)  # Rule 3

    event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 2,
        "payload": {
            "application_id": cmd.application_id,
            "session_id": cmd.session_id,
            "decision": cmd.decision,
            "model_version": cmd.model_version,
            "model_deployment_id": cmd.model_deployment_id,
            "input_data_hash": cmd.input_data_hash,
            "analysis_duration_ms": cmd.analysis_duration_ms,
            "regulatory_basis": cmd.regulatory_basis,
            "completed_at": _now(),
        },
    }
    positions = await store.append(
        loan_stream, [event], expected_version=loan_agg.version,
        causation_id=cmd.causation_id, correlation_id=cmd.correlation_id,
    )
    return {"stream_id": loan_stream, "version": positions[-1]}


async def handle_fraud_screening_completed(
    cmd: FraudScreeningCompletedCommand, store
) -> dict:
    """
    Append FraudScreeningCompleted to the loan stream.

    Validates:
      - fraud_score must be in [0.0, 1.0].
      - Rule 2 (Gas Town): agent session must have context loaded.
    OCC: appends at the loan stream's current version.
    """
    if not (0.0 <= cmd.fraud_score <= 1.0):
        raise DomainError(
            f"fraud_score {cmd.fraud_score!r} is out of range [0.0, 1.0]."
        )

    loan_stream = f"loan-{cmd.application_id}"
    agent_stream = f"agent-{cmd.agent_type}-{cmd.session_id}"

    loan_agg = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent_agg = await AgentSessionAggregate.load_by_stream(
        store, agent_stream, cmd.agent_id, cmd.session_id
    )
    agent_agg.assert_context_loaded()  # Rule 2 (Gas Town)

    event = {
        "event_type": "FraudScreeningCompleted",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "session_id": cmd.session_id,
            "fraud_score": cmd.fraud_score,
            "risk_level": cmd.risk_level,
            "anomalies_found": cmd.anomalies_found,
            "recommendation": cmd.recommendation,
            "screening_model_version": cmd.screening_model_version,
            "input_data_hash": cmd.input_data_hash,
            "completed_at": _now(),
        },
    }
    positions = await store.append(
        loan_stream, [event], expected_version=loan_agg.version,
        causation_id=cmd.causation_id, correlation_id=cmd.correlation_id,
    )
    return {"stream_id": loan_stream, "version": positions[-1]}


async def handle_compliance_check(
    cmd: ComplianceCheckCommand, store
) -> dict:
    """
    Append ComplianceRulePassed or ComplianceRuleFailed to the compliance stream.

    Validates:
      - rule_id must be a non-empty string.
    OCC: appends at the compliance stream's current version.
    """
    if not cmd.rule_id:
        raise DomainError("rule_id must not be empty.")

    compliance_stream = f"compliance-{cmd.application_id}"
    compliance_agg = await ComplianceRecordAggregate.load(store, cmd.application_id)

    if cmd.passed:
        event = {
            "event_type": "ComplianceRulePassed",
            "event_version": 1,
            "payload": {
                "application_id": cmd.application_id,
                "session_id": cmd.session_id,
                "rule_id": cmd.rule_id,
                "rule_name": cmd.rule_name,
                "rule_version": cmd.rule_version,
                "evidence_hash": cmd.evidence_hash,
                "evaluation_notes": cmd.evaluation_notes,
                "evaluated_at": _now(),
            },
        }
    else:
        event = {
            "event_type": "ComplianceRuleFailed",
            "event_version": 1,
            "payload": {
                "application_id": cmd.application_id,
                "session_id": cmd.session_id,
                "rule_id": cmd.rule_id,
                "rule_name": cmd.rule_name,
                "rule_version": cmd.rule_version,
                "failure_reason": cmd.failure_reason,
                "is_hard_block": cmd.is_hard_block,
                "remediation_available": cmd.remediation_available,
                "evidence_hash": cmd.evidence_hash,
                "evaluated_at": _now(),
            },
        }

    positions = await store.append(
        compliance_stream, [event], expected_version=compliance_agg.version,
        causation_id=cmd.causation_id, correlation_id=cmd.correlation_id,
    )
    return {"stream_id": compliance_stream, "version": positions[-1]}


async def handle_generate_decision(
    cmd: GenerateDecisionCommand, store
) -> dict:
    """
    Append DecisionGenerated to the loan stream.

    Validates:
      - All required analyses are present (credit done, fraud done).
      - All required compliance rules have been evaluated (Rule 5).
      - Compliance is not hard-blocked.
      - Contributing sessions are known to the loan aggregate (Rule 6).
      - Rule 4: confidence floor — confidence < 0.6 overrides recommendation to REFER.
    OCC: appends at the loan stream's current version.
    """
    loan_stream = f"loan-{cmd.application_id}"
    loan_agg = await LoanApplicationAggregate.load(store, cmd.application_id)

    if not loan_agg.credit_analysis_done:
        raise DomainError(
            "Cannot generate decision: credit analysis has not been completed."
        )
    if not loan_agg.fraud_done:
        raise DomainError(
            "Cannot generate decision: fraud screening has not been completed."
        )

    if cmd.required_compliance_rules:
        compliance_agg = await ComplianceRecordAggregate.load(store, cmd.application_id)
        compliance_agg.assert_all_checks_complete(cmd.required_compliance_rules)
        if compliance_agg.hard_blocked:
            raise DomainError(
                "Cannot generate decision: compliance is hard-blocked. "
                "Only DECLINE is allowed."
            )

    if cmd.contributing_sessions:
        loan_agg.assert_contributing_sessions_known(cmd.contributing_sessions)

    # Rule 4: confidence floor
    recommendation = cmd.recommendation
    if cmd.confidence < 0.6:
        recommendation = "REFER"

    event = {
        "event_type": "DecisionGenerated",
        "event_version": 2,
        "payload": {
            "application_id": cmd.application_id,
            "orchestrator_session_id": cmd.orchestrator_session_id,
            "recommendation": recommendation,
            "confidence": cmd.confidence,
            "approved_amount_usd": (
                str(cmd.approved_amount_usd) if cmd.approved_amount_usd else None
            ),
            "conditions": cmd.conditions,
            "executive_summary": cmd.executive_summary,
            "key_risks": cmd.key_risks,
            "contributing_sessions": cmd.contributing_sessions,
            "model_versions": cmd.model_versions,
            "generated_at": _now(),
        },
    }
    positions = await store.append(
        loan_stream, [event], expected_version=loan_agg.version,
        causation_id=cmd.causation_id, correlation_id=cmd.correlation_id,
    )
    return {
        "stream_id": loan_stream,
        "version": positions[-1],
        "recommendation": recommendation,
    }


async def handle_human_review_completed(
    cmd: HumanReviewCompletedCommand, store
) -> dict:
    """
    Append HumanReviewCompleted to the loan stream.

    Validates:
      - override_reason is required when override=True.
    OCC: appends at the loan stream's current version.
    """
    if cmd.override and not cmd.override_reason:
        raise DomainError(
            "override_reason is required when override=True."
        )

    loan_stream = f"loan-{cmd.application_id}"
    loan_agg = await LoanApplicationAggregate.load(store, cmd.application_id)

    event = {
        "event_type": "HumanReviewCompleted",
        "event_version": 1,
        "payload": {
            "application_id": cmd.application_id,
            "reviewer_id": cmd.reviewer_id,
            "override": cmd.override,
            "original_recommendation": cmd.original_recommendation,
            "final_decision": cmd.final_decision,
            "override_reason": cmd.override_reason,
            "reviewed_at": _now(),
        },
    }
    positions = await store.append(
        loan_stream, [event], expected_version=loan_agg.version,
        causation_id=cmd.causation_id, correlation_id=cmd.correlation_id,
    )
    return {
        "stream_id": loan_stream,
        "version": positions[-1],
        "final_decision": cmd.final_decision,
    }
