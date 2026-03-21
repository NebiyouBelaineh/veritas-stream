"""
ledger/domain/aggregates/loan_application.py
=============================================
LoanApplicationAggregate — pure domain logic, no I/O except through the
EventStore interface passed to load().

Business rules enforced here:
  1. State machine: only valid transitions allowed
  2. Credit analysis is locked once completed (no second analysis without override)
  3. Confidence floor: confidence < 0.6 forces recommendation = REFER
  4. Compliance dependency: approval only if compliance is CLEAR
  5. Causal chain: contributing_sessions must reference sessions seen by this aggregate
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum


class DomainError(Exception):
    """Raised when a business rule is violated."""


class ApplicationState(str, Enum):
    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    DOCUMENTS_PENDING = "DOCUMENTS_PENDING"
    DOCUMENTS_UPLOADED = "DOCUMENTS_UPLOADED"
    DOCUMENTS_PROCESSED = "DOCUMENTS_PROCESSED"
    CREDIT_ANALYSIS_REQUESTED = "CREDIT_ANALYSIS_REQUESTED"
    CREDIT_ANALYSIS_COMPLETE = "CREDIT_ANALYSIS_COMPLETE"
    FRAUD_SCREENING_REQUESTED = "FRAUD_SCREENING_REQUESTED"
    FRAUD_SCREENING_COMPLETE = "FRAUD_SCREENING_COMPLETE"
    COMPLIANCE_CHECK_REQUESTED = "COMPLIANCE_CHECK_REQUESTED"
    COMPLIANCE_CHECK_COMPLETE = "COMPLIANCE_CHECK_COMPLETE"
    PENDING_DECISION = "PENDING_DECISION"
    PENDING_HUMAN_REVIEW = "PENDING_HUMAN_REVIEW"
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"
    DECLINED_COMPLIANCE = "DECLINED_COMPLIANCE"
    REFERRED = "REFERRED"


VALID_TRANSITIONS: dict[ApplicationState, list[ApplicationState]] = {
    ApplicationState.NEW: [ApplicationState.SUBMITTED],
    ApplicationState.SUBMITTED: [ApplicationState.DOCUMENTS_PENDING],
    ApplicationState.DOCUMENTS_PENDING: [ApplicationState.DOCUMENTS_UPLOADED],
    ApplicationState.DOCUMENTS_UPLOADED: [ApplicationState.DOCUMENTS_PROCESSED],
    ApplicationState.DOCUMENTS_PROCESSED: [ApplicationState.CREDIT_ANALYSIS_REQUESTED],
    ApplicationState.CREDIT_ANALYSIS_REQUESTED: [ApplicationState.CREDIT_ANALYSIS_COMPLETE],
    ApplicationState.CREDIT_ANALYSIS_COMPLETE: [ApplicationState.FRAUD_SCREENING_REQUESTED],
    ApplicationState.FRAUD_SCREENING_REQUESTED: [ApplicationState.FRAUD_SCREENING_COMPLETE],
    ApplicationState.FRAUD_SCREENING_COMPLETE: [ApplicationState.COMPLIANCE_CHECK_REQUESTED],
    ApplicationState.COMPLIANCE_CHECK_REQUESTED: [ApplicationState.COMPLIANCE_CHECK_COMPLETE],
    ApplicationState.COMPLIANCE_CHECK_COMPLETE: [
        ApplicationState.PENDING_DECISION,
        ApplicationState.DECLINED_COMPLIANCE,
    ],
    ApplicationState.PENDING_DECISION: [
        ApplicationState.APPROVED,
        ApplicationState.DECLINED,
        ApplicationState.PENDING_HUMAN_REVIEW,
    ],
    ApplicationState.PENDING_HUMAN_REVIEW: [
        ApplicationState.APPROVED,
        ApplicationState.DECLINED,
    ],
}


@dataclass
class LoanApplicationAggregate:
    application_id: str
    state: ApplicationState = ApplicationState.NEW
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    loan_purpose: str | None = None
    version: int = -1

    # Business rule state
    credit_analysis_done: bool = False
    fraud_done: bool = False
    compliance_verdict: str | None = None          # "CLEAR" | "BLOCKED" | "CONDITIONAL"
    recommendation: str | None = None              # from DecisionGenerated
    confidence: float | None = None               # from DecisionGenerated
    known_sessions: set = field(default_factory=set)  # agent session_ids seen via AgentOutputWritten

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        """Load and replay the event stream to rebuild aggregate state."""
        agg = cls(application_id=application_id)
        stream_events = await store.load_stream(f"loan-{application_id}")
        for event in stream_events:
            agg.apply(event)
        return agg

    def apply(self, event: dict) -> None:
        """
        Dispatch one stored event to its per-type handler method.

        Precondition: event dict contains 'event_type' and 'payload'.
        Guarantee: self.version increments; matching _on_<event_type> called if it exists.
        Unknown event types are silently ignored (forward compatibility).
        """
        self.version += 1
        handler = getattr(self, f"_on_{event.get('event_type', '')}", None)
        if handler:
            handler(event.get("payload", {}))

    # ── Per-event handlers ────────────────────────────────────────────────────

    def _on_ApplicationSubmitted(self, p: dict) -> None:
        # Transition: NEW → SUBMITTED. Sets applicant identity and loan terms.
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = p.get("applicant_id")
        self.requested_amount_usd = p.get("requested_amount_usd")
        self.loan_purpose = p.get("loan_purpose")

    def _on_DocumentUploadRequested(self, p: dict) -> None:
        # Transition: SUBMITTED → DOCUMENTS_PENDING.
        self.state = ApplicationState.DOCUMENTS_PENDING

    def _on_DocumentUploaded(self, p: dict) -> None:
        # Transition: DOCUMENTS_PENDING → DOCUMENTS_UPLOADED.
        self.state = ApplicationState.DOCUMENTS_UPLOADED

    def _on_PackageReadyForAnalysis(self, p: dict) -> None:
        # Transition: DOCUMENTS_UPLOADED → DOCUMENTS_PROCESSED.
        self.state = ApplicationState.DOCUMENTS_PROCESSED

    def _on_CreditAnalysisRequested(self, p: dict) -> None:
        # Transition: DOCUMENTS_PROCESSED → CREDIT_ANALYSIS_REQUESTED.
        self.state = ApplicationState.CREDIT_ANALYSIS_REQUESTED

    def _on_CreditAnalysisCompleted(self, p: dict) -> None:
        # Transition: CREDIT_ANALYSIS_REQUESTED → CREDIT_ANALYSIS_COMPLETE. Locks credit analysis.
        self.state = ApplicationState.CREDIT_ANALYSIS_COMPLETE
        self.credit_analysis_done = True

    def _on_FraudScreeningRequested(self, p: dict) -> None:
        # Transition: CREDIT_ANALYSIS_COMPLETE → FRAUD_SCREENING_REQUESTED.
        self.state = ApplicationState.FRAUD_SCREENING_REQUESTED

    def _on_FraudScreeningCompleted(self, p: dict) -> None:
        # Transition: FRAUD_SCREENING_REQUESTED → FRAUD_SCREENING_COMPLETE.
        self.state = ApplicationState.FRAUD_SCREENING_COMPLETE
        self.fraud_done = True

    def _on_ComplianceCheckRequested(self, p: dict) -> None:
        # Transition: FRAUD_SCREENING_COMPLETE → COMPLIANCE_CHECK_REQUESTED.
        self.state = ApplicationState.COMPLIANCE_CHECK_REQUESTED

    def _on_ComplianceCheckCompleted(self, p: dict) -> None:
        # Transition: COMPLIANCE_CHECK_REQUESTED → COMPLIANCE_CHECK_COMPLETE or DECLINED_COMPLIANCE.
        verdict = p.get("overall_verdict", "BLOCKED")
        self.compliance_verdict = verdict
        if p.get("has_hard_block", False) or verdict == "BLOCKED":
            self.state = ApplicationState.DECLINED_COMPLIANCE
        else:
            self.state = ApplicationState.COMPLIANCE_CHECK_COMPLETE

    def _on_DecisionRequested(self, p: dict) -> None:
        # Intermediate event — state unchanged; orchestrator manages flow.
        pass

    def _on_DecisionGenerated(self, p: dict) -> None:
        # Transition: COMPLIANCE_CHECK_COMPLETE → PENDING_DECISION or PENDING_HUMAN_REVIEW.
        # Rule 4: confidence floor forces REFER below 0.6.
        confidence = p.get("confidence", 0.0)
        recommendation = p.get("recommendation", "REFER")
        if confidence < 0.6:
            recommendation = "REFER"
        self.recommendation = recommendation
        self.confidence = confidence
        for sid in p.get("contributing_sessions", []):
            self.known_sessions.add(sid)
        if recommendation in ("REFER", "MANUAL_REVIEW"):
            self.state = ApplicationState.PENDING_HUMAN_REVIEW
        else:
            self.state = ApplicationState.PENDING_DECISION

    def _on_HumanReviewRequested(self, p: dict) -> None:
        # Transition: PENDING_DECISION → PENDING_HUMAN_REVIEW.
        self.state = ApplicationState.PENDING_HUMAN_REVIEW

    def _on_HumanReviewCompleted(self, p: dict) -> None:
        # Transition: PENDING_HUMAN_REVIEW → APPROVED or DECLINED.
        final = p.get("final_decision", "DECLINED")
        if final == "APPROVED":
            self.state = ApplicationState.APPROVED
        else:
            self.state = ApplicationState.DECLINED

    def _on_ApplicationApproved(self, p: dict) -> None:
        # Transition: PENDING_DECISION → APPROVED.
        self.state = ApplicationState.APPROVED

    def _on_ApplicationDeclined(self, p: dict) -> None:
        # Transition: PENDING_DECISION → DECLINED.
        self.state = ApplicationState.DECLINED

    def _on_AgentOutputWritten(self, p: dict) -> None:
        # Records contributing session IDs for Rule 6 validation.
        session_id = p.get("session_id")
        if session_id:
            self.known_sessions.add(session_id)

    # ── Business rule assertions ──────────────────────────────────────────────

    def assert_valid_transition(self, target: ApplicationState) -> None:
        """Rule 1: Only valid state-machine transitions are allowed."""
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            raise DomainError(
                f"Invalid transition {self.state} → {target}. "
                f"Allowed from {self.state}: {[s.value for s in allowed]}"
            )

    def assert_credit_analysis_not_locked(self) -> None:
        """Rule 3: A second CreditAnalysisCompleted is rejected once one exists."""
        if self.credit_analysis_done:
            raise DomainError(
                "CreditAnalysisCompleted already recorded for this application. "
                "A HumanReviewOverride is required before a second analysis."
            )

    def assert_compliance_cleared(self) -> None:
        """Rule 5: ApplicationApproved requires compliance verdict = CLEAR."""
        if self.compliance_verdict != "CLEAR":
            raise DomainError(
                f"Cannot approve application: compliance verdict is "
                f"{self.compliance_verdict!r}, expected 'CLEAR'."
            )

    def assert_contributing_sessions_known(self, contributing_sessions: list[str]) -> None:
        """Rule 6: Every session cited in DecisionGenerated must be known to this aggregate."""
        unknown = set(contributing_sessions) - self.known_sessions
        if unknown:
            raise DomainError(
                f"DecisionGenerated references unknown agent sessions: {unknown}. "
                "Sessions must have written output to this application before being cited."
            )
