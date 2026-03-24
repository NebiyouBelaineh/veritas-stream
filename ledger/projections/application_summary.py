"""
ledger/projections/application_summary.py
==========================================
ApplicationSummaryProjection — one row per loan application, updated as
events flow through the ProjectionDaemon.

The projection listens to ALL streams (events are delivered globally by the
daemon) and selects events that carry an application_id field.
"""
from __future__ import annotations
from dataclasses import dataclass, field

from ledger.projections import Projection


@dataclass
class ApplicationSummaryRow:
    application_id: str
    state: str = "SUBMITTED"
    applicant_id: str | None = None
    requested_amount_usd: str | None = None
    approved_amount_usd: str | None = None
    risk_tier: str | None = None
    fraud_score: float | None = None
    compliance_status: str | None = None     # verdict from ComplianceCheckCompleted
    decision: str | None = None              # recommendation from DecisionGenerated
    agent_sessions_completed: list = field(default_factory=list)
    last_event_type: str | None = None
    last_event_at: str | None = None
    human_reviewer_id: str | None = None
    final_decision_at: str | None = None


class ApplicationSummaryProjection(Projection):
    """
    Read model providing a denormalised summary of every loan application.

    Handles events from loan, compliance, and agent streams (the daemon
    delivers all events globally so no stream filtering is needed here).
    """

    name = "application_summary"

    def __init__(self) -> None:
        self._rows: dict[str, ApplicationSummaryRow] = {}

    # ── Projection interface ──────────────────────────────────────────────────

    async def handle(self, event: dict) -> None:
        et = event["event_type"]
        p = event.get("payload", {})
        app_id = p.get("application_id")
        if not app_id:
            return

        recorded_at = str(event.get("recorded_at", ""))
        row = self._rows.get(app_id)

        if et == "ApplicationSubmitted":
            row = ApplicationSummaryRow(
                application_id=app_id,
                state="SUBMITTED",
                applicant_id=p.get("applicant_id"),
                requested_amount_usd=str(p.get("requested_amount_usd", "")),
            )
            self._rows[app_id] = row

        if row is None:
            return  # event for an application we haven't seen submitted yet

        row.last_event_type = et
        row.last_event_at = recorded_at

        if et == "CreditAnalysisCompleted":
            decision = p.get("decision") or {}
            row.risk_tier = decision.get("risk_tier")

        elif et == "FraudScreeningCompleted":
            row.fraud_score = p.get("fraud_score")

        elif et == "ComplianceRulePassed":
            # Individual rule passed; mark PASSED unless already BLOCKED.
            if row.compliance_status != "BLOCKED":
                row.compliance_status = "PASSED"

        elif et == "ComplianceRuleFailed":
            if p.get("is_hard_block"):
                row.compliance_status = "BLOCKED"
            elif row.compliance_status != "BLOCKED":
                row.compliance_status = "FAILED"

        elif et == "ComplianceCheckCompleted":
            # Summary event overrides incremental rule status with the authoritative verdict.
            row.compliance_status = p.get("overall_verdict")

        elif et == "DecisionGenerated":
            row.decision = p.get("recommendation")
            approved = p.get("approved_amount_usd")
            if approved:
                row.approved_amount_usd = str(approved)

        elif et == "HumanReviewCompleted":
            final = p.get("final_decision", "DECLINED")
            row.state = final
            row.human_reviewer_id = p.get("reviewer_id")
            row.final_decision_at = recorded_at

        elif et == "ApplicationApproved":
            row.state = "APPROVED"
            row.approved_amount_usd = str(p.get("approved_amount_usd", ""))
            row.final_decision_at = recorded_at

        elif et == "ApplicationDeclined":
            row.state = "DECLINED"
            row.final_decision_at = recorded_at

        elif et == "AgentSessionCompleted":
            sid = p.get("session_id")
            if sid and sid not in row.agent_sessions_completed:
                row.agent_sessions_completed.append(sid)

    async def truncate(self) -> None:
        self._rows.clear()

    # ── Query interface ───────────────────────────────────────────────────────

    def get(self, application_id: str) -> ApplicationSummaryRow | None:
        return self._rows.get(application_id)

    def all(self) -> list[ApplicationSummaryRow]:
        return list(self._rows.values())
