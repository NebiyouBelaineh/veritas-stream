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

    async def handle(self, event: dict, conn=None) -> None:
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

        if conn is not None:
            await self._upsert(row, conn)

    async def _upsert(self, row: "ApplicationSummaryRow", conn) -> None:
        """Idempotent upsert of one row to the application_summary table."""
        import decimal
        def _numeric(v):
            if v is None:
                return None
            try:
                return decimal.Decimal(str(v))
            except Exception:
                return None

        last_event_at = None
        if row.last_event_at:
            try:
                from datetime import datetime
                last_event_at = datetime.fromisoformat(row.last_event_at)
            except (ValueError, TypeError):
                pass

        final_decision_at = None
        if row.final_decision_at:
            try:
                from datetime import datetime
                final_decision_at = datetime.fromisoformat(row.final_decision_at)
            except (ValueError, TypeError):
                pass

        await conn.execute(
            """INSERT INTO application_summary (
                   application_id, state, applicant_id,
                   requested_amount_usd, approved_amount_usd,
                   risk_tier, fraud_score, compliance_status, decision,
                   agent_sessions_completed, last_event_type, last_event_at,
                   human_reviewer_id, final_decision_at
               ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
               ON CONFLICT (application_id) DO UPDATE SET
                   state                    = EXCLUDED.state,
                   applicant_id             = EXCLUDED.applicant_id,
                   requested_amount_usd     = EXCLUDED.requested_amount_usd,
                   approved_amount_usd      = EXCLUDED.approved_amount_usd,
                   risk_tier                = EXCLUDED.risk_tier,
                   fraud_score              = EXCLUDED.fraud_score,
                   compliance_status        = EXCLUDED.compliance_status,
                   decision                 = EXCLUDED.decision,
                   agent_sessions_completed = EXCLUDED.agent_sessions_completed,
                   last_event_type          = EXCLUDED.last_event_type,
                   last_event_at            = EXCLUDED.last_event_at,
                   human_reviewer_id        = EXCLUDED.human_reviewer_id,
                   final_decision_at        = EXCLUDED.final_decision_at""",
            row.application_id,
            row.state,
            row.applicant_id,
            _numeric(row.requested_amount_usd),
            _numeric(row.approved_amount_usd),
            row.risk_tier,
            _numeric(row.fraud_score),
            row.compliance_status,
            row.decision,
            row.agent_sessions_completed,
            row.last_event_type,
            last_event_at,
            row.human_reviewer_id,
            final_decision_at,
        )

    async def warm_load(self, pool) -> None:
        """
        Populate the in-memory dict from the application_summary table.

        Called once at server startup so that previously processed applications
        are available immediately without replaying the full event stream.
        The daemon resumes from its checkpoint and handles only new events.
        """
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT application_id, state, applicant_id,
                          requested_amount_usd, approved_amount_usd,
                          risk_tier, fraud_score, compliance_status, decision,
                          agent_sessions_completed, last_event_type,
                          last_event_at, human_reviewer_id, final_decision_at
                   FROM application_summary"""
            )
        for r in rows:
            self._rows[r["application_id"]] = ApplicationSummaryRow(
                application_id=r["application_id"],
                state=r["state"] or "SUBMITTED",
                applicant_id=r["applicant_id"],
                requested_amount_usd=str(r["requested_amount_usd"]) if r["requested_amount_usd"] is not None else None,
                approved_amount_usd=str(r["approved_amount_usd"]) if r["approved_amount_usd"] is not None else None,
                risk_tier=r["risk_tier"],
                fraud_score=float(r["fraud_score"]) if r["fraud_score"] is not None else None,
                compliance_status=r["compliance_status"],
                decision=r["decision"],
                agent_sessions_completed=list(r["agent_sessions_completed"] or []),
                last_event_type=r["last_event_type"],
                last_event_at=r["last_event_at"].isoformat() if r["last_event_at"] else None,
                human_reviewer_id=r["human_reviewer_id"],
                final_decision_at=r["final_decision_at"].isoformat() if r["final_decision_at"] else None,
            )

    async def truncate(self) -> None:
        self._rows.clear()

    # ── Query interface ───────────────────────────────────────────────────────

    def get(self, application_id: str) -> ApplicationSummaryRow | None:
        return self._rows.get(application_id)

    def all(self) -> list[ApplicationSummaryRow]:
        return list(self._rows.values())
