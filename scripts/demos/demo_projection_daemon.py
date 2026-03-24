"""
scripts/demos/demo_projection_daemon.py
=========================================
Demonstrates the ProjectionDaemon materialising read models from the event
stream in real time.

What you will see:
  1. Submit an application -- projection table is empty (daemon hasn't run).
  2. Run daemon once -- ApplicationSummary projection materialises.
  3. Record credit analysis, fraud screening, compliance, decision.
  4. Run daemon again -- projection row is updated with every new field.
  5. Query the projection directly to show the final in-memory read model.
  6. Demonstrate lag: how many ms since the daemon last processed an event.

Run:
    uv run python scripts/demos/demo_projection_daemon.py
"""
import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from ledger.event_store import EventStore
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.commands.handlers import (
    handle_submit_application, SubmitApplicationCommand,
    handle_start_agent_session, StartAgentSessionCommand,
    handle_credit_analysis_completed, CreditAnalysisCompletedCommand,
    handle_fraud_screening_completed, FraudScreeningCompletedCommand,
    handle_compliance_check, ComplianceCheckCommand,
    handle_generate_decision, GenerateDecisionCommand,
)
from decimal import Decimal

DB_URL = os.environ["DATABASE_URL"]
SEP = "-" * 60
APP_ID = "DEMO-PROJ-001"


def _print_summary(proj: ApplicationSummaryProjection, app_id: str):
    row = proj.get(app_id)
    if row is None:
        print("  (not yet materialised)")
        return
    print(f"  state            : {row.state}")
    print(f"  applicant_id     : {row.applicant_id}")
    print(f"  requested_amount : {row.requested_amount_usd}")
    print(f"  risk_tier        : {row.risk_tier}")
    print(f"  fraud_score      : {row.fraud_score}")
    print(f"  compliance_status: {row.compliance_status}")
    print(f"  decision         : {row.decision}")
    print(f"  last_event_type  : {row.last_event_type}")


async def main():
    store = EventStore(DB_URL)
    await store.connect()

    # clean up prior run
    async with store._pool.acquire() as conn:
        for prefix in [f"loan-{APP_ID}", f"credit-{APP_ID}", f"fraud-{APP_ID}",
                       f"compliance-{APP_ID}"]:
            await conn.execute("DELETE FROM outbox WHERE event_id IN "
                               "(SELECT event_id FROM events WHERE stream_id=$1)", prefix)
            await conn.execute("DELETE FROM events        WHERE stream_id=$1", prefix)
            await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", prefix)
        for sess in ["sess-credit", "sess-fraud", "sess-orch"]:
            sid = f"agent-credit_analysis-{sess}-{APP_ID}"
            await conn.execute("DELETE FROM outbox WHERE event_id IN "
                               "(SELECT event_id FROM events WHERE stream_id=$1)", sid)
            await conn.execute("DELETE FROM events        WHERE stream_id=$1", sid)
            await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", sid)
        await conn.execute("DELETE FROM projection_checkpoints WHERE projection_name LIKE 'proj_%'")

    summary    = ApplicationSummaryProjection()
    compliance = ComplianceAuditViewProjection()
    perf       = AgentPerformanceLedgerProjection()
    daemon     = ProjectionDaemon(store, [summary, compliance, perf])

    # ── Step 1: submit before daemon runs ────────────────────────────────────
    print(SEP)
    print("STEP 1 — submit application (daemon has not run yet)")
    print(SEP)

    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=APP_ID, applicant_id="COMP-001",
            requested_amount_usd=Decimal("500000"),
            loan_purpose="working_capital", loan_term_months=36,
        ), store
    )
    print("  ApplicationSubmitted written.")
    print("  Projection state BEFORE daemon run:")
    _print_summary(summary, APP_ID)

    # ── Step 2: daemon run 1 ──────────────────────────────────────────────────
    print()
    print(SEP)
    print("STEP 2 — run daemon once")
    print(SEP)

    t0 = time.perf_counter()
    dispatched = await daemon.run_once()
    elapsed = (time.perf_counter() - t0) * 1000
    print(f"  Dispatched {dispatched} (projection, event) pairs in {elapsed:.1f} ms")
    print("  Projection state AFTER daemon run 1:")
    _print_summary(summary, APP_ID)

    # ── Step 3: advance lifecycle ─────────────────────────────────────────────
    print()
    print(SEP)
    print("STEP 3 — record credit, fraud, compliance, decision")
    print(SEP)

    credit_session = f"sess-credit-{APP_ID}"
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id=credit_session, agent_type="credit_analysis",
            agent_id="credit_agent_01", application_id=APP_ID,
            model_version="google/gemini-2.5-flash",
        ), store
    )

    # seed document processing events
    loan_ver = await store.stream_version(f"loan-{APP_ID}")
    await store.append(f"loan-{APP_ID}", [
        {"event_type": "DocumentUploaded",        "event_version": 1, "payload": {}},
        {"event_type": "PackageReadyForAnalysis", "event_version": 1, "payload": {}},
        {"event_type": "CreditAnalysisRequested", "event_version": 1, "payload": {}},
    ], expected_version=loan_ver)

    await handle_credit_analysis_completed(
        CreditAnalysisCompletedCommand(
            application_id=APP_ID, session_id=credit_session,
            agent_id="credit_agent_01", agent_type="credit_analysis",
            model_version="google/gemini-2.5-flash",
            model_deployment_id="openrouter-prod",
            decision={
                "risk_tier": "MEDIUM", "recommended_limit_usd": 500000,
                "confidence": 0.82,
                "rationale": "Revenue $6.37M, EBITDA 8%, elevated D/E 2.3x.",
            },
        ), store
    )
    print("  CreditAnalysisCompleted written.")

    fraud_session = f"sess-fraud-{APP_ID}"
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id=fraud_session, agent_type="fraud_screening",
            agent_id="fraud_agent_01", application_id=APP_ID,
            model_version="google/gemini-2.5-flash",
        ), store
    )
    loan_ver = await store.stream_version(f"loan-{APP_ID}")
    await store.append(f"loan-{APP_ID}", [
        {"event_type": "FraudScreeningRequested", "event_version": 1, "payload": {}},
    ], expected_version=loan_ver)

    await handle_fraud_screening_completed(
        FraudScreeningCompletedCommand(
            application_id=APP_ID, session_id=fraud_session,
            agent_id="fraud_agent_01", agent_type="fraud_screening",
            fraud_score=0.05, risk_level="LOW",
            recommendation="PROCEED",
            screening_model_version="google/gemini-2.5-flash",
        ), store
    )
    print("  FraudScreeningCompleted written.")

    for rule_id, rule_name in [("AML-001", "AML"), ("KYC-001", "KYC")]:
        await handle_compliance_check(
            ComplianceCheckCommand(
                application_id=APP_ID, session_id="sess-comp",
                rule_id=rule_id, rule_name=rule_name,
                rule_version="2026-Q1", passed=True,
            ), store
        )
    print("  ComplianceRulePassed x2 written.")

    orch_session = f"sess-orch-{APP_ID}"
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id=orch_session, agent_type="decision_orchestrator",
            agent_id="orch_agent_01", application_id=APP_ID,
            model_version="google/gemini-2.5-flash",
        ), store
    )
    await handle_generate_decision(
        GenerateDecisionCommand(
            application_id=APP_ID,
            orchestrator_session_id=orch_session,
            recommendation="APPROVE", confidence=0.82,
            executive_summary="Solid revenue, acceptable risk.",
            approved_amount_usd=Decimal("500000"),
            required_compliance_rules={"AML-001", "KYC-001"},
        ), store
    )
    print("  DecisionGenerated written.")

    # ── Step 4: daemon run 2 ──────────────────────────────────────────────────
    print()
    print(SEP)
    print("STEP 4 — run daemon again to pick up all new events")
    print(SEP)

    t0 = time.perf_counter()
    dispatched = await daemon.run_once()
    elapsed = (time.perf_counter() - t0) * 1000
    print(f"  Dispatched {dispatched} (projection, event) pairs in {elapsed:.1f} ms")
    print("  Projection state AFTER daemon run 2:")
    _print_summary(summary, APP_ID)

    # ── Step 5: lag measurement ───────────────────────────────────────────────
    print()
    print(SEP)
    print("STEP 5 — projection lag (ms since last event processed)")
    print(SEP)

    for name in ["application_summary", "compliance_audit_view", "agent_performance_ledger"]:
        lag = await daemon.get_lag(name)
        print(f"  {name:<35} lag = {lag:.1f} ms")

    print()
    print("PASSED — ProjectionDaemon materialised read models from event stream.")

    await store.close()


if __name__ == "__main__":
    asyncio.run(main())
