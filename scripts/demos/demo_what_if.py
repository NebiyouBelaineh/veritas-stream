"""
scripts/demos/demo_what_if.py
==============================
Demonstrates the what-if projector with configurable counterfactual scenarios.

Each scenario in the config file defines:
  - which event in the loan stream to branch at
  - which payload fields to override in the counterfactual

The script seeds one application, builds the real projection baseline, then runs
every scenario from the config and prints a side-by-side divergence summary.

The store is never written to. All replay happens in memory.

Run with the default scenario file:
    uv run python scripts/demos/demo_what_if.py

Run with a custom scenario file:
    uv run python scripts/demos/demo_what_if.py --config path/to/my_scenarios.json

Config file format (JSON):
    {
      "app_id": "DEMO-WHATIF-001",
      "scenarios": [
        {
          "name": "Human-readable label",
          "description": "What question this scenario answers",
          "branch_at": "CreditAnalysisCompleted",
          "overrides": {
            "decision": { "risk_tier": "HIGH" }
          }
        }
      ]
    }

Supported branch_at values:
  - "CreditAnalysisCompleted"  overrideable fields: decision.risk_tier,
                                decision.confidence, decision.recommended_limit_usd
  - "FraudScreeningCompleted"  overrideable fields: fraud_score, risk_level,
                                recommendation
"""
import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from ledger.event_store import EventStore
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.commands.handlers import (
    handle_submit_application, SubmitApplicationCommand,
    handle_start_agent_session, StartAgentSessionCommand,
    handle_credit_analysis_completed, CreditAnalysisCompletedCommand,
    handle_fraud_screening_completed, FraudScreeningCompletedCommand,
    handle_compliance_check, ComplianceCheckCommand,
    handle_generate_decision, GenerateDecisionCommand,
)
from what_if.projector import run_what_if

DEFAULT_CONFIG = Path(__file__).parent / "what_if_scenarios.json"
DB_URL = os.environ["DATABASE_URL"]
SEP = "-" * 60
WIDE_SEP = "=" * 60


# ── Helpers ───────────────────────────────────────────────────────────────────

def _deep_merge(base: dict, overrides: dict) -> dict:
    """Return a new dict with overrides merged into base.
    Nested dicts are merged recursively so callers only need to specify
    the fields that change, not the entire payload."""
    result = dict(base)
    for key, value in overrides.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _print_summary_row(row: dict):
    print(f"    state             : {row.get('state')}")
    print(f"    risk_tier         : {row.get('risk_tier')}")
    print(f"    fraud_score       : {row.get('fraud_score')}")
    print(f"    compliance_status : {row.get('compliance_status')}")
    print(f"    decision          : {row.get('decision')}")
    print(f"    approved_amount   : {row.get('approved_amount_usd')}")
    print(f"    last_event_type   : {row.get('last_event_type')}")


def _print_overrides(overrides: dict, indent: int = 4):
    """Pretty-print the override dict so it is easy to read at a glance."""
    pad = " " * indent
    for key, value in overrides.items():
        if isinstance(value, dict):
            print(f"{pad}{key}:")
            _print_overrides(value, indent + 2)
        else:
            print(f"{pad}{key}: {value}")


# ── Seeding ───────────────────────────────────────────────────────────────────

async def seed_application(store, app_id: str):
    """Write a complete loan lifecycle: Submitted → Credit → Fraud → Compliance → Decision."""
    credit_session = f"sess-credit-{app_id}"
    fraud_session  = f"sess-fraud-{app_id}"
    orch_session   = f"sess-orch-{app_id}"

    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id, applicant_id="COMP-001",
            requested_amount_usd=Decimal("500000"),
            loan_purpose="working_capital", loan_term_months=36,
        ), store
    )
    print("  ApplicationSubmitted written.")

    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id=credit_session, agent_type="credit_analysis",
            agent_id="credit_agent_01", application_id=app_id,
            model_version="google/gemini-2.5-flash",
        ), store
    )
    loan_ver = await store.stream_version(f"loan-{app_id}")
    await store.append(f"loan-{app_id}", [
        {"event_type": "DocumentUploaded",        "event_version": 1, "payload": {}},
        {"event_type": "PackageReadyForAnalysis", "event_version": 1, "payload": {}},
        {"event_type": "CreditAnalysisRequested", "event_version": 1, "payload": {}},
    ], expected_version=loan_ver)

    await handle_credit_analysis_completed(
        CreditAnalysisCompletedCommand(
            application_id=app_id, session_id=credit_session,
            agent_id="credit_agent_01", agent_type="credit_analysis",
            model_version="google/gemini-2.5-flash",
            model_deployment_id="openrouter-prod",
            decision={
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": 500000,
                "confidence": 0.82,
                "rationale": "Revenue $6.37M, EBITDA 8%, elevated D/E 2.3x.",
            },
        ), store
    )
    print("  CreditAnalysisCompleted written. (risk_tier=MEDIUM, confidence=0.82)")

    # Capture the CreditAnalysisCompleted event_id so DecisionGenerated can
    # declare a causal dependency on it. This enables the what-if projector to
    # suppress DecisionGenerated (and ApplicationApproved) when branching at
    # CreditAnalysisCompleted, producing a materially different counterfactual.
    loan_events_after_credit = await store.load_stream(f"loan-{app_id}")
    credit_event_id = str(next(
        e["event_id"] for e in loan_events_after_credit
        if e["event_type"] == "CreditAnalysisCompleted"
    ))

    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id=fraud_session, agent_type="fraud_screening",
            agent_id="fraud_agent_01", application_id=app_id,
            model_version="google/gemini-2.5-flash",
        ), store
    )
    loan_ver = await store.stream_version(f"loan-{app_id}")
    await store.append(f"loan-{app_id}", [
        {"event_type": "FraudScreeningRequested", "event_version": 1, "payload": {}},
    ], expected_version=loan_ver)

    await handle_fraud_screening_completed(
        FraudScreeningCompletedCommand(
            application_id=app_id, session_id=fraud_session,
            agent_id="fraud_agent_01", agent_type="fraud_screening",
            fraud_score=0.05, risk_level="LOW",
            recommendation="PROCEED",
            screening_model_version="google/gemini-2.5-flash",
            # Declares causal dependency on credit: fraud screening was triggered
            # because CreditAnalysisCompleted initiated the pipeline stage.
            # This extends the causal chain so that branching at CreditAnalysisCompleted
            # also transitively suppresses FraudScreeningCompleted (and DecisionGenerated).
            causation_id=credit_event_id,
        ), store
    )
    print("  FraudScreeningCompleted written. (fraud_score=0.05)")

    # Capture the FraudScreeningCompleted event_id so DecisionGenerated can
    # declare a causal dependency on it. This means branching at either
    # CreditAnalysisCompleted or FraudScreeningCompleted will transitively
    # suppress DecisionGenerated via the chain: Credit → Fraud → Decision.
    loan_events_after_fraud = await store.load_stream(f"loan-{app_id}")
    fraud_event_id = str(next(
        e["event_id"] for e in loan_events_after_fraud
        if e["event_type"] == "FraudScreeningCompleted"
    ))

    for rule_id, rule_name in [("AML-001", "AML"), ("KYC-001", "KYC")]:
        await handle_compliance_check(
            ComplianceCheckCommand(
                application_id=app_id, session_id="sess-comp",
                rule_id=rule_id, rule_name=rule_name,
                rule_version="2026-Q1", passed=True,
            ), store
        )
    print("  ComplianceRulePassed x2 written.")

    # Append ComplianceCheckCompleted summary so the projection baseline shows
    # compliance_status=PASSED. This goes on the compliance stream; the what-if
    # projector only loads the loan stream so both real and counterfactual show
    # None symmetrically (no false divergence).
    compliance_ver = await store.stream_version(f"compliance-{app_id}")
    await store.append(f"compliance-{app_id}", [
        {
            "event_type": "ComplianceCheckCompleted",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "session_id": "sess-comp",
                "rules_evaluated": ["AML-001", "KYC-001"],
                "rules_passed": ["AML-001", "KYC-001"],
                "rules_failed": [],
                "rules_noted": [],
                "has_hard_block": False,
                "overall_verdict": "PASSED",
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
        }
    ], expected_version=compliance_ver)
    print("  ComplianceCheckCompleted written. (overall_verdict=PASSED)")

    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id=orch_session, agent_type="decision_orchestrator",
            agent_id="orch_agent_01", application_id=app_id,
            model_version="google/gemini-2.5-flash",
        ), store
    )
    await handle_generate_decision(
        GenerateDecisionCommand(
            application_id=app_id,
            orchestrator_session_id=orch_session,
            recommendation="APPROVE", confidence=0.82,
            executive_summary="Solid revenue, acceptable risk.",
            approved_amount_usd=Decimal("500000"),
            required_compliance_rules={"AML-001", "KYC-001"},
            # Declares causal dependency on fraud: the orchestrator's decision
            # depends on the fraud screening outcome. Combined with fraud's own
            # causation_id pointing to credit, the chain is:
            #   CreditAnalysisCompleted → FraudScreeningCompleted → DecisionGenerated
            # Branching at either credit OR fraud will transitively suppress
            # DecisionGenerated (and ApplicationApproved) via the chain.
            causation_id=fraud_event_id,
        ), store
    )
    print("  DecisionGenerated written. (APPROVE, $500,000)")

    # Capture DecisionGenerated event_id and append ApplicationApproved with a
    # causal link so the projector shows state=APPROVED. In counterfactual credit
    # scenarios, DecisionGenerated is suppressed → ApplicationApproved (whose
    # causation_id = DecisionGenerated event_id) is also suppressed transitively,
    # yielding state=SUBMITTED vs state=APPROVED in the divergence summary.
    loan_events_after_decision = await store.load_stream(f"loan-{app_id}")
    decision_event_id = str(next(
        e["event_id"] for e in loan_events_after_decision
        if e["event_type"] == "DecisionGenerated"
    ))
    loan_ver = await store.stream_version(f"loan-{app_id}")
    await store.append(f"loan-{app_id}", [
        {
            "event_type": "ApplicationApproved",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "approved_amount_usd": "500000",
                "interest_rate_pct": 8.5,
                "term_months": 36,
                "conditions": [],
                "approved_by": "decision_orchestrator",
                "effective_date": "2026-03-24",
                "approved_at": datetime.now(timezone.utc).isoformat(),
            },
        }
    ], expected_version=loan_ver, causation_id=decision_event_id)
    print("  ApplicationApproved written. (state → APPROVED)")


# ── Scenario runner ───────────────────────────────────────────────────────────

async def run_scenario(store, scenario: dict, stream_events: list, app_id: str,
                       scenario_number: int, total: int):
    """
    Run one what-if scenario from the config.

    Finds the real event of the branch type in the stream, deep-merges the
    overrides into its payload, and calls run_what_if() to compare outcomes.
    """
    name        = scenario["name"]
    description = scenario.get("description", "")
    branch_at   = scenario["branch_at"]
    overrides   = scenario.get("overrides", {})

    print()
    print(WIDE_SEP)
    print(f"SCENARIO {scenario_number}/{total}: {name}")
    print(WIDE_SEP)
    print(f"  Branch at : {branch_at}")
    print(f"  Question  : {description}")
    print()
    print("  Overrides applied to the counterfactual event:")
    _print_overrides(overrides)

    # Find the real stored event of this type in the stream
    real_event = next(
        (e for e in stream_events if e.get("event_type") == branch_at), None
    )
    if real_event is None:
        print(f"\n  SKIP: event type '{branch_at}' not found in stream.")
        return None

    # Build the counterfactual by merging overrides into the real payload
    counterfactual_payload = _deep_merge(real_event["payload"], overrides)
    counterfactual = {
        "event_type": real_event["event_type"],
        "event_version": real_event["event_version"],
        "payload": counterfactual_payload,
        "metadata": {},
    }

    t0 = time.perf_counter()
    result = await run_what_if(
        store=store,
        application_id=app_id,
        branch_at_event_type=branch_at,
        counterfactual_events=[counterfactual],
        projections=[
            ApplicationSummaryProjection(),
            ComplianceAuditViewProjection(),
        ],
    )
    elapsed = (time.perf_counter() - t0) * 1000

    real_row = result.real_outcome.get("application_summary", {}).get(app_id, {})
    cf_row   = result.counterfactual_outcome.get("application_summary", {}).get(app_id, {})

    print()
    print(f"  Completed in {elapsed:.1f} ms")
    print(f"  Branch position : {result.branch_position}")
    print(f"  Events before   : {result.events_before_branch}")
    print(f"  Events suppressed (causally dependent): {result.real_events_suppressed}")

    print()
    print("  --- Real outcome (ApplicationSummary) ---")
    _print_summary_row(real_row)

    print()
    print("  --- Counterfactual outcome (ApplicationSummary) ---")
    _print_summary_row(cf_row)

    print()
    if result.divergence_summary:
        print(f"  Divergences found: {len(result.divergence_summary)}")
        for i, diff in enumerate(result.divergence_summary, 1):
            print(f"  [{i}] {diff}")
    else:
        print("  No divergences: both paths produced identical projection state.")

    return result


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(config_path: Path):
    config = json.loads(config_path.read_text())
    app_id    = config.get("app_id", "DEMO-WHATIF-001")
    scenarios = config.get("scenarios", [])

    if not scenarios:
        print("No scenarios found in config. Exiting.")
        return

    store = EventStore(DB_URL)
    await store.connect()

    # Clean up any prior run so the demo is idempotent
    async with store._pool.acquire() as conn:
        for prefix in [
            f"loan-{app_id}", f"credit-{app_id}", f"fraud-{app_id}",
            f"compliance-{app_id}",
        ]:
            await conn.execute(
                "DELETE FROM outbox WHERE event_id IN "
                "(SELECT event_id FROM events WHERE stream_id=$1)", prefix
            )
            await conn.execute("DELETE FROM events        WHERE stream_id=$1", prefix)
            await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", prefix)
        for sid in [
            f"agent-credit_analysis-sess-credit-{app_id}",
            f"agent-fraud_screening-sess-fraud-{app_id}",
            f"agent-decision_orchestrator-sess-orch-{app_id}",
        ]:
            await conn.execute(
                "DELETE FROM outbox WHERE event_id IN "
                "(SELECT event_id FROM events WHERE stream_id=$1)", sid
            )
            await conn.execute("DELETE FROM events        WHERE stream_id=$1", sid)
            await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", sid)
        await conn.execute(
            "DELETE FROM projection_checkpoints WHERE projection_name LIKE 'proj_%'"
        )

    # ── STEP 1: Seed application ───────────────────────────────────────────────
    print(WIDE_SEP)
    print(f"SETUP - Seeding loan application {app_id}")
    print(WIDE_SEP)
    await seed_application(store, app_id)
    stream_events = await store.load_stream(f"loan-{app_id}")
    print(f"\n  Stream loan-{app_id} has {len(stream_events)} events.")

    # ── STEP 2: Build real projection baseline ─────────────────────────────────
    print()
    print(WIDE_SEP)
    print("SETUP - Materialise real projection baseline (run daemon once)")
    print(WIDE_SEP)

    summary    = ApplicationSummaryProjection()
    compliance = ComplianceAuditViewProjection()
    daemon     = ProjectionDaemon(store, [summary, compliance])

    t0 = time.perf_counter()
    dispatched = await daemon.run_once()
    elapsed = (time.perf_counter() - t0) * 1000
    print(f"  Dispatched {dispatched} (projection, event) pairs in {elapsed:.1f} ms")

    baseline = summary.get(app_id)
    assert baseline is not None, "Projection should have a row after daemon run"
    print(f"\n  Baseline ApplicationSummary for {app_id}:")
    _print_summary_row(vars(baseline))

    # ── STEP 3: Run each scenario ──────────────────────────────────────────────
    results = []
    for i, scenario in enumerate(scenarios, 1):
        result = await run_scenario(
            store, scenario, stream_events, app_id,
            scenario_number=i, total=len(scenarios),
        )
        if result:
            results.append((scenario["name"], result))

    # ── STEP 4: Summary table ──────────────────────────────────────────────────
    print()
    print(WIDE_SEP)
    print("SUMMARY - All scenarios vs real outcome")
    print(WIDE_SEP)
    print(f"  {'Scenario':<45} {'risk_tier':<10} {'fraud_score':<13} {'decision'}")
    print(f"  {'-'*45} {'-'*10} {'-'*13} {'-'*10}")

    real_row = results[0][1].real_outcome.get("application_summary", {}).get(app_id, {}) if results else {}
    print(f"  {'[REAL]':<45} {str(real_row.get('risk_tier')):<10} "
          f"{str(real_row.get('fraud_score')):<13} {real_row.get('decision')}")

    for name, result in results:
        cf_row = result.counterfactual_outcome.get("application_summary", {}).get(app_id, {})
        label = name[:44]
        print(f"  {label:<45} {str(cf_row.get('risk_tier')):<10} "
              f"{str(cf_row.get('fraud_score')):<13} {cf_row.get('decision')}")

    # Confirm the store was never written to
    async with store._pool.acquire() as conn:
        event_count = await conn.fetchval(
            "SELECT COUNT(*) FROM events WHERE stream_id=$1", f"loan-{app_id}"
        )
    assert event_count == len(stream_events), \
        f"FAIL: DB was modified! Expected {len(stream_events)} events, found {event_count}."
    print()
    print(f"  DB check: loan-{app_id} still has {event_count} events (store never written).")

    # Confirm the live baseline projection was never touched by any scenario
    live = summary.get(app_id)
    assert live is not None
    assert live.risk_tier == "MEDIUM", "Live projection must be untouched."
    assert live.state == "APPROVED", "Live projection state must be untouched."
    print(f"  Live projection check: risk_tier=MEDIUM, state=APPROVED (untouched by all scenarios).")

    print()
    print(f"PASSED - {len(results)} scenario(s) ran. Store never written.")

    await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="What-if projector demo")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="Path to a JSON scenario config file (default: what_if_scenarios.json)",
    )
    args = parser.parse_args()

    if not args.config.exists():
        print(f"Config file not found: {args.config}")
        sys.exit(1)

    print(f"Loading scenarios from: {args.config}")
    asyncio.run(main(args.config))