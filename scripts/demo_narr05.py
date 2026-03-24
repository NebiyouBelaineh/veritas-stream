"""
scripts/demo_narr05.py
======================
NARR-05 end-to-end demo: full pipeline for COMP-068, then human override to APPROVE.

Usage:
    uv run python scripts/demo_narr05.py

Completes in < 90 seconds. Writes artifacts/regulatory_package_NARR05.json.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from anthropic import AsyncAnthropic

from ledger.event_store import InMemoryEventStore
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent
from ledger.agents.stub_agents import (
    DocumentProcessingAgent, FraudDetectionAgent,
    ComplianceAgent, DecisionOrchestratorAgent,
)
from ledger.commands.handlers import (
    handle_submit_application, handle_human_review_completed,
    SubmitApplicationCommand, HumanReviewCompletedCommand,
)


APP_ID = "NARR05-COMP068"
APPLICANT_ID = "COMP-068"


def _print_event_log(store: InMemoryEventStore, app_id: str) -> None:
    """Print a structured event log to stdout."""
    all_events = sorted(store._global, key=lambda e: e["global_position"])
    print(f"\n{'='*60}")
    print(f"EVENT LOG — application {app_id}")
    print(f"{'='*60}")
    for ev in all_events:
        print(
            f"  pos={ev['global_position']:04d}  "
            f"stream={ev['stream_id']:45s}  "
            f"type={ev['event_type']}"
        )
    print(f"{'='*60}\n")


def _build_regulatory_package(store: InMemoryEventStore, app_id: str) -> dict:
    """Build a regulatory examination package from all streams for the application."""
    streams = [
        f"loan-{app_id}",
        f"docpkg-{app_id}",
        f"credit-{app_id}",
        f"fraud-{app_id}",
        f"compliance-{app_id}",
    ]
    package: dict = {
        "package_version": "1.0",
        "application_id": app_id,
        "generated_at": datetime.now().isoformat(),
        "streams": {},
    }
    for stream_id in streams:
        events = store._streams.get(stream_id, [])
        package["streams"][stream_id] = [
            {
                "event_id": str(e["event_id"]),
                "stream_position": e["stream_position"],
                "event_type": e["event_type"],
                "event_version": e["event_version"],
                "payload": e["payload"],
                "recorded_at": str(e.get("recorded_at", "")),
            }
            for e in events
        ]

    # Summary
    loan_events = store._streams.get(f"loan-{app_id}", [])
    decision = next(
        (e for e in reversed(loan_events) if e["event_type"] == "DecisionGenerated"), None
    )
    approved = next(
        (e for e in reversed(loan_events) if e["event_type"] == "ApplicationApproved"), None
    )
    compliance_events = store._streams.get(f"compliance-{app_id}", [])
    compliance_completed = next(
        (e for e in reversed(compliance_events) if e["event_type"] == "ComplianceCheckCompleted"),
        None,
    )

    package["summary"] = {
        "initial_recommendation": decision["payload"].get("recommendation") if decision else None,
        "final_outcome": "APPROVED" if approved else "PENDING",
        "approved_amount_usd": approved["payload"].get("approved_amount_usd") if approved else None,
        "compliance_verdict": compliance_completed["payload"].get("overall_verdict") if compliance_completed else None,
        "total_events": len(store._global),
    }
    return package


async def _make_mock_registry():
    """Build a mock registry that passes all compliance rules."""
    from unittest.mock import AsyncMock, MagicMock
    registry = MagicMock()
    profile = MagicMock()
    profile.company_id = APPLICANT_ID
    profile.name = "Meridian Technologies Inc."
    profile.industry = "technology"
    profile.jurisdiction = "TX"
    profile.legal_type = "Corp"
    profile.founded_year = 2014
    profile.trajectory = "GROWTH"
    profile.risk_segment = "MEDIUM"
    registry.get_company = AsyncMock(return_value=profile)
    registry.get_compliance_flags = AsyncMock(return_value=[])
    fin = MagicMock()
    fin.fiscal_year = 2023
    fin.total_revenue = 2_500_000.0
    fin.ebitda = 600_000.0
    fin.net_income = 300_000.0
    fin.total_assets = 4_000_000.0
    fin.total_liabilities = 1_200_000.0
    registry.get_financial_history = AsyncMock(return_value=[fin])
    registry.get_loan_relationships = AsyncMock(return_value=[])
    return registry


async def _seed_loan_stream(store, app_id: str) -> None:
    """Seed ApplicationSubmitted + DocumentUploaded events."""
    dummy_pdf = str(Path(__file__).parent.parent / "data" / "applicant_profiles.json")

    submitted = {
        "event_type": "ApplicationSubmitted", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "applicant_id": APPLICANT_ID,
            "requested_amount_usd": "750000",
            "loan_purpose": "working_capital",
            "loan_term_months": 60,
            "submission_channel": "online",
            "contact_email": "cfo@meridian.tech",
            "contact_name": "Maria Chen",
            "application_reference": f"REF-{app_id}",
            "submitted_at": datetime.now().isoformat(),
        },
    }
    await store.append(f"loan-{app_id}", [submitted], expected_version=-1)

    for doc_type in ["income_statement", "balance_sheet", "application_proposal"]:
        uploaded = {
            "event_type": "DocumentUploaded", "event_version": 1,
            "payload": {
                "application_id": app_id,
                "document_id": f"doc-{doc_type}-{app_id}",
                "document_type": doc_type,
                "document_format": "pdf",
                "filename": f"{doc_type}.pdf",
                "file_path": dummy_pdf,
                "file_size_bytes": 4096,
                "file_hash": f"hash-{doc_type}",
                "fiscal_year": 2024,
                "uploaded_at": datetime.now().isoformat(),
                "uploaded_by": APPLICANT_ID,
            },
        }
        ver = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [uploaded], expected_version=ver)


async def main() -> None:
    t_start = time.time()
    print(f"[NARR-05] Starting demo for application {APP_ID}")

    store = InMemoryEventStore()
    registry = await _make_mock_registry()

    api_key = os.getenv("ANTHROPIC_API_KEY")
    try:
        client = AsyncAnthropic(api_key=api_key) if api_key else None
    except Exception:
        client = None

    # If no API key, use mock responses
    if not client:
        print("[NARR-05] No ANTHROPIC_API_KEY — using mock LLM responses")
        from unittest.mock import AsyncMock, MagicMock, patch

        def _make_mock_client(response_dict: dict) -> MagicMock:
            c = MagicMock()
            msg = MagicMock()
            msg.content = [MagicMock(text=json.dumps(response_dict))]
            msg.usage = MagicMock(input_tokens=100, output_tokens=50)
            c.messages = MagicMock()
            c.messages.create = AsyncMock(return_value=msg)
            return c

        client = _make_mock_client({"overall_confidence": 0.8, "is_coherent": True,
                                     "anomalies": [], "critical_missing_fields": [],
                                     "reextraction_recommended": False, "auditor_notes": "OK"})

    # Step 1: Seed the loan stream
    print("[NARR-05] Step 1: Seeding loan stream...")
    await _seed_loan_stream(store, APP_ID)

    # Seed docpkg stream with extraction results (bypassing actual PDF extraction)
    print("[NARR-05] Step 2: Seeding document extraction results...")
    for doc_type in ["income_statement", "balance_sheet"]:
        from ledger.schema.events import ExtractionStarted, ExtractionCompleted, FinancialFacts, DocumentType
        start = ExtractionStarted(
            package_id=f"docpkg-{APP_ID}",
            document_id=f"doc-{doc_type}-{APP_ID}",
            document_type=DocumentType(doc_type),
            pipeline_version="papermind-1.0",
            extraction_model="fast_text",
            started_at=datetime.now(),
        ).to_store_dict()
        ver = await store.stream_version(f"docpkg-{APP_ID}")
        await store.append(f"docpkg-{APP_ID}", [start], expected_version=ver)

        facts = FinancialFacts(
            total_revenue=Decimal("2500000"),
            gross_profit=Decimal("1000000"),
            operating_income=Decimal("625000"),
            ebitda=Decimal("600000"),
            net_income=Decimal("300000"),
            total_assets=Decimal("4000000"),
            total_liabilities=Decimal("1200000"),
            total_equity=Decimal("2800000"),
            field_confidence={"total_revenue": 0.92, "ebitda": 0.88},
            extraction_notes=[],
        )
        complete = ExtractionCompleted(
            package_id=f"docpkg-{APP_ID}",
            document_id=f"doc-{doc_type}-{APP_ID}",
            document_type=DocumentType(doc_type),
            facts=facts,
            raw_text_length=5000,
            tables_extracted=3,
            processing_ms=800,
            completed_at=datetime.now(),
        ).to_store_dict()
        ver = await store.stream_version(f"docpkg-{APP_ID}")
        await store.append(f"docpkg-{APP_ID}", [complete], expected_version=ver)

    # Seed FraudScreeningRequested trigger
    ver = await store.stream_version(f"loan-{APP_ID}")
    await store.append(f"loan-{APP_ID}", [{
        "event_type": "FraudScreeningRequested", "event_version": 1,
        "payload": {
            "application_id": APP_ID,
            "requested_at": datetime.now().isoformat(),
            "triggered_by_event_id": "seed",
        },
    }], expected_version=ver)

    # Step 3: Run CreditAnalysisAgent
    print("[NARR-05] Step 3: Running CreditAnalysisAgent...")
    credit_agent = CreditAnalysisAgent(
        agent_id="credit-narr05", agent_type="credit_analysis",
        store=store, registry=registry, client=client,
    )
    await credit_agent.process_application(APP_ID)
    print(f"  Credit agent session: {credit_agent.session_id}")

    # Step 4: Run FraudDetectionAgent
    print("[NARR-05] Step 4: Running FraudDetectionAgent...")
    fraud_agent = FraudDetectionAgent(
        agent_id="fraud-narr05", agent_type="fraud_detection",
        store=store, registry=registry, client=client,
    )
    with patch_llm_if_needed(fraud_agent):
        await fraud_agent.process_application(APP_ID)
    print(f"  Fraud agent session: {fraud_agent.session_id}")

    # Step 5: Run ComplianceAgent
    print("[NARR-05] Step 5: Running ComplianceAgent...")
    compliance_agent = ComplianceAgent(
        agent_id="compliance-narr05", agent_type="compliance",
        store=store, registry=registry, client=client,
    )
    await compliance_agent.process_application(APP_ID)
    print(f"  Compliance agent session: {compliance_agent.session_id}")

    # Step 6: Run DecisionOrchestratorAgent
    print("[NARR-05] Step 6: Running DecisionOrchestratorAgent...")
    orch_agent = DecisionOrchestratorAgent(
        agent_id="orchestrator-narr05", agent_type="decision_orchestrator",
        store=store, registry=registry, client=client,
    )
    with patch_llm_if_needed(orch_agent):
        await orch_agent.process_application(APP_ID)
    print(f"  Orchestrator session: {orch_agent.session_id}")

    # Step 7: Human override
    print("[NARR-05] Step 7: Human override by LO-Sarah-Chen...")
    loan_events = await store.load_stream(f"loan-{APP_ID}")
    decision_ev = next(
        (e for e in reversed(loan_events) if e["event_type"] == "DecisionGenerated"), None
    )
    if decision_ev:
        print(f"  Initial recommendation: {decision_ev['payload']['recommendation']}")

    review_result = await handle_human_review_completed(
        HumanReviewCompletedCommand(
            application_id=APP_ID,
            reviewer_id="LO-Sarah-Chen",
            override=True,
            original_recommendation=decision_ev["payload"]["recommendation"] if decision_ev else "REFER",
            final_decision="APPROVE",
            override_reason="Strong management team and sector growth justify exception approval.",
        ),
        store,
    )

    # Append ApplicationApproved
    ver = await store.stream_version(f"loan-{APP_ID}")
    await store.append(f"loan-{APP_ID}", [{
        "event_type": "ApplicationApproved", "event_version": 1,
        "payload": {
            "application_id": APP_ID,
            "approved_amount_usd": "750000",
            "interest_rate_pct": 8.5,
            "term_months": 60,
            "conditions": [
                "Quarterly financial reporting required",
                "Personal guarantee required",
            ],
            "approved_by": "LO-Sarah-Chen",
            "effective_date": datetime.now().strftime("%Y-%m-%d"),
            "approved_at": datetime.now().isoformat(),
        },
    }], expected_version=ver)
    print("  ApplicationApproved appended with approved_amount_usd=750000")

    # Print event log
    _print_event_log(store, APP_ID)

    # Step 8: Generate regulatory package
    print("[NARR-05] Step 8: Generating regulatory package...")
    artifacts_dir = Path(__file__).parent.parent / "artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    package = _build_regulatory_package(store, APP_ID)
    output_path = artifacts_dir / "regulatory_package_NARR05.json"
    output_path.write_text(json.dumps(package, indent=2, default=str))
    print(f"  Regulatory package written to {output_path}")

    elapsed = time.time() - t_start
    print(f"\nNARR-05 complete in {elapsed:.1f}s")
    return elapsed


def patch_llm_if_needed(agent):
    """Context manager that patches _call_llm if no API key is set."""
    import os
    from unittest.mock import patch as _patch, AsyncMock
    if os.getenv("ANTHROPIC_API_KEY") or os.getenv("GEMINI_API_KEY"):
        class _Noop:
            def __enter__(self): return self
            def __exit__(self, *a): pass
        return _Noop()

    fraud_resp = json.dumps({
        "fraud_score": 0.08, "anomalies": [],
        "recommendation": "PROCEED",
    })
    orch_resp = json.dumps({
        "recommendation": "DECLINE",
        "confidence": 0.55,
        "approved_amount_usd": None,
        "conditions": [],
        "executive_summary": "High risk tier with below-threshold confidence.",
        "key_risks": ["confidence below threshold"],
        "contributing_sessions": [],
    })
    text = fraud_resp if hasattr(agent, "_node_analyze") else orch_resp
    return _patch.object(type(agent), "_call_llm", return_value=(text, 100, 50, 0.001))


if __name__ == "__main__":
    elapsed = asyncio.run(main())
    if elapsed > 90:
        print(f"WARNING: Demo took {elapsed:.1f}s (limit: 90s)")
        sys.exit(1)
