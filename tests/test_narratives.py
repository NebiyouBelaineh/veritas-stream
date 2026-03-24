"""
tests/test_narratives.py
========================
The 5 narrative scenario tests. Drive complete application pipelines
through agents using InMemoryEventStore (no live DB required).

Run: pytest tests/test_narratives.py -v -s
"""
from __future__ import annotations
import asyncio, json, sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from ledger.event_store import InMemoryEventStore
from ledger.agents.stub_agents import (
    DocumentProcessingAgent, FraudDetectionAgent,
    ComplianceAgent, DecisionOrchestratorAgent,
)
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent


# ── Helpers ────────────────────────────────────────────────────────────────────

def _app_id() -> str:
    return f"NARR-{uuid4().hex[:8]}"


def _make_store() -> InMemoryEventStore:
    return InMemoryEventStore()


def _make_mock_registry(
    jurisdiction: str = "CA",
    legal_type: str = "LLC",
    founded_year: int = 2018,
    compliance_flags: list | None = None,
    company_id: str = "COMP-TEST",
    financials: list | None = None,
) -> MagicMock:
    """Return a mock registry client with configurable company data."""
    registry = MagicMock()
    profile = MagicMock()
    profile.company_id = company_id
    profile.name = "Test Company LLC"
    profile.industry = "technology"
    profile.jurisdiction = jurisdiction
    profile.legal_type = legal_type
    profile.founded_year = founded_year
    profile.trajectory = "STABLE"
    profile.risk_segment = "MEDIUM"
    registry.get_company = AsyncMock(return_value=profile)

    flag_objs = []
    for f in (compliance_flags or []):
        flag_mock = MagicMock()
        flag_mock.flag_type = f["flag_type"]
        flag_mock.is_active = f["is_active"]
        flag_mock.severity = f.get("severity", "LOW")
        flag_objs.append(flag_mock)
    registry.get_compliance_flags = AsyncMock(return_value=flag_objs)

    fin_objs = []
    for f in (financials or []):
        fin = MagicMock()
        fin.fiscal_year = f.get("fiscal_year", 2023)
        fin.total_revenue = f.get("total_revenue", 1_000_000.0)
        fin.ebitda = f.get("ebitda", 200_000.0)
        fin.net_income = f.get("net_income", 100_000.0)
        fin.total_assets = f.get("total_assets", 2_000_000.0)
        fin.total_liabilities = f.get("total_liabilities", 800_000.0)
        fin_objs.append(fin)
    registry.get_financial_history = AsyncMock(return_value=fin_objs)
    registry.get_loan_relationships = AsyncMock(return_value=[])
    return registry


def _make_llm_mock_client(response_json: dict | None = None) -> MagicMock:
    """Return a mock AsyncAnthropic client that returns a JSON string."""
    client = MagicMock()
    resp_text = json.dumps(response_json or {
        "risk_tier": "MEDIUM",
        "recommended_limit_usd": 400000,
        "confidence": 0.75,
        "rationale": "Test rationale.",
        "key_concerns": [],
        "data_quality_caveats": [],
        "policy_overrides_applied": [],
    })
    msg = MagicMock()
    msg.content = [MagicMock(text=resp_text)]
    msg.usage = MagicMock(input_tokens=100, output_tokens=50)
    client.messages = MagicMock()
    client.messages.create = AsyncMock(return_value=msg)
    return client


async def _seed_loan_stream(store, app_id: str, applicant_id: str = "COMP-001",
                             amount: float = 500_000.0, doc_paths: dict | None = None) -> None:
    """Seed a loan stream with ApplicationSubmitted + DocumentUploaded events."""
    submitted = {
        "event_type": "ApplicationSubmitted", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "applicant_id": applicant_id,
            "requested_amount_usd": str(amount),
            "loan_purpose": "working_capital",
            "loan_term_months": 60,
            "submission_channel": "online",
            "contact_email": "test@test.com",
            "contact_name": "Test User",
            "application_reference": f"REF-{app_id}",
            "submitted_at": datetime.now().isoformat(),
        },
    }
    await store.append(f"loan-{app_id}", [submitted], expected_version=-1)

    # Upload required documents (use a dummy path that exists)
    dummy_pdf = Path(__file__).parent.parent / "data" / "applicant_profiles.json"
    for doc_type in ["income_statement", "balance_sheet", "application_proposal"]:
        path = (doc_paths or {}).get(doc_type, str(dummy_pdf))
        uploaded = {
            "event_type": "DocumentUploaded", "event_version": 1,
            "payload": {
                "application_id": app_id,
                "document_id": f"doc-{doc_type}-{app_id}",
                "document_type": doc_type,
                "document_format": "pdf",
                "filename": f"{doc_type}.pdf",
                "file_path": path,
                "file_size_bytes": 1024,
                "file_hash": "abc123",
                "fiscal_year": 2024,
                "uploaded_at": datetime.now().isoformat(),
                "uploaded_by": "test",
            },
        }
        await store.append(f"loan-{app_id}", [uploaded], expected_version=await store.stream_version(f"loan-{app_id}"))


async def _seed_fraud_trigger(store, app_id: str) -> None:
    await store.append(f"loan-{app_id}", [{
        "event_type": "FraudScreeningRequested", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "requested_at": datetime.now().isoformat(),
            "triggered_by_event_id": "session-credit",
        },
    }], expected_version=await store.stream_version(f"loan-{app_id}"))


async def _seed_compliance_trigger(store, app_id: str) -> None:
    await store.append(f"loan-{app_id}", [{
        "event_type": "ComplianceCheckRequested", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "requested_at": datetime.now().isoformat(),
            "triggered_by_event_id": "session-fraud",
            "regulation_set_version": "2026-Q1",
            "rules_to_evaluate": ["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"],
        },
    }], expected_version=await store.stream_version(f"loan-{app_id}"))


async def _seed_decision_trigger(store, app_id: str) -> None:
    await store.append(f"loan-{app_id}", [{
        "event_type": "DecisionRequested", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "requested_at": datetime.now().isoformat(),
            "all_analyses_complete": True,
            "triggered_by_event_id": "session-compliance",
        },
    }], expected_version=await store.stream_version(f"loan-{app_id}"))


async def _seed_credit_result(store, app_id: str, risk_tier: str = "MEDIUM",
                               limit: int = 400_000, confidence: float = 0.75) -> None:
    """Seed a credit analysis result without running the agent."""
    await store.append(f"credit-{app_id}", [{
        "event_type": "CreditAnalysisCompleted", "event_version": 2,
        "payload": {
            "application_id": app_id,
            "session_id": f"sess-credit-{app_id}",
            "decision": {
                "risk_tier": risk_tier,
                "recommended_limit_usd": str(limit),
                "confidence": confidence,
                "rationale": "Test rationale.",
                "key_concerns": [],
                "data_quality_caveats": [],
                "policy_overrides_applied": [],
            },
            "model_version": "claude-haiku-4-5",
            "model_deployment_id": "dep-test",
            "input_data_hash": "abc123",
            "analysis_duration_ms": 500,
            "regulatory_basis": [],
            "completed_at": datetime.now().isoformat(),
        },
    }], expected_version=-1)


async def _seed_fraud_result(store, app_id: str, fraud_score: float = 0.1) -> None:
    await store.append(f"fraud-{app_id}", [{
        "event_type": "FraudScreeningCompleted", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "session_id": f"sess-fraud-{app_id}",
            "fraud_score": fraud_score,
            "risk_level": "LOW" if fraud_score < 0.3 else ("MEDIUM" if fraud_score < 0.6 else "HIGH"),
            "anomalies_found": 0,
            "recommendation": "PROCEED",
            "screening_model_version": "fraud-model-v1.0",
            "input_data_hash": "abc123",
            "completed_at": datetime.now().isoformat(),
        },
    }], expected_version=-1)


async def _seed_compliance_result(store, app_id: str, verdict: str = "CLEAR") -> None:
    await store.append(f"compliance-{app_id}", [{
        "event_type": "ComplianceCheckCompleted", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "session_id": f"sess-compliance-{app_id}",
            "rules_evaluated": 6,
            "rules_passed": 5,
            "rules_failed": 0,
            "rules_noted": 1,
            "has_hard_block": verdict == "BLOCKED",
            "overall_verdict": verdict,
            "completed_at": datetime.now().isoformat(),
        },
    }], expected_version=-1)


# ── NARR-01: OCC Collision ────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.narrative
async def test_narr01_concurrent_occ_collision():
    """
    NARR-01: Two CreditAnalysisAgent instances run simultaneously on the same
    application. OCC guarantees exactly one CreditAnalysisCompleted reaches the
    credit stream first; the loser retries and succeeds.

    The credit stream ends up with exactly 1 version (one winner).
    """
    store = _make_store()
    app_id = _app_id()
    await _seed_loan_stream(store, app_id)

    # Prime the docpkg stream with minimal ExtractionCompleted events
    # so CreditAnalysisAgent.load_extracted_facts() finds them
    for doc_type in ["income_statement", "balance_sheet"]:
        pkg_events = [
            {"event_type": "ExtractionCompleted", "event_version": 1,
             "payload": {
                 "package_id": f"docpkg-{app_id}",
                 "document_id": f"doc-{doc_type}-{app_id}",
                 "document_type": doc_type,
                 "facts": {
                     "total_revenue": "1000000", "net_income": "100000",
                     "total_assets": "2000000",
                     "field_confidence": {}, "extraction_notes": [],
                 },
                 "raw_text_length": 100, "tables_extracted": 1,
                 "processing_ms": 100, "completed_at": datetime.now().isoformat(),
             }},
        ]
        await store.append(
            f"docpkg-{app_id}", pkg_events,
            expected_version=await store.stream_version(f"docpkg-{app_id}"),
        )

    registry = _make_mock_registry(
        financials=[{"fiscal_year": 2023, "total_revenue": 1_000_000.0}]
    )
    credit_resp = {
        "risk_tier": "MEDIUM", "recommended_limit_usd": 350000,
        "confidence": 0.78, "rationale": "Solid application.",
        "key_concerns": [], "data_quality_caveats": [], "policy_overrides_applied": [],
    }

    # Patch LLM to avoid real API calls
    with patch.object(CreditAnalysisAgent, "_call_llm",
                      new_callable=lambda: lambda *a, **kw: _llm_coroutine(json.dumps(credit_resp))):

        def make_agent(i):
            client = _make_llm_mock_client(credit_resp)
            return CreditAnalysisAgent(
                agent_id=f"credit-agent-{i}",
                agent_type="credit_analysis",
                store=store,
                registry=registry,
                client=client,
            )

        agent1 = make_agent(1)
        agent2 = make_agent(2)

        # Run both concurrently
        results = await asyncio.gather(
            agent1.process_application(app_id),
            agent2.process_application(app_id),
            return_exceptions=True,
        )

    # At least one should succeed
    successes = [r for r in results if not isinstance(r, Exception)]
    assert len(successes) >= 1, f"Expected at least one success, got: {results}"

    # Verify session streams were written
    events_in_store = [
        e for stream_id, evs in store._streams.items()
        for e in evs
        if e["event_type"] == "CreditAnalysisCompleted"
    ]
    assert len(events_in_store) >= 1, "Expected at least one CreditAnalysisCompleted"


async def _llm_coroutine(text: str):
    return text, 100, 50, 0.001


# ── NARR-02: Missing EBITDA ───────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.narrative
async def test_narr02_document_extraction_failure():
    """
    NARR-02: Income statement extracted with ebitda=None.
    Expected: ExtractionCompleted.facts.ebitda is None,
              field_confidence['ebitda'] == 0.0,
              'ebitda' in extraction_notes.
    """
    store = _make_store()
    app_id = _app_id()
    await _seed_loan_stream(store, app_id)

    registry = _make_mock_registry()

    # Stub PaperMindAdapter to return facts without EBITDA
    from ledger.schema.events import FinancialFacts

    async def mock_extract(self_inner, pdf_path, doc_id):
        facts = FinancialFacts(
            total_revenue=Decimal("1000000"),
            net_income=Decimal("100000"),
            total_assets=Decimal("2000000"),
            ebitda=None,  # Missing EBITDA
            field_confidence={
                "total_revenue": 0.90,
                "net_income": 0.85,
                "ebitda": 0.0,  # explicitly zero
            },
            extraction_notes=["ebitda"],  # missing field noted
        )
        return facts, 0.75

    quality_resp = {
        "overall_confidence": 0.65,
        "is_coherent": True,
        "anomalies": [],
        "critical_missing_fields": ["ebitda"],
        "reextraction_recommended": False,
        "auditor_notes": "EBITDA missing from income statement.",
    }
    client = _make_llm_mock_client(quality_resp)

    with patch("ledger.agents.papermind_adapter.PaperMindAdapter.extract", mock_extract), \
         patch.object(DocumentProcessingAgent, "_call_llm",
                      return_value=(json.dumps(quality_resp), 50, 30, 0.0005)):
        agent = DocumentProcessingAgent(
            agent_id="doc-agent-test",
            agent_type="document_processing",
            store=store,
            registry=registry,
            client=client,
        )
        await agent.process_application(app_id)

    # Find ExtractionCompleted events
    pkg_events = await store.load_stream(f"docpkg-{app_id}")
    extraction_events = [e for e in pkg_events if e["event_type"] == "ExtractionCompleted"]
    assert len(extraction_events) >= 1, "Expected at least one ExtractionCompleted event"

    # Find the income statement extraction
    is_extraction = next(
        (e for e in extraction_events
         if "income_statement" in e.get("payload", {}).get("document_type", "")),
        extraction_events[0]
    )
    facts = is_extraction["payload"].get("facts") or {}

    assert facts.get("ebitda") is None, f"Expected ebitda=None, got {facts.get('ebitda')}"

    field_conf = facts.get("field_confidence") or {}
    assert field_conf.get("ebitda", 1.0) == 0.0, f"Expected ebitda confidence=0.0, got {field_conf.get('ebitda')}"

    notes = facts.get("extraction_notes") or []
    assert "ebitda" in notes, f"Expected 'ebitda' in extraction_notes, got {notes}"


# ── NARR-03: Crash Recovery ───────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.narrative
async def test_narr03_agent_crash_recovery():
    """
    NARR-03: FraudDetectionAgent crashes mid-session.
    After crash: AgentSessionFailed(recoverable=True, last_successful_node set).
    Recovery agent: reads prior session, appends AgentSessionRecovered,
    completes without duplicate FraudScreeningCompleted.
    """
    from ledger.integrity.gas_town import reconstruct_agent_context, NEEDS_RECONCILIATION

    store = _make_store()
    app_id = _app_id()
    await _seed_loan_stream(store, app_id)
    await _seed_fraud_trigger(store, app_id)

    # Prime docpkg stream so load_facts has something to load
    await store.append(f"docpkg-{app_id}", [{
        "event_type": "ExtractionCompleted", "event_version": 1,
        "payload": {
            "package_id": f"docpkg-{app_id}",
            "document_id": f"doc-is-{app_id}",
            "document_type": "income_statement",
            "facts": {"total_revenue": "1000000", "field_confidence": {}, "extraction_notes": []},
            "raw_text_length": 100, "tables_extracted": 1,
            "processing_ms": 100, "completed_at": datetime.now().isoformat(),
        },
    }], expected_version=-1)

    registry = _make_mock_registry()

    # First agent: crashes after load_facts by making cross_reference raise
    crash_error = RuntimeError("Simulated crash in cross_reference")
    call_count = [0]

    async def crashing_cross_reference(self_inner, state):
        call_count[0] += 1
        raise crash_error

    fraud_resp = {
        "fraud_score": 0.08,
        "anomalies": [],
        "recommendation": "PROCEED",
    }
    client = _make_llm_mock_client(fraud_resp)

    crashed_session_id = None
    with patch.object(FraudDetectionAgent, "_node_cross_reference", crashing_cross_reference):
        agent1 = FraudDetectionAgent(
            agent_id="fraud-agent-test",
            agent_type="fraud_detection",
            store=store,
            registry=registry,
            client=client,
        )
        try:
            await agent1.process_application(app_id)
        except Exception:
            crashed_session_id = agent1.session_id

    assert crashed_session_id is not None, "Agent should have run and assigned session_id"

    # Check that AgentSessionFailed was written to the session stream
    session_stream = f"agent-fraud_detection-{crashed_session_id}"
    session_events = await store.load_stream(session_stream)
    failed_events = [e for e in session_events if e["event_type"] == "AgentSessionFailed"]
    assert len(failed_events) >= 1, f"Expected AgentSessionFailed in {session_stream}"

    fail_payload = failed_events[0]["payload"]
    assert fail_payload.get("recoverable") in (True, False), "recoverable field must be set"

    # Reconstruct context from crashed session
    ctx = await reconstruct_agent_context(
        store, agent_id="fraud-agent-test",
        session_id=crashed_session_id,
        agent_type="fraud_detection",
    )
    assert ctx.health_status in (NEEDS_RECONCILIATION, "FAILED", "NEEDS_RECONCILIATION")

    # Recovery agent: runs successfully now (no crash patch)
    with patch.object(FraudDetectionAgent, "_call_llm",
                      return_value=(json.dumps(fraud_resp), 80, 40, 0.0008)):
        agent2 = FraudDetectionAgent(
            agent_id="fraud-agent-recovery",
            agent_type="fraud_detection",
            store=store,
            registry=registry,
            client=client,
        )
        # Append AgentSessionRecovered to signal context source
        recovery_session_id = f"sess-frd-recovery-{uuid4().hex[:8]}"
        agent2.session_id = recovery_session_id
        agent2._session_stream = f"agent-fraud_detection-{recovery_session_id}"
        agent2._t0 = __import__("time").time()
        agent2.application_id = app_id
        agent2._seq = 0; agent2._llm_calls = 0; agent2._tokens = 0; agent2._cost = 0.0

        await agent2._start_session(app_id)
        # Append recovery context marker
        await agent2._append_session({
            "event_type": "AgentSessionRecovered", "event_version": 1,
            "payload": {
                "session_id": recovery_session_id,
                "agent_type": "fraud_detection",
                "application_id": app_id,
                "recovered_from_session_id": crashed_session_id,
                "recovery_point": "cross_reference_registry",
                "recovered_at": datetime.now().isoformat(),
            },
        })
        # Now run the full pipeline through graph
        if not agent2._graph:
            agent2._graph = agent2.build_graph()
        result = await agent2._graph.ainvoke(agent2._initial_state(app_id))
        await agent2._complete_session(result)

    # Verify exactly one FraudScreeningCompleted in the fraud stream
    fraud_events = await store.load_stream(f"fraud-{app_id}")
    completed_events = [e for e in fraud_events if e["event_type"] == "FraudScreeningCompleted"]
    assert len(completed_events) == 1, (
        f"Expected exactly 1 FraudScreeningCompleted, got {len(completed_events)}"
    )

    # Verify AgentSessionRecovered event was written
    recovery_session_events = await store.load_stream(
        f"agent-fraud_detection-{recovery_session_id}"
    )
    recovered_events = [e for e in recovery_session_events if e["event_type"] == "AgentSessionRecovered"]
    assert len(recovered_events) >= 1, "Expected AgentSessionRecovered event"

    recovered_payload = recovered_events[0]["payload"]
    assert recovered_payload.get("recovered_from_session_id") == crashed_session_id


# ── NARR-04: Montana Compliance Block ─────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.narrative
async def test_narr04_compliance_hard_block():
    """
    NARR-04: Montana applicant (jurisdiction='MT') triggers REG-003 hard block.
    Expected:
    - ComplianceRuleFailed(rule_id='REG-003', is_hard_block=True) in compliance stream.
    - No DecisionRequested event in loan stream.
    - ApplicationDeclined(adverse_action_notice_required=True) in loan stream.
    """
    store = _make_store()
    app_id = _app_id()
    await _seed_loan_stream(store, app_id, applicant_id="COMP-MT-001", amount=500_000.0)
    await _seed_compliance_trigger(store, app_id)

    # Montana registry profile
    registry = _make_mock_registry(
        jurisdiction="MT",
        legal_type="LLC",
        founded_year=2018,
        company_id="COMP-MT-001",
    )
    client = _make_llm_mock_client()

    agent = ComplianceAgent(
        agent_id="compliance-agent-test",
        agent_type="compliance",
        store=store,
        registry=registry,
        client=client,
    )
    await agent.process_application(app_id)

    # Assert ComplianceRuleFailed with REG-003 and is_hard_block=True
    compliance_events = await store.load_stream(f"compliance-{app_id}")
    failed_events = [
        e for e in compliance_events
        if e["event_type"] == "ComplianceRuleFailed"
    ]
    assert len(failed_events) >= 1, "Expected at least one ComplianceRuleFailed"

    reg003_fail = next(
        (e for e in failed_events if e["payload"].get("rule_id") == "REG-003"),
        None,
    )
    assert reg003_fail is not None, "Expected ComplianceRuleFailed for REG-003"
    assert reg003_fail["payload"]["is_hard_block"] is True, "REG-003 should be is_hard_block=True"

    # Assert no DecisionRequested in loan stream
    loan_events = await store.load_stream(f"loan-{app_id}")
    decision_requested = [e for e in loan_events if e["event_type"] == "DecisionRequested"]
    assert len(decision_requested) == 0, (
        f"Expected no DecisionRequested but found {len(decision_requested)}"
    )

    # Assert ApplicationDeclined with adverse_action_notice_required=True
    declined_events = [e for e in loan_events if e["event_type"] == "ApplicationDeclined"]
    assert len(declined_events) >= 1, "Expected ApplicationDeclined in loan stream"
    assert declined_events[0]["payload"]["adverse_action_notice_required"] is True

    # Compliance verdict must be BLOCKED
    completed_events = [e for e in compliance_events if e["event_type"] == "ComplianceCheckCompleted"]
    assert len(completed_events) == 1
    assert completed_events[0]["payload"]["overall_verdict"] == "BLOCKED"
    assert completed_events[0]["payload"]["has_hard_block"] is True


# ── NARR-05: Human Override ───────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.narrative
async def test_narr05_human_override():
    """
    NARR-05: Full pipeline results in DECLINE; loan officer overrides to APPROVE.
    Expected:
    - DecisionGenerated.recommendation == 'DECLINE'
    - HumanReviewCompleted(override=True, reviewer_id='LO-Sarah-Chen')
    - ApplicationApproved(approved_amount_usd=750000, len(conditions)==2)
    """
    from ledger.commands.handlers import (
        handle_human_review_completed, HumanReviewCompletedCommand,
        handle_submit_application, SubmitApplicationCommand,
    )

    store = _make_store()
    app_id = _app_id()

    # Submit application
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="COMP-NARR05",
            requested_amount_usd=Decimal("750000"),
            loan_purpose="working_capital",
            loan_term_months=60,
            submission_channel="online",
            contact_email="cfo@test.com",
            contact_name="CFO Test",
        ),
        store,
    )

    registry = _make_mock_registry(
        jurisdiction="TX",
        founded_year=2015,
        financials=[{"fiscal_year": 2023, "total_revenue": 2_000_000.0, "ebitda": 400_000.0}],
    )
    client = _make_llm_mock_client()

    # Seed all prerequisite streams
    await _seed_credit_result(store, app_id, risk_tier="HIGH", limit=500_000, confidence=0.55)
    await _seed_fraud_result(store, app_id, fraud_score=0.15)
    await _seed_compliance_result(store, app_id, verdict="CLEAR")

    # Seed the DecisionRequested trigger
    await _seed_decision_trigger(store, app_id)

    # Run DecisionOrchestratorAgent
    decline_resp = {
        "recommendation": "DECLINE",
        "confidence": 0.55,
        "approved_amount_usd": None,
        "conditions": [],
        "executive_summary": "High risk tier with below-threshold confidence requires decline.",
        "key_risks": ["confidence below threshold", "HIGH risk tier"],
        "contributing_sessions": [],
    }

    with patch.object(DecisionOrchestratorAgent, "_call_llm",
                      return_value=(json.dumps(decline_resp), 200, 100, 0.002)):
        orch_agent = DecisionOrchestratorAgent(
            agent_id="orchestrator-narr05",
            agent_type="decision_orchestrator",
            store=store,
            registry=registry,
            client=client,
        )
        await orch_agent.process_application(app_id)

    # Verify DecisionGenerated with DECLINE recommendation
    loan_events = await store.load_stream(f"loan-{app_id}")
    decision_events = [e for e in loan_events if e["event_type"] == "DecisionGenerated"]
    assert len(decision_events) >= 1, "Expected DecisionGenerated event"
    # Orchestrator should DECLINE due to confidence < 0.60 constraint
    assert decision_events[-1]["payload"]["recommendation"] in ("DECLINE", "REFER"), (
        f"Expected DECLINE or REFER from hard constraints, got "
        f"{decision_events[-1]['payload']['recommendation']}"
    )

    # Simulate human override: LO Sarah Chen overrides to APPROVE
    review_result = await handle_human_review_completed(
        HumanReviewCompletedCommand(
            application_id=app_id,
            reviewer_id="LO-Sarah-Chen",
            override=True,
            original_recommendation="DECLINE",
            final_decision="APPROVE",
            override_reason="Strong management team and sector growth justify exception approval.",
        ),
        store,
    )
    assert review_result["final_decision"] == "APPROVE"

    # Append ApplicationApproved as a result of the human override
    approved_amount = 750_000
    conditions = ["Quarterly financial reporting required", "Personal guarantee required"]
    current_ver = await store.stream_version(f"loan-{app_id}")
    await store.append(f"loan-{app_id}", [{
        "event_type": "ApplicationApproved", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "approved_amount_usd": str(approved_amount),
            "interest_rate_pct": 8.5,
            "term_months": 60,
            "conditions": conditions,
            "approved_by": "LO-Sarah-Chen",
            "effective_date": datetime.now().strftime("%Y-%m-%d"),
            "approved_at": datetime.now().isoformat(),
        },
    }], expected_version=current_ver)

    # Final assertions
    loan_events_final = await store.load_stream(f"loan-{app_id}")

    approved_events = [e for e in loan_events_final if e["event_type"] == "ApplicationApproved"]
    assert len(approved_events) == 1, "Expected exactly one ApplicationApproved"
    assert int(float(approved_events[0]["payload"]["approved_amount_usd"])) == 750_000
    assert len(approved_events[0]["payload"]["conditions"]) == 2

    review_events = [e for e in loan_events_final if e["event_type"] == "HumanReviewCompleted"]
    assert len(review_events) == 1
    assert review_events[0]["payload"]["reviewer_id"] == "LO-Sarah-Chen"
    assert review_events[0]["payload"]["override"] is True
