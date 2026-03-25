"""
api/services/pipeline_service.py — Pipeline execution service.

Wraps the agent invocation pattern from scripts/run_pipeline.py as an
importable async function. Reuses _seed_streams, _JsonRegistryAdapter,
and _patch_llm_if_needed directly from the script to avoid duplication.
"""
from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Ensure scripts/ is importable
_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

# Reuse helpers from run_pipeline.py
from run_pipeline import (
    _seed_streams,
    _JsonRegistryAdapter,
    _patch_llm_if_needed,
    DOCUMENTS_DIR,
    DATA_DIR,
)

from ledger.agents.credit_analysis_agent import CreditAnalysisAgent
from ledger.agents.stub_agents import (
    DocumentProcessingAgent, FraudDetectionAgent,
    ComplianceAgent, DecisionOrchestratorAgent,
)


AGENT_MAP = {
    "document":   (DocumentProcessingAgent,  "document_processing"),
    "credit":     (CreditAnalysisAgent,       "credit_analysis"),
    "fraud":      (FraudDetectionAgent,       "fraud_detection"),
    "compliance": (ComplianceAgent,           "compliance"),
    "decision":   (DecisionOrchestratorAgent, "decision_orchestrator"),
}

PHASES_FOR = {
    "all":        ["document", "credit", "fraud", "compliance", "decision"],
    "document":   ["document"],
    "credit":     ["credit"],
    "fraud":      ["fraud"],
    "compliance": ["compliance"],
    "decision":   ["decision"],
}


@dataclass
class AgentTiming:
    agent_name: str
    phase: str
    elapsed_s: float
    session_id: str | None = None


@dataclass
class PipelineResult:
    application_id: str
    phase: str
    timings: list[AgentTiming]
    total_elapsed_s: float
    outcome: str | None = None
    approved_amount: str | None = None
    risk_tier: str | None = None
    fraud_score: Any = None
    compliance_verdict: str | None = None


async def _ensure_document_events(store, app_id: str, registry) -> None:
    """
    If the loan stream has no DocumentUploaded events, append them now using
    the applicant_id from ApplicationSubmitted.  This handles apps created via
    the UI where _seed_streams was not called at submission time.
    """
    try:
        loan_events = await store.load_stream(f"loan-{app_id}")
    except Exception:
        return

    has_docs = any(e.get("event_type") == "DocumentUploaded" for e in loan_events)
    if has_docs:
        return

    # Find applicant_id from ApplicationSubmitted
    applicant_id = None
    for e in loan_events:
        if e.get("event_type") == "ApplicationSubmitted":
            applicant_id = e.get("payload", {}).get("applicant_id")
            break

    if not applicant_id:
        return

    # Only seed docs if the folder actually exists on disk
    doc_dir = DOCUMENTS_DIR / applicant_id
    if not doc_dir.exists():
        return

    from datetime import datetime
    now = datetime.now()
    doc_files = {
        "income_statement": "income_statement_2024.pdf",
        "balance_sheet": "balance_sheet_2024.pdf",
        "application_proposal": "application_proposal.pdf",
    }
    for doc_type, filename in doc_files.items():
        file_path = str(doc_dir / filename)
        if not (doc_dir / filename).exists():
            continue
        uploaded = {
            "event_type": "DocumentUploaded", "event_version": 1,
            "payload": {
                "application_id": app_id,
                "document_id": f"doc-{doc_type}-{app_id}",
                "document_type": doc_type,
                "document_format": "pdf",
                "filename": filename,
                "file_path": file_path,
                "file_size_bytes": (doc_dir / filename).stat().st_size,
                "file_hash": f"sha256-{doc_type}-{app_id}",
                "fiscal_year": 2024,
                "uploaded_at": now.isoformat(),
                "uploaded_by": applicant_id,
            },
        }
        ver = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [uploaded], expected_version=ver)


_PHASE_COMPLETION: dict[str, tuple[str, str]] = {
    # phase → (stream prefix, completion event type)
    "document":   ("loan",       "DocumentProcessingCompleted"),
    "credit":     ("credit",     "CreditAnalysisCompleted"),
    "fraud":      ("fraud",      "FraudScreeningCompleted"),
    "compliance": ("compliance", "ComplianceCheckCompleted"),
    "decision":   ("loan",       "DecisionGenerated"),
}


async def _phase_already_done(store, app_id: str, phase: str) -> bool:
    """
    Return True if the phase should be skipped:
    - completion event present (successfully finished), OR
    - output stream already exists with events (partially ran; re-running would
      OCC because the agent opens with expected_version=-1).
    """
    prefix, event_type = _PHASE_COMPLETION.get(phase, ("", ""))
    if not prefix:
        return False
    # document phase uses the loan stream which always has events — check only
    # for the specific completion event, not mere stream existence.
    if phase == "document":
        try:
            events = await store.load_stream(f"loan-{app_id}")
            return any(e.get("event_type") == event_type for e in events)
        except Exception:
            return False
    try:
        version = await store.stream_version(f"{prefix}-{app_id}")
        return version > 0
    except Exception:
        return False


async def run_pipeline(
    app_id: str,
    phase: str,
    store,
    registry,
    seed_streams: bool = False,
) -> PipelineResult:
    """
    Run the specified pipeline phase(s) for app_id.

    Parameters
    ----------
    app_id:        Application ID (e.g. "APP-978CE55A" or legacy "COMP-001")
    phase:         "all" | "document" | "credit" | "fraud" | "compliance" | "decision"
    store:         EventStore or InstrumentedEventStore
    registry:      ApplicantRegistryClient or _JsonRegistryAdapter
    seed_streams:  If True, seed loan + docpkg streams first (for datagen-style apps)
    """
    t_start = time.perf_counter()

    if seed_streams:
        profile = await registry.get_company(app_id)
        await _seed_streams(store, app_id, app_id, profile)
    else:
        # For UI-created apps: append DocumentUploaded events if missing
        await _ensure_document_events(store, app_id, registry)

    phases = PHASES_FOR.get(phase, ["credit"])
    timings: list[AgentTiming] = []

    for p in phases:
        if await _phase_already_done(store, app_id, p):
            continue  # skip phases that already completed successfully

        cls, atype = AGENT_MAP[p]
        agent = cls(
            agent_id=f"{atype}-{app_id}",
            agent_type=atype,
            store=store,
            registry=registry,
        )
        t0 = time.perf_counter()
        with _patch_llm_if_needed(agent):
            await agent.process_application(app_id)
        elapsed = time.perf_counter() - t0
        timings.append(AgentTiming(
            agent_name=cls.__name__,
            phase=p,
            elapsed_s=elapsed,
            session_id=getattr(agent, "session_id", None),
        ))

    total_elapsed = time.perf_counter() - t_start

    # Extract outcome from store (works for both InMemory and Postgres)
    outcome = await _extract_outcome(store, app_id)

    return PipelineResult(
        application_id=app_id,
        phase=phase,
        timings=timings,
        total_elapsed_s=total_elapsed,
        **outcome,
    )


async def _extract_outcome(store, app_id: str) -> dict:
    """Read final decision info from the loan stream."""
    try:
        events = await store.load_stream(f"loan-{app_id}")
    except Exception:
        return {}

    result: dict = {}
    for e in reversed(events):
        etype = e.get("event_type", "")
        payload = e.get("payload", {})
        if etype == "ApplicationApproved" and "outcome" not in result:
            result["outcome"] = "APPROVED"
            result["approved_amount"] = str(payload.get("approved_amount_usd", ""))
        elif etype == "ApplicationDeclined" and "outcome" not in result:
            result["outcome"] = "DECLINED"
        elif etype == "DecisionGenerated" and "outcome" not in result:
            result["outcome"] = payload.get("recommendation", "UNKNOWN")

    try:
        credit_events = await store.load_stream(f"credit-{app_id}")
        for e in reversed(credit_events):
            if e.get("event_type") == "CreditAnalysisCompleted":
                decision = e.get("payload", {}).get("decision") or {}
                result["risk_tier"] = decision.get("risk_tier")
                break
    except Exception:
        pass

    try:
        fraud_events = await store.load_stream(f"fraud-{app_id}")
        for e in reversed(fraud_events):
            if e.get("event_type") == "FraudScreeningCompleted":
                result["fraud_score"] = e.get("payload", {}).get("fraud_score")
                break
    except Exception:
        pass

    try:
        compliance_events = await store.load_stream(f"compliance-{app_id}")
        for e in reversed(compliance_events):
            if e.get("event_type") == "ComplianceCheckCompleted":
                result["compliance_verdict"] = e.get("payload", {}).get("overall_verdict")
                break
    except Exception:
        pass

    return result
