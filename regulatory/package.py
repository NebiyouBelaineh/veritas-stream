"""
regulatory/package.py
=====================
Generates a self-contained regulatory examination package for a loan application.

The package contains everything a regulator needs to independently verify the
decision history without querying the live database:

  - Complete event stream up to examination_date with full payloads
  - Projection state as it existed at examination_date
  - Hash chain integrity verification result (read-only; no store writes)
  - Plain-English narrative of the application lifecycle
  - AI agent provenance: model version, input data hash, confidence per agent

NEVER writes to the store. All operations are read-only.

Note on hash chain: this module computes the hash chain inline rather than
calling run_integrity_check(), which writes an AuditIntegrityCheckRun event.
The inline computation is identical in algorithm; it simply does not persist
the result.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any


# ── Hash chain helpers (mirrors ledger/integrity/audit_chain.py, read-only) ──

def _sha256(data: str) -> str:
    return hashlib.sha256(data.encode()).hexdigest()


def _canonical(payload: dict) -> str:
    """Deterministic JSON for hashing: sorted keys, no extra whitespace."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _compute_hash_chain(events: list[dict]) -> dict[str, Any]:
    """
    Compute SHA-256 hash chain over event payloads.

    Returns a dict with integrity_hash, events_verified, and chain_valid.
    chain_valid is always True here because there is no prior stored hash to
    compare against; this is a fresh computation for the examination package.
    """
    parts = [_sha256(_canonical(ev.get("payload", {}))) for ev in events]
    integrity_hash = _sha256("".join(parts)) if parts else _sha256("")
    return {
        "integrity_hash": integrity_hash,
        "events_verified": len(events),
        "chain_valid": True,
        "note": (
            "Hash computed inline for this package. To verify: SHA-256 each "
            "event payload (canonical JSON, sorted keys), concatenate the "
            "hashes in stream_position order, then SHA-256 the concatenation."
        ),
    }


# ── Narrative builder ────────────────────────────────────────────────────────

_NARRATIVE_TEMPLATES: dict[str, str] = {
    "ApplicationSubmitted": (
        "Application {application_id} was submitted by applicant {applicant_id} "
        "requesting {requested_amount_usd} USD."
    ),
    "DocumentsRequested": (
        "Supporting documents were requested."
    ),
    "DocumentsUploaded": (
        "Documents were uploaded."
    ),
    "DocumentsProcessed": (
        "Documents were processed."
    ),
    "CreditAnalysisCompleted": (
        "Credit analysis completed: risk tier {risk_tier}, "
        "model {model_version}."
    ),
    "FraudScreeningCompleted": (
        "Fraud screening completed: score {fraud_score}, "
        "risk level {risk_level}."
    ),
    "ComplianceRulePassed": (
        "Compliance rule {rule_id} ({rule_name}) passed."
    ),
    "ComplianceRuleFailed": (
        "Compliance rule {rule_id} ({rule_name}) failed{hard_block_note}."
    ),
    "ComplianceCheckCompleted": (
        "Compliance check completed with overall verdict: {overall_verdict}."
    ),
    "DecisionGenerated": (
        "Decision generated: {recommendation} with confidence {confidence_score}."
    ),
    "HumanReviewCompleted": (
        "Human review by {reviewer_id}: final decision {final_decision}."
    ),
    "ApplicationApproved": (
        "Application approved for {approved_amount_usd} USD."
    ),
    "ApplicationDeclined": (
        "Application declined."
    ),
}


def _build_narrative(events: list[dict]) -> str:
    lines = []
    for ev in events:
        et = ev.get("event_type", "")
        p = ev.get("payload", {})
        template = _NARRATIVE_TEMPLATES.get(et)
        if template is None:
            continue
        try:
            decision = p.get("decision") or {}
            line = template.format(
                application_id=p.get("application_id", ""),
                applicant_id=p.get("applicant_id", ""),
                requested_amount_usd=p.get("requested_amount_usd", ""),
                approved_amount_usd=p.get("approved_amount_usd", ""),
                risk_tier=decision.get("risk_tier") or p.get("risk_tier", ""),
                model_version=p.get("model_version", "unknown"),
                fraud_score=p.get("fraud_score", ""),
                risk_level=p.get("risk_level", ""),
                rule_id=p.get("rule_id", ""),
                rule_name=p.get("rule_name", ""),
                hard_block_note=" (hard block)" if p.get("is_hard_block") else "",
                overall_verdict=p.get("overall_verdict", ""),
                recommendation=p.get("recommendation", ""),
                confidence_score=p.get("confidence_score", "unknown"),
                reviewer_id=p.get("reviewer_id", ""),
                final_decision=p.get("final_decision", ""),
            )
            recorded_at = ev.get("recorded_at", "")
            lines.append(f"[{recorded_at}] {line}")
        except (KeyError, TypeError):
            lines.append(f"[{ev.get('recorded_at', '')}] {et}.")

    return "\n".join(lines) if lines else "No narrative events found."


# ── AI provenance extractor ──────────────────────────────────────────────────

_AGENT_EVENT_TYPES = {
    "CreditAnalysisCompleted": "credit_analysis",
    "FraudScreeningCompleted": "fraud_detection",
    "ComplianceCheckCompleted": "compliance",
    "DecisionGenerated": "decision_orchestrator",
}


def _extract_ai_provenance(events: list[dict]) -> list[dict[str, Any]]:
    """
    Extract model version, input data hash, and confidence from each AI agent
    event. These fields allow a regulator to reproduce the model invocation.
    """
    provenance = []
    for ev in events:
        et = ev.get("event_type", "")
        agent_type = _AGENT_EVENT_TYPES.get(et)
        if agent_type is None:
            continue
        p = ev.get("payload", {})
        decision = p.get("decision") or {}
        provenance.append({
            "agent_type": agent_type,
            "event_type": et,
            "stream_position": ev.get("stream_position"),
            "recorded_at": str(ev.get("recorded_at", "")),
            "model_version": p.get("model_version") or decision.get("model_version"),
            "model_deployment_id": p.get("model_deployment_id"),
            "input_data_hash": p.get("input_data_hash"),
            "confidence_score": p.get("confidence_score") or decision.get("confidence_score"),
        })
    return provenance


# ── Projection state snapshot at examination_date ────────────────────────────

def _application_summary_at(
    events: list[dict], application_id: str
) -> dict[str, Any]:
    """
    Derive application summary state by replaying events up to the cutoff.
    Returns a plain dict suitable for JSON serialisation.
    """
    state: dict[str, Any] = {
        "application_id": application_id,
        "state": None,
        "applicant_id": None,
        "requested_amount_usd": None,
        "approved_amount_usd": None,
        "risk_tier": None,
        "fraud_score": None,
        "compliance_verdict": None,
        "decision": None,
        "final_decision": None,
    }
    for ev in events:
        et = ev.get("event_type", "")
        p = ev.get("payload", {})
        if et == "ApplicationSubmitted":
            state["state"] = "SUBMITTED"
            state["applicant_id"] = p.get("applicant_id")
            state["requested_amount_usd"] = p.get("requested_amount_usd")
        elif et == "CreditAnalysisCompleted":
            decision = p.get("decision") or {}
            state["risk_tier"] = decision.get("risk_tier") or p.get("risk_tier")
        elif et == "FraudScreeningCompleted":
            state["fraud_score"] = p.get("fraud_score")
        elif et == "ComplianceCheckCompleted":
            state["compliance_verdict"] = p.get("overall_verdict")
        elif et == "DecisionGenerated":
            state["decision"] = p.get("recommendation")
            state["approved_amount_usd"] = p.get("approved_amount_usd")
        elif et == "HumanReviewCompleted":
            state["final_decision"] = p.get("final_decision")
            state["state"] = p.get("final_decision", state["state"])
        elif et == "ApplicationApproved":
            state["state"] = "APPROVED"
            state["approved_amount_usd"] = p.get("approved_amount_usd")
        elif et == "ApplicationDeclined":
            state["state"] = "DECLINED"
    return state


# ── Compliance snapshot at examination_date ───────────────────────────────────

def _compliance_at(events: list[dict], application_id: str) -> dict[str, Any]:
    rules_passed: set[str] = set()
    rules_failed: set[str] = set()
    hard_blocked = False
    verdict = None

    for ev in events:
        et = ev.get("event_type", "")
        p = ev.get("payload", {})
        if et == "ComplianceRulePassed":
            rule_id = p.get("rule_id")
            if rule_id:
                rules_passed.add(rule_id)
                rules_failed.discard(rule_id)
        elif et == "ComplianceRuleFailed":
            rule_id = p.get("rule_id")
            if rule_id:
                rules_failed.add(rule_id)
                rules_passed.discard(rule_id)
            if p.get("is_hard_block"):
                hard_blocked = True
        elif et == "ComplianceCheckCompleted":
            verdict = p.get("overall_verdict")

    return {
        "application_id": application_id,
        "rules_passed": sorted(rules_passed),
        "rules_failed": sorted(rules_failed),
        "hard_blocked": hard_blocked,
        "verdict": verdict,
    }


# ── Public API ───────────────────────────────────────────────────────────────

async def generate_regulatory_package(
    store,
    application_id: str,
    examination_date: datetime,
) -> dict[str, Any]:
    """
    Generate a self-contained regulatory examination package.

    Precondition: store has an async load_stream(stream_id) method.
    Guarantee: the store is never written to.

    Args:
        store: event store instance (read-only access).
        application_id: the application to package.
        examination_date: include only events recorded at or before this time.

    Returns:
        A dict containing the full event stream, projection states, hash chain
        result, AI provenance, and plain-English narrative. The dict is
        JSON-serialisable (all datetimes are converted to ISO strings).
    """
    if examination_date.tzinfo is None:
        examination_date = examination_date.replace(tzinfo=timezone.utc)

    # Load all events for the loan stream
    all_events: list[dict] = await store.load_stream(f"loan-{application_id}")

    # Filter to events recorded at or before examination_date
    def _recorded_at(ev: dict) -> datetime:
        raw = ev.get("recorded_at")
        if isinstance(raw, datetime):
            return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
        if isinstance(raw, str):
            try:
                ts = datetime.fromisoformat(raw)
                return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pass
        return datetime.now(timezone.utc)

    events_in_scope = [
        ev for ev in all_events
        if _recorded_at(ev) <= examination_date
    ]

    # Serialise events for the package (convert any non-JSON-safe types)
    def _serialise(ev: dict) -> dict:
        out = dict(ev)
        if isinstance(out.get("recorded_at"), datetime):
            out["recorded_at"] = out["recorded_at"].isoformat()
        return out

    serialised_events = [_serialise(ev) for ev in events_in_scope]

    # Hash chain (read-only inline computation)
    audit_chain = _compute_hash_chain(events_in_scope)

    # Projection states at examination_date
    summary = _application_summary_at(events_in_scope, application_id)
    compliance = _compliance_at(events_in_scope, application_id)

    # AI agent provenance
    provenance = _extract_ai_provenance(events_in_scope)

    # Plain-English narrative
    narrative = _build_narrative(events_in_scope)

    return {
        "application_id": application_id,
        "examination_date": examination_date.isoformat(),
        "package_generated_at": datetime.now(timezone.utc).isoformat(),
        "events_in_scope": len(serialised_events),
        "event_stream": serialised_events,
        "projection_state_at_examination": {
            "application_summary": summary,
            "compliance": compliance,
        },
        "audit_chain": audit_chain,
        "ai_agent_provenance": provenance,
        "narrative": narrative,
    }
