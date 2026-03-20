"""
ledger/mcp/tools.py
====================
MCP tools — 8 command tools that wrap the ledger command handlers.

Each tool:
  - Accepts typed parameters described for LLM consumers.
  - Catches DomainError / OptimisticConcurrencyError and returns a typed
    error object rather than a raw exception string.
  - Returns {"success": True, ...result...} on success.

Error envelope:
  {
    "success": False,
    "error_type": "DomainError" | "OptimisticConcurrencyError" | "ValidationError",
    "message": "...",
    "suggested_action": "...",
    # Optional — present for OCC errors:
    "stream_id": "...",
    "expected_version": N,
    "actual_version": M,
  }
"""
from __future__ import annotations

from decimal import Decimal

from fastmcp import FastMCP

from ledger.commands.handlers import (
    SubmitApplicationCommand,
    StartAgentSessionCommand,
    CreditAnalysisCompletedCommand,
    FraudScreeningCompletedCommand,
    ComplianceCheckCommand,
    GenerateDecisionCommand,
    HumanReviewCompletedCommand,
    handle_submit_application,
    handle_start_agent_session,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_compliance_check,
    handle_generate_decision,
    handle_human_review_completed,
    DomainError,
    OptimisticConcurrencyError,
)
from ledger.integrity.audit_chain import run_integrity_check as _run_integrity_check


def _err(error_type: str, message: str, suggested_action: str, **extra) -> dict:
    return {
        "success": False,
        "error_type": error_type,
        "message": message,
        "suggested_action": suggested_action,
        **extra,
    }


def register_tools(mcp: FastMCP, store) -> None:
    """Register all 8 command tools on `mcp`, closing over `store`."""

    # ── 1. submit_application ─────────────────────────────────────────────────

    @mcp.tool(
        description=(
            "Submit a new loan application. "
            "Precondition: application_id must not already exist in the ledger. "
            "Returns the stream_id and version on success."
        )
    )
    async def submit_application(
        application_id: str,
        applicant_id: str,
        requested_amount_usd: str,
        loan_purpose: str,
        loan_term_months: int,
        submission_channel: str = "online",
        contact_email: str = "",
        contact_name: str = "",
        application_reference: str = "",
    ) -> dict:
        try:
            cmd = SubmitApplicationCommand(
                application_id=application_id,
                applicant_id=applicant_id,
                requested_amount_usd=Decimal(requested_amount_usd),
                loan_purpose=loan_purpose,
                loan_term_months=loan_term_months,
                submission_channel=submission_channel,
                contact_email=contact_email,
                contact_name=contact_name,
                application_reference=application_reference,
            )
            result = await handle_submit_application(cmd, store)
            return {"success": True, **result}
        except DomainError as e:
            return _err("DomainError", str(e),
                        "Check that the application_id is unique before submitting.")
        except Exception as e:
            return _err("UnexpectedError", str(e), "Contact support.")

    # ── 2. start_agent_session ────────────────────────────────────────────────

    @mcp.tool(
        description=(
            "Open an agent session for a loan application. "
            "Emits AgentSessionStarted + AgentInputValidated (Gas Town contract). "
            "Precondition: application must exist; session_id must be unique."
        )
    )
    async def start_agent_session(
        session_id: str,
        agent_type: str,
        agent_id: str,
        application_id: str,
        model_version: str,
        langgraph_graph_version: str = "1.0",
        context_source: str = "event_store",
        context_token_count: int = 0,
    ) -> dict:
        try:
            cmd = StartAgentSessionCommand(
                session_id=session_id,
                agent_type=agent_type,
                agent_id=agent_id,
                application_id=application_id,
                model_version=model_version,
                langgraph_graph_version=langgraph_graph_version,
                context_source=context_source,
                context_token_count=context_token_count,
            )
            result = await handle_start_agent_session(cmd, store)
            return {"success": True, **result}
        except DomainError as e:
            return _err("DomainError", str(e),
                        "Ensure the session_id is unique and the application exists.")
        except Exception as e:
            return _err("UnexpectedError", str(e), "Contact support.")

    # ── 3. record_credit_analysis ─────────────────────────────────────────────

    @mcp.tool(
        description=(
            "Record a completed credit analysis for a loan application. "
            "Preconditions: (1) start_agent_session must have been called for this "
            "session_id (Gas Town rule); (2) credit analysis must not already be "
            "locked; (3) model_version must match the declared session version."
        )
    )
    async def record_credit_analysis(
        application_id: str,
        session_id: str,
        agent_id: str,
        agent_type: str,
        model_version: str,
        model_deployment_id: str,
        risk_tier: str,
        recommended_limit_usd: str,
        confidence: float,
        rationale: str,
        input_data_hash: str = "n/a",
        analysis_duration_ms: int = 0,
        regulatory_basis: list | None = None,
    ) -> dict:
        try:
            cmd = CreditAnalysisCompletedCommand(
                application_id=application_id,
                session_id=session_id,
                agent_id=agent_id,
                agent_type=agent_type,
                model_version=model_version,
                model_deployment_id=model_deployment_id,
                decision={
                    "risk_tier": risk_tier,
                    "recommended_limit_usd": recommended_limit_usd,
                    "confidence": confidence,
                    "rationale": rationale,
                },
                input_data_hash=input_data_hash,
                analysis_duration_ms=analysis_duration_ms,
                regulatory_basis=regulatory_basis or [],
            )
            result = await handle_credit_analysis_completed(cmd, store)
            return {"success": True, **result}
        except DomainError as e:
            return _err(
                "DomainError", str(e),
                "Ensure start_agent_session was called and credit analysis is not locked.",
            )
        except OptimisticConcurrencyError as e:
            return _err(
                "OptimisticConcurrencyError", str(e),
                "Another writer updated the stream — reload state and retry.",
                stream_id=f"loan-{application_id}",
            )
        except Exception as e:
            return _err("UnexpectedError", str(e), "Contact support.")

    # ── 4. record_fraud_screening ─────────────────────────────────────────────

    @mcp.tool(
        description=(
            "Record a completed fraud screening for a loan application. "
            "Preconditions: start_agent_session must have been called (Gas Town); "
            "fraud_score must be in [0.0, 1.0]."
        )
    )
    async def record_fraud_screening(
        application_id: str,
        session_id: str,
        agent_id: str,
        agent_type: str,
        fraud_score: float,
        risk_level: str,
        recommendation: str,
        screening_model_version: str,
        input_data_hash: str = "n/a",
        anomalies_found: int = 0,
    ) -> dict:
        try:
            cmd = FraudScreeningCompletedCommand(
                application_id=application_id,
                session_id=session_id,
                agent_id=agent_id,
                agent_type=agent_type,
                fraud_score=fraud_score,
                risk_level=risk_level,
                recommendation=recommendation,
                screening_model_version=screening_model_version,
                input_data_hash=input_data_hash,
                anomalies_found=anomalies_found,
            )
            result = await handle_fraud_screening_completed(cmd, store)
            return {"success": True, **result}
        except DomainError as e:
            return _err("DomainError", str(e),
                        "Ensure start_agent_session was called and fraud_score is in [0.0, 1.0].")
        except OptimisticConcurrencyError as e:
            return _err("OptimisticConcurrencyError", str(e),
                        "Reload state and retry.", stream_id=f"loan-{application_id}")
        except Exception as e:
            return _err("UnexpectedError", str(e), "Contact support.")

    # ── 5. record_compliance_check ────────────────────────────────────────────

    @mcp.tool(
        description=(
            "Record a compliance rule evaluation (pass or fail). "
            "Precondition: rule_id must be non-empty."
        )
    )
    async def record_compliance_check(
        application_id: str,
        session_id: str,
        rule_id: str,
        rule_name: str,
        rule_version: str,
        passed: bool,
        evidence_hash: str = "n/a",
        evaluation_notes: str = "",
        is_hard_block: bool = False,
        failure_reason: str = "",
        remediation_available: bool = False,
    ) -> dict:
        try:
            cmd = ComplianceCheckCommand(
                application_id=application_id,
                session_id=session_id,
                rule_id=rule_id,
                rule_name=rule_name,
                rule_version=rule_version,
                passed=passed,
                evidence_hash=evidence_hash,
                evaluation_notes=evaluation_notes,
                is_hard_block=is_hard_block,
                failure_reason=failure_reason,
                remediation_available=remediation_available,
            )
            result = await handle_compliance_check(cmd, store)
            return {"success": True, **result}
        except DomainError as e:
            return _err("DomainError", str(e), "Provide a valid non-empty rule_id.")
        except OptimisticConcurrencyError as e:
            return _err("OptimisticConcurrencyError", str(e),
                        "Reload compliance state and retry.",
                        stream_id=f"compliance-{application_id}")
        except Exception as e:
            return _err("UnexpectedError", str(e), "Contact support.")

    # ── 6. generate_decision ──────────────────────────────────────────────────

    @mcp.tool(
        description=(
            "Generate a final lending decision for a loan application. "
            "Preconditions: credit analysis AND fraud screening must be complete. "
            "Rule 4: confidence < 0.6 automatically overrides recommendation to REFER. "
            "Rule 5: all required_compliance_rules must be evaluated before calling."
        )
    )
    async def generate_decision(
        application_id: str,
        orchestrator_session_id: str,
        recommendation: str,
        confidence: float,
        executive_summary: str,
        contributing_sessions: list | None = None,
        approved_amount_usd: str | None = None,
        conditions: list | None = None,
        key_risks: list | None = None,
        model_versions: dict | None = None,
        required_compliance_rules: list | None = None,
    ) -> dict:
        try:
            cmd = GenerateDecisionCommand(
                application_id=application_id,
                orchestrator_session_id=orchestrator_session_id,
                recommendation=recommendation,
                confidence=confidence,
                executive_summary=executive_summary,
                contributing_sessions=contributing_sessions or [],
                approved_amount_usd=Decimal(approved_amount_usd) if approved_amount_usd else None,
                conditions=conditions or [],
                key_risks=key_risks or [],
                model_versions=model_versions or {},
                required_compliance_rules=set(required_compliance_rules or []),
            )
            result = await handle_generate_decision(cmd, store)
            return {"success": True, **result}
        except DomainError as e:
            return _err(
                "DomainError", str(e),
                "Ensure credit analysis, fraud screening, and all required compliance checks "
                "are complete before generating a decision.",
            )
        except OptimisticConcurrencyError as e:
            return _err("OptimisticConcurrencyError", str(e),
                        "Reload loan state and retry.",
                        stream_id=f"loan-{application_id}")
        except Exception as e:
            return _err("UnexpectedError", str(e), "Contact support.")

    # ── 7. record_human_review ────────────────────────────────────────────────

    @mcp.tool(
        description=(
            "Record the outcome of a human review of an AI decision. "
            "Precondition: override_reason is REQUIRED when override=True."
        )
    )
    async def record_human_review(
        application_id: str,
        reviewer_id: str,
        override: bool,
        original_recommendation: str,
        final_decision: str,
        override_reason: str | None = None,
    ) -> dict:
        try:
            cmd = HumanReviewCompletedCommand(
                application_id=application_id,
                reviewer_id=reviewer_id,
                override=override,
                original_recommendation=original_recommendation,
                final_decision=final_decision,
                override_reason=override_reason,
            )
            result = await handle_human_review_completed(cmd, store)
            return {"success": True, **result}
        except DomainError as e:
            return _err(
                "DomainError", str(e),
                "If override=True, you must provide an override_reason.",
            )
        except OptimisticConcurrencyError as e:
            return _err("OptimisticConcurrencyError", str(e),
                        "Reload loan state and retry.",
                        stream_id=f"loan-{application_id}")
        except Exception as e:
            return _err("UnexpectedError", str(e), "Contact support.")

    # ── 8. run_integrity_check ────────────────────────────────────────────────

    @mcp.tool(
        description=(
            "Run a cryptographic audit hash-chain check on a stream. "
            "Detects any tampering since the last check. "
            "Records the result in the audit stream for the entity."
        )
    )
    async def run_integrity_check(
        stream_id: str,
        entity_type: str | None = None,
        entity_id: str | None = None,
    ) -> dict:
        try:
            result = await _run_integrity_check(
                store, stream_id,
                entity_type=entity_type,
                entity_id=entity_id,
            )
            return {"success": True, **result}
        except Exception as e:
            return _err("UnexpectedError", str(e), "Verify the stream_id exists.")
