"""
ledger/agents/stub_agents.py
============================
Implementations of DocumentProcessingAgent, FraudDetectionAgent,
ComplianceAgent, and DecisionOrchestratorAgent.

Pattern: follows CreditAnalysisAgent exactly. Same build_graph() structure,
same _record_node_execution() calls, same _append_with_retry() for domain writes.
"""
from __future__ import annotations
import asyncio, json, os, time
from datetime import datetime
from decimal import Decimal
from typing import TypedDict
from uuid import uuid4

from langgraph.graph import StateGraph, END

from ledger.agents.base_agent import BaseApexAgent
from ledger.schema.events import (
    # Document package
    DocumentFormatValidated, ExtractionStarted, ExtractionCompleted, ExtractionFailed,
    QualityAssessmentCompleted, PackageReadyForAnalysis, DocumentType,
    # Loan application
    CreditAnalysisRequested, ComplianceCheckRequested, DecisionRequested,
    ApplicationDeclined, ApplicationApproved, HumanReviewRequested, DecisionGenerated,
    # Fraud
    FraudScreeningInitiated, FraudAnomalyDetected, FraudScreeningCompleted,
    FraudAnomaly, FraudAnomalyType,
    # Compliance
    ComplianceCheckInitiated, ComplianceRulePassed, ComplianceRuleFailed,
    ComplianceRuleNoted, ComplianceCheckCompleted, ComplianceVerdict,
    # Value objects
    FinancialFacts,
)


# ─── DOCUMENT PROCESSING AGENT ───────────────────────────────────────────────

class DocProcState(TypedDict):
    application_id: str
    session_id: str
    applicant_id: str | None
    document_ids: list[str] | None
    document_paths: dict[str, str] | None   # doc_type -> file_path
    is_facts: dict | None                    # income statement FinancialFacts dict
    bs_facts: dict | None                    # balance sheet FinancialFacts dict
    quality_assessment: dict | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


QUALITY_SYSTEM_PROMPT = """You are a financial document quality analyst reviewing extracted financial data.

Check ONLY the following:
1. Internal consistency (e.g. Assets = Liabilities + Equity within 5% tolerance)
2. Implausible values (e.g. negative revenue, gross profit > revenue)
3. Critical missing fields that would prevent credit analysis

Return ONLY this JSON object with no preamble:
{
  "overall_confidence": <float 0.0-1.0>,
  "is_coherent": <bool>,
  "anomalies": [<string>],
  "critical_missing_fields": [<string>],
  "reextraction_recommended": <bool>,
  "auditor_notes": "<string>"
}

DO NOT make credit or lending decisions. Only assess data quality."""


class DocumentProcessingAgent(BaseApexAgent):
    """
    Wraps the paperMind-ai extraction pipeline.
    Processes uploaded PDFs and appends extraction events.

    Nodes:
        validate_inputs -> validate_document_formats -> extract_income_statement ->
        extract_balance_sheet -> assess_quality -> write_output
    """

    def build_graph(self):
        g = StateGraph(DocProcState)
        g.add_node("validate_inputs",           self._node_validate_inputs)
        g.add_node("validate_document_formats", self._node_validate_formats)
        g.add_node("extract_income_statement",  self._node_extract_is)
        g.add_node("extract_balance_sheet",     self._node_extract_bs)
        g.add_node("assess_quality",            self._node_assess_quality)
        g.add_node("write_output",              self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",           "validate_document_formats")
        g.add_edge("validate_document_formats", "extract_income_statement")
        g.add_edge("extract_income_statement",  "extract_balance_sheet")
        g.add_edge("extract_balance_sheet",     "assess_quality")
        g.add_edge("assess_quality",            "write_output")
        g.add_edge("write_output",              END)
        return g.compile()

    def _initial_state(self, application_id: str) -> DocProcState:
        return DocProcState(
            application_id=application_id, session_id=self.session_id,
            applicant_id=None, document_ids=None, document_paths=None,
            is_facts=None, bs_facts=None, quality_assessment=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: DocProcState) -> DocProcState:
        t = time.perf_counter()
        app_id = state["application_id"]
        errors = []

        # Load DocumentUploaded events from loan stream
        try:
            loan_events = await self.store.load_stream(f"loan-{app_id}")
        except Exception as exc:
            errors.append(f"Failed to load loan stream: {exc}")
            await self._record_input_failed([], errors)
            raise ValueError(f"Input validation failed: {errors}")

        # Collect document paths by type
        doc_paths: dict[str, str] = {}
        doc_ids: list[str] = []
        applicant_id = None

        for ev in loan_events:
            et = ev["event_type"]
            p = ev.get("payload", {})
            if et == "ApplicationSubmitted":
                applicant_id = p.get("applicant_id")
            elif et == "DocumentUploaded":
                doc_type = p.get("document_type", "")
                file_path = p.get("file_path", "")
                doc_id = p.get("document_id", "")
                doc_paths[doc_type] = file_path
                doc_ids.append(doc_id)

        # Verify required documents
        required = [
            DocumentType.APPLICATION_PROPOSAL.value,
            DocumentType.INCOME_STATEMENT.value,
            DocumentType.BALANCE_SHEET.value,
        ]
        missing = [r for r in required if r not in doc_paths]
        if missing:
            errors.append(f"Missing required documents: {missing}")
            await self._record_input_failed(missing, errors)
            raise ValueError(f"Input validation failed: missing documents {missing}")

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_input_validated(
            ["application_id", "document_ids", "file_paths"], ms
        )
        await self._record_node_execution(
            "validate_inputs",
            ["application_id"],
            ["document_paths", "document_ids", "applicant_id"],
            ms,
        )
        return {
            **state,
            "applicant_id": applicant_id,
            "document_ids": doc_ids,
            "document_paths": doc_paths,
            "errors": errors,
        }

    async def _node_validate_formats(self, state: DocProcState) -> DocProcState:
        t = time.perf_counter()
        app_id = state["application_id"]
        doc_paths = state.get("document_paths") or {}

        for doc_type, file_path in doc_paths.items():
            t_tool = time.perf_counter()
            path_obj = __import__("pathlib").Path(file_path)
            exists = path_obj.exists()
            ms_tool = int((time.perf_counter() - t_tool) * 1000)
            await self._record_tool_call(
                "filesystem_check",
                f"doc_type={doc_type} path={file_path}",
                f"exists={exists}",
                ms_tool,
            )

            if exists:
                doc_id = f"doc-{doc_type}-{app_id}"
                try:
                    page_count = 1
                    try:
                        import pdfplumber
                        with pdfplumber.open(file_path) as pdf:
                            page_count = len(pdf.pages)
                    except Exception:
                        pass

                    event = DocumentFormatValidated(
                        package_id=f"docpkg-{app_id}",
                        document_id=doc_id,
                        document_type=DocumentType(doc_type),
                        page_count=page_count,
                        detected_format="pdf",
                        validated_at=datetime.now(),
                    ).to_store_dict()
                    await self._append_with_retry(f"docpkg-{app_id}", [event])
                except Exception:
                    pass

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_node_execution(
            "validate_document_formats",
            ["document_paths"],
            ["DocumentFormatValidated"],
            ms,
        )
        return state

    async def _node_extract_is(self, state: DocProcState) -> DocProcState:
        t = time.perf_counter()
        app_id = state["application_id"]
        doc_paths = state.get("document_paths") or {}
        is_path = doc_paths.get(DocumentType.INCOME_STATEMENT.value)
        doc_id = f"doc-{DocumentType.INCOME_STATEMENT.value}-{app_id}"

        # Signal extraction start
        start_event = ExtractionStarted(
            package_id=f"docpkg-{app_id}",
            document_id=doc_id,
            document_type=DocumentType.INCOME_STATEMENT,
            pipeline_version="papermind-1.0",
            extraction_model="fast_text",
            started_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"docpkg-{app_id}", [start_event])

        facts: FinancialFacts | None = None
        try:
            from ledger.agents.papermind_adapter import PaperMindAdapter
            facts, confidence = await PaperMindAdapter().extract(is_path, doc_id)
        except Exception as exc:
            fail_event = ExtractionFailed(
                package_id=f"docpkg-{app_id}",
                document_id=doc_id,
                error_type=type(exc).__name__,
                error_message=str(exc)[:500],
                failed_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"docpkg-{app_id}", [fail_event])
            ms = int((time.perf_counter() - t) * 1000)
            await self._record_tool_call("papermind_extraction", f"doc={doc_id}", f"FAILED: {exc!s:.100}", ms)
            await self._record_node_execution("extract_income_statement", ["is_path"], ["ExtractionFailed"], ms)
            return {**state, "is_facts": None}

        complete_event = ExtractionCompleted(
            package_id=f"docpkg-{app_id}",
            document_id=doc_id,
            document_type=DocumentType.INCOME_STATEMENT,
            facts=facts,
            raw_text_length=1000,
            tables_extracted=1,
            processing_ms=int((time.perf_counter() - t) * 1000),
            completed_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"docpkg-{app_id}", [complete_event])

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_tool_call(
            "papermind_extraction",
            f"doc={doc_id} type=income_statement",
            f"facts extracted, ebitda={'present' if facts.ebitda else 'missing'}",
            ms,
        )
        await self._record_node_execution(
            "extract_income_statement", ["is_path"], ["ExtractionCompleted"], ms
        )
        return {**state, "is_facts": facts.model_dump(mode="json") if facts else None}

    async def _node_extract_bs(self, state: DocProcState) -> DocProcState:
        t = time.perf_counter()
        app_id = state["application_id"]
        doc_paths = state.get("document_paths") or {}
        bs_path = doc_paths.get(DocumentType.BALANCE_SHEET.value)
        doc_id = f"doc-{DocumentType.BALANCE_SHEET.value}-{app_id}"

        start_event = ExtractionStarted(
            package_id=f"docpkg-{app_id}",
            document_id=doc_id,
            document_type=DocumentType.BALANCE_SHEET,
            pipeline_version="papermind-1.0",
            extraction_model="fast_text",
            started_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"docpkg-{app_id}", [start_event])

        facts: FinancialFacts | None = None
        try:
            from ledger.agents.papermind_adapter import PaperMindAdapter
            facts, confidence = await PaperMindAdapter().extract(bs_path, doc_id)
        except Exception as exc:
            fail_event = ExtractionFailed(
                package_id=f"docpkg-{app_id}",
                document_id=doc_id,
                error_type=type(exc).__name__,
                error_message=str(exc)[:500],
                failed_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"docpkg-{app_id}", [fail_event])
            ms = int((time.perf_counter() - t) * 1000)
            await self._record_tool_call("papermind_extraction", f"doc={doc_id}", f"FAILED: {exc!s:.100}", ms)
            await self._record_node_execution("extract_balance_sheet", ["bs_path"], ["ExtractionFailed"], ms)
            return {**state, "bs_facts": None}

        complete_event = ExtractionCompleted(
            package_id=f"docpkg-{app_id}",
            document_id=doc_id,
            document_type=DocumentType.BALANCE_SHEET,
            facts=facts,
            raw_text_length=1000,
            tables_extracted=1,
            processing_ms=int((time.perf_counter() - t) * 1000),
            completed_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"docpkg-{app_id}", [complete_event])

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_tool_call(
            "papermind_extraction",
            f"doc={doc_id} type=balance_sheet",
            f"assets={'present' if facts and facts.total_assets else 'missing'}",
            ms,
        )
        await self._record_node_execution(
            "extract_balance_sheet", ["bs_path"], ["ExtractionCompleted"], ms
        )
        return {**state, "bs_facts": facts.model_dump(mode="json") if facts else None}

    async def _node_assess_quality(self, state: DocProcState) -> DocProcState:
        t = time.perf_counter()
        app_id = state["application_id"]
        is_facts = state.get("is_facts") or {}
        bs_facts = state.get("bs_facts") or {}

        # Merge facts for LLM prompt
        merged = {**is_facts, **{k: v for k, v in bs_facts.items() if v is not None and k not in is_facts}}
        doc_id = f"docpkg-{app_id}"

        user_message = f"""Extracted financial facts (merged income statement + balance sheet):
{json.dumps({k: str(v) for k, v in merged.items() if v is not None and k not in ('field_confidence','extraction_notes','page_references')}, indent=2)}

Missing fields (extraction returned None):
{[k for k, v in merged.items() if v is None and k not in ('field_confidence','extraction_notes','page_references','balance_sheet_balances','balance_discrepancy_usd')]}

Assess quality only. Do not make lending decisions."""

        ti = to = 0
        cost = 0.0
        qa: dict = {
            "overall_confidence": 0.6,
            "is_coherent": True,
            "anomalies": [],
            "critical_missing_fields": [],
            "reextraction_recommended": False,
            "auditor_notes": "Automated quality check completed.",
        }
        try:
            content, ti, to, cost = await self._call_llm(QUALITY_SYSTEM_PROMPT, user_message, max_tokens=512)
            qa = self._parse_json(content)
        except Exception as exc:
            qa["auditor_notes"] = f"LLM quality check failed: {exc!s:.100}; defaults applied."

        # Append QualityAssessmentCompleted for the combined package
        qa_event = QualityAssessmentCompleted(
            package_id=doc_id,
            document_id=doc_id,
            overall_confidence=float(qa.get("overall_confidence", 0.6)),
            is_coherent=bool(qa.get("is_coherent", True)),
            anomalies=[str(a) for a in qa.get("anomalies", [])],
            critical_missing_fields=[str(f) for f in qa.get("critical_missing_fields", [])],
            reextraction_recommended=bool(qa.get("reextraction_recommended", False)),
            auditor_notes=str(qa.get("auditor_notes", "")),
            assessed_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"docpkg-{app_id}", [qa_event])

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_node_execution(
            "assess_quality",
            ["is_facts", "bs_facts"],
            ["QualityAssessmentCompleted"],
            ms, ti, to, cost,
        )
        return {**state, "quality_assessment": qa}

    async def _node_write_output(self, state: DocProcState) -> DocProcState:
        t = time.perf_counter()
        app_id = state["application_id"]
        qa = state.get("quality_assessment") or {}
        anomalies = qa.get("anomalies", [])

        # Append PackageReadyForAnalysis to docpkg stream
        ready_event = PackageReadyForAnalysis(
            package_id=f"docpkg-{app_id}",
            application_id=app_id,
            documents_processed=2,
            has_quality_flags=bool(anomalies),
            quality_flag_count=len(anomalies),
            ready_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"docpkg-{app_id}", [ready_event])

        # Trigger CreditAnalysisAgent
        credit_trigger = CreditAnalysisRequested(
            application_id=app_id,
            requested_at=datetime.now(),
            requested_by=self.session_id,
            priority="NORMAL",
        ).to_store_dict()
        await self._append_with_retry(f"loan-{app_id}", [credit_trigger])

        events_written = [
            {"stream_id": f"docpkg-{app_id}", "event_type": "PackageReadyForAnalysis"},
            {"stream_id": f"loan-{app_id}", "event_type": "CreditAnalysisRequested"},
        ]
        await self._record_output_written(events_written, "Document package ready. Credit analysis triggered.")

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_node_execution(
            "write_output", ["quality_assessment"], ["events_written"], ms
        )
        return {**state, "output_events": events_written, "next_agent": "credit_analysis"}


# ─── FRAUD DETECTION AGENT ───────────────────────────────────────────────────

class FraudState(TypedDict):
    application_id: str
    session_id: str
    extracted_facts: dict | None
    registry_profile: dict | None
    historical_financials: list[dict] | None
    fraud_signals: list[dict] | None
    fraud_score: float | None
    anomalies: list[dict] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


FRAUD_SYSTEM_PROMPT = """You are a financial fraud analyst at a commercial lending institution.

Given current-year extracted facts and 3-year historical registry data, identify specific
anomalies that may indicate fraud or document manipulation.

For each anomaly found, return a JSON object. Compute a final fraud_score (0.0-1.0).

Return ONLY this JSON:
{
  "fraud_score": <float 0.0-1.0>,
  "anomalies": [
    {
      "anomaly_type": "revenue_discrepancy"|"balance_sheet_inconsistency"|"unusual_submission_pattern"|"identity_mismatch"|"document_alteration_suspected",
      "description": "<specific observation>",
      "severity": "LOW"|"MEDIUM"|"HIGH",
      "evidence": "<specific data points>",
      "affected_fields": ["<field_name>"]
    }
  ],
  "recommendation": "PROCEED"|"FLAG_FOR_REVIEW"|"DECLINE"
}

SCORING: fraud_score > 0.60 -> DECLINE, 0.30-0.60 -> FLAG_FOR_REVIEW, < 0.30 -> PROCEED.
If no anomalies are found, return fraud_score 0.05 and empty anomalies list."""


class FraudDetectionAgent(BaseApexAgent):
    """
    Cross-references extracted document facts against historical registry data.
    Detects anomalous discrepancies that suggest fraud.

    Nodes:
        validate_inputs -> load_document_facts -> cross_reference_registry ->
        analyze_fraud_patterns -> write_output
    """

    def build_graph(self):
        g = StateGraph(FraudState)
        g.add_node("validate_inputs",          self._node_validate_inputs)
        g.add_node("load_document_facts",      self._node_load_facts)
        g.add_node("cross_reference_registry", self._node_cross_reference)
        g.add_node("analyze_fraud_patterns",   self._node_analyze)
        g.add_node("write_output",             self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",          "load_document_facts")
        g.add_edge("load_document_facts",      "cross_reference_registry")
        g.add_edge("cross_reference_registry", "analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns",   "write_output")
        g.add_edge("write_output",             END)
        return g.compile()

    def _initial_state(self, application_id: str) -> FraudState:
        return FraudState(
            application_id=application_id, session_id=self.session_id,
            extracted_facts=None, registry_profile=None, historical_financials=None,
            fraud_signals=None, fraud_score=None, anomalies=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: FraudState) -> FraudState:
        t = time.perf_counter()
        app_id = state["application_id"]
        errors = []

        try:
            loan_events = await self.store.load_stream(f"loan-{app_id}")
        except Exception as exc:
            errors.append(f"Failed to load loan stream: {exc}")
            await self._record_input_failed([], errors)
            raise ValueError(f"Input validation failed: {errors}")

        # Find FraudScreeningRequested event
        has_trigger = any(e["event_type"] == "FraudScreeningRequested" for e in loan_events)
        if not has_trigger:
            errors.append("FraudScreeningRequested event not found on loan stream")
            await self._record_input_failed(["FraudScreeningRequested"], errors)
            raise ValueError(f"Input validation failed: {errors}")

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_input_validated(["application_id", "FraudScreeningRequested"], ms)
        await self._record_node_execution(
            "validate_inputs", ["application_id"], ["validated"], ms
        )
        return {**state, "errors": errors}

    async def _node_load_facts(self, state: FraudState) -> FraudState:
        t = time.perf_counter()
        app_id = state["application_id"]

        # Append FraudScreeningInitiated to fraud stream
        init_event = FraudScreeningInitiated(
            application_id=app_id,
            session_id=self.session_id,
            screening_model_version="fraud-model-v1.0",
            initiated_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"fraud-{app_id}", [init_event])

        # Load ExtractionCompleted events from docpkg stream
        pkg_events = await self.store.load_stream(f"docpkg-{app_id}")
        extraction_events = [e for e in pkg_events if e["event_type"] == "ExtractionCompleted"]

        merged_facts: dict = {}
        for ev in extraction_events:
            payload = ev.get("payload", {})
            facts = payload.get("facts") or {}
            for k, v in facts.items():
                if v is not None and k not in merged_facts:
                    merged_facts[k] = v

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_tool_call(
            "load_event_store_stream",
            f"stream_id=docpkg-{app_id} filter=ExtractionCompleted",
            f"Loaded {len(extraction_events)} extraction events",
            ms,
        )
        await self._record_node_execution(
            "load_document_facts", ["docpkg_stream"], ["extracted_facts"], ms
        )
        return {**state, "extracted_facts": merged_facts}

    async def _node_cross_reference(self, state: FraudState) -> FraudState:
        t = time.perf_counter()
        app_id = state["application_id"]

        # Get applicant_id from loan stream
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        applicant_id = None
        for ev in loan_events:
            if ev["event_type"] == "ApplicationSubmitted":
                applicant_id = ev.get("payload", {}).get("applicant_id")
                break

        profile_dict: dict = {}
        financials: list[dict] = []

        if applicant_id and self.registry:
            try:
                profile = await self.registry.get_company(applicant_id)
                if profile:
                    profile_dict = {
                        "company_id": profile.company_id,
                        "name": profile.name,
                        "industry": profile.industry,
                        "trajectory": profile.trajectory,
                        "risk_segment": profile.risk_segment,
                    }
                hist = await self.registry.get_financial_history(applicant_id)
                financials = [
                    {
                        "fiscal_year": f.fiscal_year,
                        "total_revenue": f.total_revenue,
                        "ebitda": f.ebitda,
                        "net_income": f.net_income,
                        "total_assets": f.total_assets,
                        "total_liabilities": f.total_liabilities,
                    }
                    for f in hist
                ]
            except Exception:
                pass

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_tool_call(
            "query_applicant_registry",
            f"company_id={applicant_id}",
            f"Loaded {len(financials)} fiscal years",
            ms,
        )
        await self._record_node_execution(
            "cross_reference_registry",
            ["applicant_id"],
            ["registry_profile", "historical_financials"],
            ms,
        )
        return {**state, "registry_profile": profile_dict, "historical_financials": financials}

    async def _node_analyze(self, state: FraudState) -> FraudState:
        t = time.perf_counter()
        app_id = state["application_id"]
        facts = state.get("extracted_facts") or {}
        hist = state.get("historical_financials") or []
        profile = state.get("registry_profile") or {}

        fins_table = "\n".join([
            f"FY{f['fiscal_year']}: Revenue=${f.get('total_revenue', 0):,.0f}"
            f" EBITDA=${f.get('ebitda', 0):,.0f}"
            f" Net=${f.get('net_income', 0):,.0f}"
            for f in hist
        ]) or "No historical registry data available"

        user_message = f"""Company: {profile.get('name', 'Unknown')} ({profile.get('industry', 'Unknown')})
Trajectory: {profile.get('trajectory', 'UNKNOWN')}

HISTORICAL FINANCIAL REGISTRY (3 years):
{fins_table}

CURRENT YEAR EXTRACTED FROM SUBMITTED DOCUMENTS:
{json.dumps({k: str(v) for k, v in facts.items() if v is not None and k not in ('field_confidence', 'extraction_notes', 'page_references')}, indent=2)}

Identify any anomalous gaps between the submitted documents and historical registry data."""

        ti = to = 0
        cost = 0.0
        assessment: dict = {"fraud_score": 0.05, "anomalies": [], "recommendation": "PROCEED"}
        try:
            content, ti, to, cost = await self._call_llm(FRAUD_SYSTEM_PROMPT, user_message, max_tokens=1024)
            assessment = self._parse_json(content)
        except Exception as exc:
            assessment["recommendation"] = "FLAG_FOR_REVIEW"
            assessment["fraud_score"] = 0.35
            assessment["anomalies"] = [{"anomaly_type": "unusual_submission_pattern",
                                         "description": f"Automated analysis failed: {exc!s:.100}",
                                         "severity": "LOW", "evidence": "System error", "affected_fields": []}]

        fraud_score = float(assessment.get("fraud_score", 0.05))
        raw_anomalies = assessment.get("anomalies", [])

        # Append FraudAnomalyDetected for each anomaly
        for raw_a in raw_anomalies:
            try:
                anomaly = FraudAnomaly(
                    anomaly_type=FraudAnomalyType(raw_a.get("anomaly_type", "unusual_submission_pattern")),
                    description=str(raw_a.get("description", "")),
                    severity=str(raw_a.get("severity", "LOW")),
                    evidence=str(raw_a.get("evidence", "")),
                    affected_fields=list(raw_a.get("affected_fields", [])),
                )
                anom_event = FraudAnomalyDetected(
                    application_id=app_id,
                    session_id=self.session_id,
                    anomaly=anomaly,
                    detected_at=datetime.now(),
                ).to_store_dict()
                await self._append_with_retry(f"fraud-{app_id}", [anom_event])
            except Exception:
                pass

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_node_execution(
            "analyze_fraud_patterns",
            ["extracted_facts", "historical_financials"],
            ["fraud_score", "anomalies"],
            ms, ti, to, cost,
        )
        return {
            **state,
            "fraud_score": fraud_score,
            "anomalies": raw_anomalies,
            "fraud_signals": assessment,
        }

    async def _node_write_output(self, state: FraudState) -> FraudState:
        t = time.perf_counter()
        app_id = state["application_id"]
        fraud_score = state.get("fraud_score") or 0.05
        anomalies = state.get("anomalies") or []

        if fraud_score > 0.60:
            risk_level = "HIGH"
            recommendation = "DECLINE"
        elif fraud_score >= 0.30:
            risk_level = "MEDIUM"
            recommendation = "FLAG_FOR_REVIEW"
        else:
            risk_level = "LOW"
            recommendation = "PROCEED"

        completed_event = FraudScreeningCompleted(
            application_id=app_id,
            session_id=self.session_id,
            fraud_score=fraud_score,
            risk_level=risk_level,
            anomalies_found=len(anomalies),
            recommendation=recommendation,
            screening_model_version="fraud-model-v1.0",
            input_data_hash=self._sha(state.get("extracted_facts") or {}),
            completed_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"fraud-{app_id}", [completed_event])

        # Trigger ComplianceAgent
        compliance_trigger = ComplianceCheckRequested(
            application_id=app_id,
            requested_at=datetime.now(),
            triggered_by_event_id=self.session_id,
            regulation_set_version="2026-Q1",
            rules_to_evaluate=["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"],
        ).to_store_dict()
        await self._append_with_retry(f"loan-{app_id}", [compliance_trigger])

        events_written = [
            {"stream_id": f"fraud-{app_id}", "event_type": "FraudScreeningCompleted"},
            {"stream_id": f"loan-{app_id}", "event_type": "ComplianceCheckRequested"},
        ]
        await self._record_output_written(
            events_written,
            f"Fraud: {risk_level} risk (score={fraud_score:.2f}), {len(anomalies)} anomalies. Compliance triggered.",
        )

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_node_execution(
            "write_output", ["fraud_score", "anomalies"], ["events_written"], ms
        )
        return {**state, "output_events": events_written, "next_agent": "compliance"}


# ─── COMPLIANCE AGENT ─────────────────────────────────────────────────────────

class ComplianceState(TypedDict):
    application_id: str
    session_id: str
    company_profile: dict | None
    requested_amount_usd: float | None
    rule_results: list[dict] | None
    has_hard_block: bool
    block_rule_id: str | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


REGULATIONS = {
    "REG-001": {
        "name": "Bank Secrecy Act (BSA) Check",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not any(
            f.get("flag_type") == "AML_WATCH" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active AML Watch flag present. Remediation required.",
        "remediation": "Provide enhanced due diligence documentation within 10 business days.",
    },
    "REG-002": {
        "name": "OFAC Sanctions Screening",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: not any(
            f.get("flag_type") == "SANCTIONS_REVIEW" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active OFAC Sanctions Review. Application blocked.",
        "remediation": None,
    },
    "REG-003": {
        "name": "Jurisdiction Lending Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: co.get("jurisdiction") != "MT",
        "failure_reason": "Jurisdiction MT not approved for commercial lending at this time.",
        "remediation": None,
    },
    "REG-004": {
        "name": "Legal Entity Type Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not (
            co.get("legal_type") == "Sole Proprietor"
            and (co.get("requested_amount_usd") or 0) > 250_000
        ),
        "failure_reason": "Sole Proprietor loans >$250K require additional documentation.",
        "remediation": "Submit SBA Form 912 and personal financial statement.",
    },
    "REG-005": {
        "name": "Minimum Operating History",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: (2026 - (co.get("founded_year") or 2026)) >= 2,
        "failure_reason": "Business must have at least 2 years of operating history.",
        "remediation": None,
    },
    "REG-006": {
        "name": "CRA Community Reinvestment",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: True,
        "note_type": "CRA_CONSIDERATION",
        "note_text": "Jurisdiction qualifies for Community Reinvestment Act consideration.",
    },
}


class ComplianceAgent(BaseApexAgent):
    """
    Evaluates 6 deterministic regulatory rules in sequence.
    Stops at first hard block. LLM used only for human-readable evidence summaries.

    Nodes:
        validate_inputs -> load_company_profile -> evaluate_reg001 -> evaluate_reg002 ->
        evaluate_reg003 -> evaluate_reg004 -> evaluate_reg005 -> evaluate_reg006 -> write_output
    """

    def _make_rule_node(self, rule_id: str):
        """Return an async node function for the given rule_id."""
        async def _rule_node(state: ComplianceState) -> ComplianceState:
            return await self._evaluate_rule(state, rule_id)
        _rule_node.__name__ = f"evaluate_{rule_id.lower().replace('-', '_')}"
        return _rule_node

    def build_graph(self):
        g = StateGraph(ComplianceState)
        g.add_node("validate_inputs",      self._node_validate_inputs)
        g.add_node("load_company_profile", self._node_load_profile)
        g.add_node("evaluate_reg001",      self._make_rule_node("REG-001"))
        g.add_node("evaluate_reg002",      self._make_rule_node("REG-002"))
        g.add_node("evaluate_reg003",      self._make_rule_node("REG-003"))
        g.add_node("evaluate_reg004",      self._make_rule_node("REG-004"))
        g.add_node("evaluate_reg005",      self._make_rule_node("REG-005"))
        g.add_node("evaluate_reg006",      self._make_rule_node("REG-006"))
        g.add_node("write_output",         self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",      "load_company_profile")
        g.add_edge("load_company_profile", "evaluate_reg001")

        for src, nxt in [
            ("evaluate_reg001", "evaluate_reg002"),
            ("evaluate_reg002", "evaluate_reg003"),
            ("evaluate_reg003", "evaluate_reg004"),
            ("evaluate_reg004", "evaluate_reg005"),
            ("evaluate_reg005", "evaluate_reg006"),
            ("evaluate_reg006", "write_output"),
        ]:
            g.add_conditional_edges(
                src,
                lambda s, _nxt=nxt: "write_output" if s["has_hard_block"] else _nxt,
            )
        g.add_edge("write_output", END)
        return g.compile()

    def _initial_state(self, application_id: str) -> ComplianceState:
        return ComplianceState(
            application_id=application_id, session_id=self.session_id,
            company_profile=None, requested_amount_usd=None,
            rule_results=[], has_hard_block=False, block_rule_id=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: ComplianceState) -> ComplianceState:
        t = time.perf_counter()
        app_id = state["application_id"]
        errors = []

        try:
            loan_events = await self.store.load_stream(f"loan-{app_id}")
        except Exception as exc:
            errors.append(f"Failed to load loan stream: {exc}")
            await self._record_input_failed([], errors)
            raise ValueError(f"Input validation failed: {errors}")

        has_trigger = any(e["event_type"] == "ComplianceCheckRequested" for e in loan_events)
        if not has_trigger:
            errors.append("ComplianceCheckRequested event not found")
            await self._record_input_failed(["ComplianceCheckRequested"], errors)
            raise ValueError(f"Input validation failed: {errors}")

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_input_validated(["application_id", "ComplianceCheckRequested"], ms)
        await self._record_node_execution(
            "validate_inputs", ["application_id"], ["validated"], ms
        )
        return {**state, "errors": errors}

    async def _node_load_profile(self, state: ComplianceState) -> ComplianceState:
        t = time.perf_counter()
        app_id = state["application_id"]

        # Get applicant_id and requested_amount from loan stream
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        applicant_id = None
        requested_amount = 0.0
        for ev in loan_events:
            if ev["event_type"] == "ApplicationSubmitted":
                p = ev.get("payload", {})
                applicant_id = p.get("applicant_id")
                requested_amount = float(p.get("requested_amount_usd", 0))
                break

        company_profile: dict = {}

        if applicant_id and self.registry:
            try:
                profile = await self.registry.get_company(applicant_id)
                flags = await self.registry.get_compliance_flags(applicant_id)
                if profile:
                    company_profile = {
                        "company_id": profile.company_id,
                        "name": profile.name,
                        "jurisdiction": profile.jurisdiction,
                        "legal_type": profile.legal_type,
                        "founded_year": profile.founded_year,
                        "compliance_flags": [
                            {
                                "flag_type": f.flag_type,
                                "is_active": f.is_active,
                                "severity": f.severity,
                            }
                            for f in flags
                        ],
                        "requested_amount_usd": requested_amount,
                    }
            except Exception:
                pass

        # Append ComplianceCheckInitiated
        init_event = ComplianceCheckInitiated(
            application_id=app_id,
            session_id=self.session_id,
            regulation_set_version="2026-Q1",
            rules_to_evaluate=list(REGULATIONS.keys()),
            initiated_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"compliance-{app_id}", [init_event])

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_tool_call(
            "query_applicant_registry",
            f"company_id={applicant_id}",
            f"Loaded profile for {company_profile.get('name', 'unknown')}",
            ms,
        )
        await self._record_node_execution(
            "load_company_profile", ["applicant_id"], ["company_profile"], ms
        )
        return {**state, "company_profile": company_profile, "requested_amount_usd": requested_amount}

    async def _evaluate_rule(self, state: ComplianceState, rule_id: str) -> ComplianceState:
        t = time.perf_counter()
        app_id = state["application_id"]
        reg = REGULATIONS[rule_id]
        co = dict(state.get("company_profile") or {})
        co["requested_amount_usd"] = state.get("requested_amount_usd") or 0.0

        passes = reg["check"](co)
        evidence_hash = self._sha(f"{rule_id}-{co.get('company_id', 'unknown')}-{passes}")

        rule_result = {"rule_id": rule_id, "passed": passes, "is_hard_block": reg["is_hard_block"]}
        results = list(state.get("rule_results") or [])
        results.append(rule_result)

        if rule_id == "REG-006":
            # Always noted
            noted_event = ComplianceRuleNoted(
                application_id=app_id,
                session_id=self.session_id,
                rule_id=rule_id,
                rule_name=reg["name"],
                note_type=reg.get("note_type", "CRA_CONSIDERATION"),
                note_text=reg.get("note_text", ""),
                evaluated_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"compliance-{app_id}", [noted_event])
        elif passes:
            passed_event = ComplianceRulePassed(
                application_id=app_id,
                session_id=self.session_id,
                rule_id=rule_id,
                rule_name=reg["name"],
                rule_version=reg["version"],
                evidence_hash=evidence_hash,
                evaluation_notes=f"Rule {rule_id} passed.",
                evaluated_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"compliance-{app_id}", [passed_event])
        else:
            failed_event = ComplianceRuleFailed(
                application_id=app_id,
                session_id=self.session_id,
                rule_id=rule_id,
                rule_name=reg["name"],
                rule_version=reg["version"],
                failure_reason=reg["failure_reason"],
                is_hard_block=reg["is_hard_block"],
                remediation_available=reg.get("remediation") is not None,
                remediation_description=reg.get("remediation"),
                evidence_hash=evidence_hash,
                evaluated_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"compliance-{app_id}", [failed_event])

        new_state = {**state, "rule_results": results}
        if not passes and reg["is_hard_block"]:
            new_state["has_hard_block"] = True
            new_state["block_rule_id"] = rule_id

        ms = int((time.perf_counter() - t) * 1000)
        node_name = f"evaluate_{rule_id.lower().replace('-', '_')}"
        await self._record_node_execution(
            node_name, ["company_profile"], [f"{rule_id}_result"], ms
        )
        return new_state

    async def _node_write_output(self, state: ComplianceState) -> ComplianceState:
        t = time.perf_counter()
        app_id = state["application_id"]
        results = state.get("rule_results") or []
        has_hard_block = state.get("has_hard_block", False)
        block_rule_id = state.get("block_rule_id")

        passed_count = sum(1 for r in results if r.get("passed") and r["rule_id"] != "REG-006")
        failed_count = sum(1 for r in results if not r.get("passed") and r["rule_id"] != "REG-006")
        noted_count = sum(1 for r in results if r["rule_id"] == "REG-006")

        if has_hard_block:
            overall_verdict = ComplianceVerdict.BLOCKED
        elif failed_count > 0:
            overall_verdict = ComplianceVerdict.CONDITIONAL
        else:
            overall_verdict = ComplianceVerdict.CLEAR

        completed_event = ComplianceCheckCompleted(
            application_id=app_id,
            session_id=self.session_id,
            rules_evaluated=len(results),
            rules_passed=passed_count,
            rules_failed=failed_count,
            rules_noted=noted_count,
            has_hard_block=has_hard_block,
            overall_verdict=overall_verdict,
            completed_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"compliance-{app_id}", [completed_event])

        events_written = [{"stream_id": f"compliance-{app_id}", "event_type": "ComplianceCheckCompleted"}]

        if has_hard_block:
            decline_event = ApplicationDeclined(
                application_id=app_id,
                decline_reasons=[
                    f"Compliance hard block: {block_rule_id} - "
                    f"{REGULATIONS.get(block_rule_id, {}).get('failure_reason', '')}"
                ],
                declined_by=f"compliance-agent-{self.session_id}",
                adverse_action_notice_required=True,
                adverse_action_codes=[block_rule_id or "COMPLIANCE_BLOCK"],
                declined_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [decline_event])
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "ApplicationDeclined"})
        else:
            decision_trigger = DecisionRequested(
                application_id=app_id,
                requested_at=datetime.now(),
                all_analyses_complete=True,
                triggered_by_event_id=self.session_id,
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [decision_trigger])
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "DecisionRequested"})

        await self._record_output_written(
            events_written,
            f"Compliance: {overall_verdict.value}, {len(results)} rules evaluated.",
        )
        ms = int((time.perf_counter() - t) * 1000)
        await self._record_node_execution(
            "write_output", ["rule_results"], ["events_written"], ms
        )
        return {
            **state,
            "output_events": events_written,
            "next_agent": None if has_hard_block else "decision_orchestrator",
        }


# ─── DECISION ORCHESTRATOR ────────────────────────────────────────────────────

class OrchestratorState(TypedDict):
    application_id: str
    session_id: str
    credit_result: dict | None
    fraud_result: dict | None
    compliance_result: dict | None
    recommendation: str | None
    confidence: float | None
    approved_amount: float | None
    executive_summary: str | None
    conditions: list[str] | None
    hard_constraints_applied: list[str] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


ORCHESTRATOR_SYSTEM_PROMPT = """You are a senior loan officer synthesising multi-agent analysis.

Given credit analysis, fraud screening, and compliance results, produce a recommendation.

Return ONLY this JSON:
{
  "recommendation": "APPROVE"|"DECLINE"|"REFER",
  "confidence": <float 0.0-1.0>,
  "approved_amount_usd": <integer or null>,
  "conditions": ["<condition>"],
  "executive_summary": "<3-5 sentences for the loan file>",
  "key_risks": ["<risk>"],
  "contributing_sessions": ["<session_id>"]
}

If recommending APPROVE: set approved_amount_usd to the credit limit.
If recommending DECLINE or REFER: set approved_amount_usd to null."""


class DecisionOrchestratorAgent(BaseApexAgent):
    """
    Synthesises credit, fraud, and compliance results into a final recommendation.
    Hard constraints are enforced in Python after the LLM recommendation.

    Nodes:
        validate_inputs -> load_credit_result -> load_fraud_result ->
        load_compliance_result -> synthesize_decision -> apply_hard_constraints ->
        write_output
    """

    def build_graph(self):
        g = StateGraph(OrchestratorState)
        g.add_node("validate_inputs",        self._node_validate_inputs)
        g.add_node("load_credit_result",     self._node_load_credit)
        g.add_node("load_fraud_result",      self._node_load_fraud)
        g.add_node("load_compliance_result", self._node_load_compliance)
        g.add_node("synthesize_decision",    self._node_synthesize)
        g.add_node("apply_hard_constraints", self._node_constraints)
        g.add_node("write_output",           self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",        "load_credit_result")
        g.add_edge("load_credit_result",     "load_fraud_result")
        g.add_edge("load_fraud_result",      "load_compliance_result")
        g.add_edge("load_compliance_result", "synthesize_decision")
        g.add_edge("synthesize_decision",    "apply_hard_constraints")
        g.add_edge("apply_hard_constraints", "write_output")
        g.add_edge("write_output",           END)
        return g.compile()

    def _initial_state(self, application_id: str) -> OrchestratorState:
        return OrchestratorState(
            application_id=application_id, session_id=self.session_id,
            credit_result=None, fraud_result=None, compliance_result=None,
            recommendation=None, confidence=None, approved_amount=None,
            executive_summary=None, conditions=None, hard_constraints_applied=[],
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: OrchestratorState) -> OrchestratorState:
        t = time.perf_counter()
        app_id = state["application_id"]
        errors = []

        try:
            loan_events = await self.store.load_stream(f"loan-{app_id}")
        except Exception as exc:
            errors.append(f"Failed to load loan stream: {exc}")
            await self._record_input_failed([], errors)
            raise ValueError(f"Input validation failed: {errors}")

        has_trigger = any(e["event_type"] == "DecisionRequested" for e in loan_events)
        if not has_trigger:
            errors.append("DecisionRequested event not found on loan stream")
            await self._record_input_failed(["DecisionRequested"], errors)
            raise ValueError(f"Input validation failed: {errors}")

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_input_validated(["application_id", "DecisionRequested"], ms)
        await self._record_node_execution(
            "validate_inputs", ["application_id"], ["validated"], ms
        )
        return {**state, "errors": errors}

    async def _node_load_credit(self, state: OrchestratorState) -> OrchestratorState:
        t = time.perf_counter()
        app_id = state["application_id"]

        credit_events = await self.store.load_stream(f"credit-{app_id}")
        credit_result: dict = {}
        for ev in reversed(credit_events):
            if ev["event_type"] == "CreditAnalysisCompleted":
                payload = ev.get("payload", {})
                decision = payload.get("decision") or {}
                credit_result = {
                    "risk_tier": decision.get("risk_tier", "MEDIUM"),
                    "recommended_limit_usd": decision.get("recommended_limit_usd", 0),
                    "confidence": float(decision.get("confidence", 0.5)),
                    "rationale": decision.get("rationale", ""),
                    "data_quality_caveats": decision.get("data_quality_caveats", []),
                    "session_id": payload.get("session_id", ""),
                }
                break

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_tool_call(
            "load_event_store_stream",
            f"stream_id=credit-{app_id} filter=CreditAnalysisCompleted",
            f"risk_tier={credit_result.get('risk_tier')}, confidence={credit_result.get('confidence')}",
            ms,
        )
        await self._record_node_execution(
            "load_credit_result", ["credit_stream"], ["credit_result"], ms
        )
        return {**state, "credit_result": credit_result}

    async def _node_load_fraud(self, state: OrchestratorState) -> OrchestratorState:
        t = time.perf_counter()
        app_id = state["application_id"]

        fraud_events = await self.store.load_stream(f"fraud-{app_id}")
        fraud_result: dict = {}
        for ev in reversed(fraud_events):
            if ev["event_type"] == "FraudScreeningCompleted":
                p = ev.get("payload", {})
                fraud_result = {
                    "fraud_score": float(p.get("fraud_score", 0.0)),
                    "risk_level": p.get("risk_level", "LOW"),
                    "recommendation": p.get("recommendation", "PROCEED"),
                    "anomalies_found": p.get("anomalies_found", 0),
                    "session_id": p.get("session_id", ""),
                }
                break

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_tool_call(
            "load_event_store_stream",
            f"stream_id=fraud-{app_id} filter=FraudScreeningCompleted",
            f"fraud_score={fraud_result.get('fraud_score')}, risk={fraud_result.get('risk_level')}",
            ms,
        )
        await self._record_node_execution(
            "load_fraud_result", ["fraud_stream"], ["fraud_result"], ms
        )
        return {**state, "fraud_result": fraud_result}

    async def _node_load_compliance(self, state: OrchestratorState) -> OrchestratorState:
        t = time.perf_counter()
        app_id = state["application_id"]

        compliance_events = await self.store.load_stream(f"compliance-{app_id}")
        compliance_result: dict = {}
        for ev in reversed(compliance_events):
            if ev["event_type"] == "ComplianceCheckCompleted":
                p = ev.get("payload", {})
                compliance_result = {
                    "overall_verdict": p.get("overall_verdict", "CLEAR"),
                    "has_hard_block": p.get("has_hard_block", False),
                    "rules_evaluated": p.get("rules_evaluated", 0),
                    "rules_failed": p.get("rules_failed", 0),
                    "session_id": p.get("session_id", ""),
                }
                break

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_tool_call(
            "load_event_store_stream",
            f"stream_id=compliance-{app_id} filter=ComplianceCheckCompleted",
            f"verdict={compliance_result.get('overall_verdict')}",
            ms,
        )
        await self._record_node_execution(
            "load_compliance_result", ["compliance_stream"], ["compliance_result"], ms
        )
        return {**state, "compliance_result": compliance_result}

    async def _node_synthesize(self, state: OrchestratorState) -> OrchestratorState:
        t = time.perf_counter()
        credit = state.get("credit_result") or {}
        fraud = state.get("fraud_result") or {}
        compliance = state.get("compliance_result") or {}

        user_message = f"""CREDIT ANALYSIS:
Risk Tier: {credit.get('risk_tier', 'UNKNOWN')}
Recommended Limit: ${float(credit.get('recommended_limit_usd') or 0):,.0f}
Confidence: {credit.get('confidence', 0.0):.0%}
Rationale: {credit.get('rationale', 'N/A')}
Data Quality Caveats: {credit.get('data_quality_caveats', [])}

FRAUD SCREENING:
Score: {fraud.get('fraud_score', 0.0):.2f}
Risk Level: {fraud.get('risk_level', 'UNKNOWN')}
Anomalies Found: {fraud.get('anomalies_found', 0)}
Recommendation: {fraud.get('recommendation', 'UNKNOWN')}

COMPLIANCE:
Verdict: {compliance.get('overall_verdict', 'UNKNOWN')}
Hard Block: {compliance.get('has_hard_block', False)}
Rules Failed: {compliance.get('rules_failed', 0)} of {compliance.get('rules_evaluated', 0)}

Synthesise these three analyses into a final recommendation."""

        ti = to = 0
        cost = 0.0
        decision: dict = {
            "recommendation": "REFER",
            "confidence": 0.5,
            "approved_amount_usd": None,
            "conditions": [],
            "executive_summary": "Analysis synthesis failed; human review required.",
            "key_risks": [],
            "contributing_sessions": [],
        }
        try:
            content, ti, to, cost = await self._call_llm(ORCHESTRATOR_SYSTEM_PROMPT, user_message, max_tokens=1024)
            decision = self._parse_json(content)
        except Exception as exc:
            decision["executive_summary"] = f"LLM synthesis failed: {exc!s:.100}"

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_node_execution(
            "synthesize_decision",
            ["credit_result", "fraud_result", "compliance_result"],
            ["recommendation", "confidence", "executive_summary"],
            ms, ti, to, cost,
        )
        return {
            **state,
            "recommendation": decision.get("recommendation", "REFER"),
            "confidence": float(decision.get("confidence", 0.5)),
            "approved_amount": decision.get("approved_amount_usd"),
            "executive_summary": decision.get("executive_summary", ""),
            "conditions": decision.get("conditions", []),
        }

    async def _node_constraints(self, state: OrchestratorState) -> OrchestratorState:
        t = time.perf_counter()
        compliance = state.get("compliance_result") or {}
        fraud = state.get("fraud_result") or {}
        credit = state.get("credit_result") or {}

        recommendation = state.get("recommendation") or "REFER"
        confidence = state.get("confidence") or 0.5
        constraints_applied: list[str] = list(state.get("hard_constraints_applied") or [])

        # Constraint 1: compliance BLOCKED -> DECLINE
        if compliance.get("overall_verdict") == "BLOCKED" or compliance.get("has_hard_block"):
            if recommendation != "DECLINE":
                recommendation = "DECLINE"
                constraints_applied.append("COMPLIANCE_BLOCKED_FORCED_DECLINE")

        # Constraint 2: fraud_score > 0.60 -> REFER
        if float(fraud.get("fraud_score") or 0.0) > 0.60 and recommendation == "APPROVE":
            recommendation = "REFER"
            constraints_applied.append("HIGH_FRAUD_SCORE_FORCED_REFER")

        # Constraint 3: confidence < 0.60 -> REFER
        if float(confidence) < 0.60 and recommendation == "APPROVE":
            recommendation = "REFER"
            constraints_applied.append("LOW_CONFIDENCE_FORCED_REFER")

        # Constraint 4: HIGH risk tier + confidence < 0.70 -> REFER
        if (credit.get("risk_tier") == "HIGH"
                and float(confidence) < 0.70
                and recommendation == "APPROVE"):
            recommendation = "REFER"
            constraints_applied.append("HIGH_RISK_LOW_CONFIDENCE_FORCED_REFER")

        ms = int((time.perf_counter() - t) * 1000)
        await self._record_node_execution(
            "apply_hard_constraints",
            ["recommendation", "confidence", "compliance_result", "fraud_result"],
            ["recommendation", "hard_constraints_applied"],
            ms,
        )
        return {
            **state,
            "recommendation": recommendation,
            "hard_constraints_applied": constraints_applied,
        }

    async def _node_write_output(self, state: OrchestratorState) -> OrchestratorState:
        t = time.perf_counter()
        app_id = state["application_id"]
        recommendation = state.get("recommendation") or "REFER"
        confidence = state.get("confidence") or 0.5
        approved_amount = state.get("approved_amount")
        executive_summary = state.get("executive_summary") or ""
        conditions = state.get("conditions") or []
        constraints = state.get("hard_constraints_applied") or []
        credit = state.get("credit_result") or {}
        fraud = state.get("fraud_result") or {}
        compliance = state.get("compliance_result") or {}

        contributing = [
            sid for sid in [
                credit.get("session_id"), fraud.get("session_id"), compliance.get("session_id")
            ] if sid
        ]

        decision_event = DecisionGenerated(
            application_id=app_id,
            orchestrator_session_id=self.session_id,
            recommendation=recommendation,
            confidence=confidence,
            approved_amount_usd=Decimal(str(approved_amount)) if approved_amount else None,
            conditions=conditions,
            executive_summary=executive_summary,
            key_risks=[],
            contributing_sessions=contributing,
            model_versions={
                "credit_model": credit.get("session_id", ""),
                "fraud_model": fraud.get("session_id", ""),
            },
            generated_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"loan-{app_id}", [decision_event])

        events_written = [{"stream_id": f"loan-{app_id}", "event_type": "DecisionGenerated"}]

        if recommendation == "APPROVE":
            approved_amt = approved_amount or credit.get("recommended_limit_usd") or 0
            approve_event = ApplicationApproved(
                application_id=app_id,
                approved_amount_usd=Decimal(str(approved_amt)),
                interest_rate_pct=7.5,
                term_months=60,
                conditions=conditions,
                approved_by=f"orchestrator-{self.session_id}",
                effective_date=datetime.now().strftime("%Y-%m-%d"),
                approved_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [approve_event])
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "ApplicationApproved"})

        elif recommendation == "DECLINE":
            decline_reasons = [f"Orchestrator decision: {recommendation}"]
            if constraints:
                decline_reasons.extend(constraints)
            decline_event = ApplicationDeclined(
                application_id=app_id,
                decline_reasons=decline_reasons,
                declined_by=f"orchestrator-{self.session_id}",
                adverse_action_notice_required=True,
                adverse_action_codes=["AUTOMATED_DECLINE"],
                declined_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [decline_event])
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "ApplicationDeclined"})

        elif recommendation == "REFER":
            review_event = HumanReviewRequested(
                application_id=app_id,
                reason=f"Automated referral: {constraints or ['confidence threshold not met']}",
                decision_event_id=self.session_id,
                requested_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [review_event])
            events_written.append({"stream_id": f"loan-{app_id}", "event_type": "HumanReviewRequested"})

        await self._record_output_written(
            events_written,
            f"Decision: {recommendation}, confidence={confidence:.0%}, constraints={constraints}",
        )
        ms = int((time.perf_counter() - t) * 1000)
        await self._record_node_execution(
            "write_output", ["recommendation", "confidence"], ["events_written"], ms
        )
        return {**state, "output_events": events_written, "next_agent": None}
