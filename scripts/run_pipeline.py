"""
scripts/run_pipeline.py — Process one application through all agents end-to-end.

Usage:
    uv run python3 scripts/run_pipeline.py --application COMP-001
    uv run python3 scripts/run_pipeline.py --application COMP-001 --phase credit
    uv run python3 scripts/run_pipeline.py --application COMP-001 --postgres

Phases: all | document | credit | fraud | compliance | decision

Modes:
    default   — InMemoryEventStore + JSON-backed registry (no DB required)
    --postgres — PostgreSQL EventStore + ApplicantRegistryClient (requires docker-compose up)
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from ledger.event_store import InMemoryEventStore
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent
from ledger.agents.stub_agents import (
    DocumentProcessingAgent, FraudDetectionAgent,
    ComplianceAgent, DecisionOrchestratorAgent,
)
from ledger.schema.events import (
    ExtractionStarted, ExtractionCompleted, FinancialFacts, DocumentType,
)

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
DOCUMENTS_DIR = ROOT / "documents"


# ─── JSON-BACKED REGISTRY ADAPTER ────────────────────────────────────────────

@dataclass
class _CompanyProfile:
    company_id: str
    name: str
    industry: str
    jurisdiction: str
    legal_type: str
    trajectory: str
    risk_segment: str
    naics: str = ""
    founded_year: int = 2015
    employee_count: int = 50
    submission_channel: str = "online"
    ip_region: str = "US"


@dataclass
class _FinancialYear:
    fiscal_year: int
    total_revenue: float
    gross_profit: float
    operating_income: float
    ebitda: float
    net_income: float
    total_assets: float
    total_liabilities: float
    total_equity: float
    long_term_debt: float = 0.0
    cash_and_equivalents: float = 0.0
    current_assets: float = 0.0
    current_liabilities: float = 0.0
    accounts_receivable: float = 0.0
    inventory: float = 0.0
    debt_to_equity: float | None = None
    current_ratio: float | None = None
    debt_to_ebitda: float | None = None
    interest_coverage_ratio: float | None = None
    gross_margin: float | None = None
    ebitda_margin: float | None = None
    net_margin: float | None = None


class _JsonRegistryAdapter:
    """
    Read-only registry backed by data/applicant_profiles.json.
    Exposes the same async interface as ApplicantRegistryClient.
    Financial history is read from documents/<app_id>/financial_summary.csv.
    """

    def __init__(self, profiles: list[dict], docs_dir: Path):
        self._profiles = {p["company_id"]: p for p in profiles}
        self._docs_dir = docs_dir

    async def get_company(self, company_id: str) -> _CompanyProfile | None:
        p = self._profiles.get(company_id)
        if not p:
            return None
        return _CompanyProfile(
            company_id=p["company_id"],
            name=p["name"],
            industry=p["industry"],
            jurisdiction=p["jurisdiction"],
            legal_type=p["legal_type"],
            trajectory=p["trajectory"],
            risk_segment=p["risk_segment"],
        )

    async def get_financial_history(self, company_id: str, years: list[int] | None = None) -> list[_FinancialYear]:
        csv_path = self._docs_dir / company_id / "financial_summary.csv"
        if not csv_path.exists():
            return []
        facts = _parse_financial_csv(csv_path)
        fiscal_year = facts.get("fiscal_year", 2024)
        if years and fiscal_year not in years:
            return []
        return [_FinancialYear(
            fiscal_year=fiscal_year,
            total_revenue=float(facts.get("total_revenue", 0)),
            gross_profit=float(facts.get("gross_profit", 0)),
            operating_income=float(facts.get("operating_income", 0)),
            ebitda=float(facts.get("ebitda", 0)),
            net_income=float(facts.get("net_income", 0)),
            total_assets=float(facts.get("total_assets", 0)),
            total_liabilities=float(facts.get("total_liabilities", 0)),
            total_equity=float(facts.get("total_equity", 0)),
            long_term_debt=float(facts.get("long_term_debt", 0)),
            cash_and_equivalents=float(facts.get("cash_and_equivalents", 0)),
            current_assets=float(facts.get("current_assets", 0)),
            current_liabilities=float(facts.get("current_liabilities", 0)),
            accounts_receivable=float(facts.get("accounts_receivable", 0)),
            inventory=float(facts.get("inventory", 0)),
        )]

    async def get_compliance_flags(self, company_id: str, active_only: bool = False) -> list:
        p = self._profiles.get(company_id, {})
        flags = p.get("compliance_flags", [])
        if active_only:
            flags = [f for f in flags if f.get("is_active", True)]
        return flags

    async def get_loan_relationships(self, company_id: str) -> list[dict]:
        return []


# ─── CSV / SEEDING HELPERS ────────────────────────────────────────────────────

def _parse_financial_csv(csv_path: Path) -> dict:
    """Parse financial_summary.csv into a flat dict of {field: value}."""
    facts: dict[str, Any] = {}
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            field = row["field"]
            value = row["value"]
            fiscal_year = row.get("fiscal_year")
            if fiscal_year:
                facts["fiscal_year"] = int(fiscal_year)
            try:
                facts[field] = Decimal(value)
            except Exception:
                facts[field] = value
    return facts


def _build_financial_facts(app_id: str) -> FinancialFacts:
    """Build FinancialFacts from financial_summary.csv. Missing fields stay None."""
    csv_path = DOCUMENTS_DIR / app_id / "financial_summary.csv"
    if not csv_path.exists():
        print(f"  [warn] {csv_path} not found — using empty FinancialFacts")
        return FinancialFacts()

    raw = _parse_financial_csv(csv_path)
    decimal_fields = [
        "total_revenue", "gross_profit", "operating_expenses", "operating_income",
        "ebitda", "depreciation_amortization", "interest_expense", "income_before_tax",
        "tax_expense", "net_income", "total_assets", "current_assets",
        "cash_and_equivalents", "accounts_receivable", "inventory",
        "total_liabilities", "current_liabilities", "long_term_debt", "total_equity",
        "operating_cash_flow", "investing_cash_flow", "financing_cash_flow", "free_cash_flow",
    ]
    float_fields = [
        "debt_to_equity", "current_ratio", "debt_to_ebitda",
        "interest_coverage", "gross_margin", "net_margin",
    ]
    kwargs: dict[str, Any] = {}
    for f in decimal_fields:
        if f in raw:
            kwargs[f] = raw[f]
    for f in float_fields:
        if f in raw:
            kwargs[f] = float(raw[f])

    fiscal_year = raw.get("fiscal_year", 2024)
    kwargs["fiscal_year_end"] = f"{fiscal_year}-12-31"
    kwargs["field_confidence"] = {k: 0.92 for k in kwargs if k in decimal_fields}
    return FinancialFacts(**kwargs)


async def _seed_streams(store, app_id: str, applicant_id: str, profile: _CompanyProfile | None) -> None:
    """Seed loan stream (ApplicationSubmitted + DocumentUploaded x3) and docpkg stream (ExtractionCompleted x2)."""
    now = datetime.now()
    company_name = profile.name if profile else applicant_id
    jurisdiction = profile.jurisdiction if profile else "TX"

    # --- loan stream ---
    submitted = {
        "event_type": "ApplicationSubmitted", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "applicant_id": applicant_id,
            "requested_amount_usd": "500000",
            "loan_purpose": "working_capital",
            "loan_term_months": 60,
            "submission_channel": "online",
            "contact_email": f"cfo@{applicant_id.lower()}.example.com",
            "contact_name": company_name,
            "application_reference": f"REF-{app_id}",
            "submitted_at": now.isoformat(),
        },
    }
    await store.append(f"loan-{app_id}", [submitted], expected_version=-1)

    doc_files = {
        "income_statement": f"income_statement_2024.pdf",
        "balance_sheet": f"balance_sheet_2024.pdf",
        "application_proposal": f"application_proposal.pdf",
    }
    for doc_type, filename in doc_files.items():
        file_path = str(DOCUMENTS_DIR / applicant_id / filename)
        uploaded = {
            "event_type": "DocumentUploaded", "event_version": 1,
            "payload": {
                "application_id": app_id,
                "document_id": f"doc-{doc_type}-{app_id}",
                "document_type": doc_type,
                "document_format": "pdf",
                "filename": filename,
                "file_path": file_path,
                "file_size_bytes": 4096,
                "file_hash": f"sha256-{doc_type}-{app_id}",
                "fiscal_year": 2024,
                "uploaded_at": now.isoformat(),
                "uploaded_by": applicant_id,
            },
        }
        ver = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [uploaded], expected_version=ver)

    # FraudScreeningRequested trigger (needed by downstream agents)
    ver = await store.stream_version(f"loan-{app_id}")
    await store.append(f"loan-{app_id}", [{
        "event_type": "FraudScreeningRequested", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "requested_at": now.isoformat(),
            "triggered_by_event_id": "seed",
        },
    }], expected_version=ver)

    # --- docpkg stream (extraction results from real CSV) ---
    facts = _build_financial_facts(applicant_id)
    for doc_type in ["income_statement", "balance_sheet"]:
        start = ExtractionStarted(
            package_id=f"docpkg-{app_id}",
            document_id=f"doc-{doc_type}-{app_id}",
            document_type=DocumentType(doc_type),
            pipeline_version="veritas-1.0",
            extraction_model="csv_reader",
            started_at=now,
        ).to_store_dict()
        ver = await store.stream_version(f"docpkg-{app_id}")
        await store.append(f"docpkg-{app_id}", [start], expected_version=ver)

        complete = ExtractionCompleted(
            package_id=f"docpkg-{app_id}",
            document_id=f"doc-{doc_type}-{app_id}",
            document_type=DocumentType(doc_type),
            facts=facts,
            raw_text_length=5000,
            tables_extracted=3,
            processing_ms=200,
            completed_at=now,
        ).to_store_dict()
        ver = await store.stream_version(f"docpkg-{app_id}")
        await store.append(f"docpkg-{app_id}", [complete], expected_version=ver)


# ─── OUTPUT HELPERS ───────────────────────────────────────────────────────────

def _print_event_log(store: InMemoryEventStore, app_id: str) -> None:
    all_events = sorted(store._global, key=lambda e: e["global_position"])
    print(f"\n{'='*72}")
    print(f"EVENT LOG — {app_id}  ({len(all_events)} events total)")
    print(f"{'='*72}")
    for ev in all_events:
        print(
            f"  gpos={ev['global_position']:04d}  "
            f"stream={ev['stream_id']:48s}  "
            f"{ev['event_type']}"
        )
    print(f"{'='*72}\n")


def _print_summary(
    app_id: str,
    store: InMemoryEventStore,
    agent_timings: list[tuple[str, float]],
    total_elapsed: float,
) -> None:
    loan_events = store._streams.get(f"loan-{app_id}", [])
    decision = next((e for e in reversed(loan_events) if e["event_type"] == "DecisionGenerated"), None)
    approved = next((e for e in reversed(loan_events) if e["event_type"] == "ApplicationApproved"), None)
    declined = next((e for e in reversed(loan_events) if e["event_type"] == "ApplicationDeclined"), None)

    if approved:
        outcome = f"APPROVED  amount={approved['payload'].get('approved_amount_usd', '?')}"
    elif declined:
        reasons = declined['payload'].get('decline_reasons') or ['?']
        outcome = f"DECLINED  reason={'; '.join(reasons)}"
    elif decision:
        outcome = f"DECISION={decision['payload'].get('recommendation', '?')}  confidence={decision['payload'].get('confidence', '?')}"
    else:
        outcome = "no final decision recorded"

    credit_events = store._streams.get(f"credit-{app_id}", [])
    credit_done = next((e for e in reversed(credit_events) if e["event_type"] == "CreditAnalysisCompleted"), None)
    _credit_decision = (credit_done["payload"].get("decision") or {}) if credit_done else {}
    risk_tier = _credit_decision.get("risk_tier", "?")
    credit_limit = _credit_decision.get("recommended_limit_usd", "?")

    fraud_events = store._streams.get(f"fraud-{app_id}", [])
    fraud_done = next((e for e in reversed(fraud_events) if e["event_type"] == "FraudScreeningCompleted"), None)
    fraud_score = fraud_done["payload"].get("fraud_score", "?") if fraud_done else "?"

    compliance_events = store._streams.get(f"compliance-{app_id}", [])
    compliance_done = next((e for e in reversed(compliance_events) if e["event_type"] == "ComplianceCheckCompleted"), None)
    compliance_verdict = compliance_done["payload"].get("overall_verdict", "?") if compliance_done else "?"

    total_events = sum(len(v) for v in store._streams.values())

    print(f"\n{'='*72}")
    print(f"PIPELINE SUMMARY — {app_id}")
    print(f"{'='*72}")
    print(f"  Outcome:           {outcome}")
    print(f"  Risk tier:         {risk_tier}")
    print(f"  Credit limit:      {credit_limit}")
    print(f"  Fraud score:       {fraud_score}")
    print(f"  Compliance:        {compliance_verdict}")
    print(f"  Total events:      {total_events}")
    print()
    print(f"  Agent timings:")
    for name, elapsed in agent_timings:
        print(f"    {name:32s}  {elapsed:.2f}s")
    print(f"    {'TOTAL':32s}  {total_elapsed:.2f}s")
    print(f"{'='*72}\n")


# ─── LLM PATCH HELPER (mirrors demo_narr05) ───────────────────────────────────

def _patch_llm_if_needed(agent):
    """No-op context manager when API key present; patches _call_llm for stubs otherwise."""
    import os
    from unittest.mock import patch as _patch, AsyncMock

    if os.getenv("GEMINI_API_KEY") or os.getenv("ANTHROPIC_API_KEY"):
        class _Noop:
            def __enter__(self): return self
            def __exit__(self, *a): pass
        return _Noop()

    fraud_resp = json.dumps({"fraud_score": 0.08, "anomalies": [], "recommendation": "PROCEED"})
    orch_resp = json.dumps({
        "recommendation": "REFER",
        "confidence": 0.65,
        "approved_amount_usd": None,
        "conditions": [],
        "executive_summary": "Stub orchestrator decision.",
        "key_risks": [],
        "contributing_sessions": [],
    })
    # Pick response by agent type
    is_fraud = hasattr(agent, "_node_analyze") and "fraud" in agent.agent_type
    text = fraud_resp if is_fraud else orch_resp
    return _patch.object(type(agent), "_call_llm", return_value=(text, 100, 50, 0.001))


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run one application through the Veritas ledger pipeline."
    )
    parser.add_argument("--application", required=True, help="Application ID, e.g. COMP-001")
    parser.add_argument(
        "--phase", default="all",
        choices=["all", "document", "credit", "fraud", "compliance", "decision"],
        help="Which phase to run (default: all)",
    )
    parser.add_argument(
        "--postgres", action="store_true",
        help="Use PostgreSQL EventStore instead of InMemoryEventStore",
    )
    parser.add_argument(
        "--db-url", default=os.environ.get("DATABASE_URL", "postgresql://postgres:apex@localhost:5433/apex_ledger"),
        help="PostgreSQL connection string (used with --postgres)",
    )
    args = parser.parse_args()

    app_id = args.application
    # applicant_id matches the documents/ folder name (same as app_id for COMP-XXX)
    applicant_id = app_id

    t_start = time.perf_counter()
    print(f"[pipeline] application={app_id}  phase={args.phase}  store={'postgres' if args.postgres else 'memory'}")

    # ── Store and registry setup ──────────────────────────────────────────────
    if args.postgres:
        import asyncpg
        from ledger.event_store import EventStore
        from ledger.registry.client import ApplicantRegistryClient

        pool = await asyncpg.create_pool(args.db_url, min_size=2, max_size=5)
        store = EventStore(args.db_url)
        await store.connect()
        registry = ApplicantRegistryClient(pool)
        profile = await registry.get_company(applicant_id)
    else:
        store = InMemoryEventStore()
        profiles_path = DATA_DIR / "applicant_profiles.json"
        profiles = json.loads(profiles_path.read_text()) if profiles_path.exists() else []
        registry = _JsonRegistryAdapter(profiles, DOCUMENTS_DIR)
        profile = await registry.get_company(applicant_id)

    if profile is None:
        print(f"[pipeline] warning: no registry entry for {applicant_id} — using bare defaults")

    # ── Seed streams ─────────────────────────────────────────────────────────
    print(f"[pipeline] seeding loan and docpkg streams for {app_id}...")
    await _seed_streams(store, app_id, applicant_id, profile)

    # ── Build agent chain based on phase ─────────────────────────────────────
    agent_timings: list[tuple[str, float]] = []

    def _agent_run(name: str, agent_cls, agent_type: str, extra_patch: bool = False):
        """Return an async coroutine that times and runs one agent."""
        async def _run():
            agent = agent_cls(
                agent_id=f"{agent_type}-{app_id}",
                agent_type=agent_type,
                store=store,
                registry=registry,
            )
            t0 = time.perf_counter()
            print(f"[pipeline] running {name}...")
            with _patch_llm_if_needed(agent):
                await agent.process_application(app_id)
            elapsed = time.perf_counter() - t0
            agent_timings.append((name, elapsed))
            print(f"  {name} done in {elapsed:.2f}s  session={agent.session_id}")
        return _run()

    phase = args.phase
    phases_to_run = {
        "all":        ["document", "credit", "fraud", "compliance", "decision"],
        "document":   ["document"],
        "credit":     ["credit"],
        "fraud":      ["fraud"],
        "compliance": ["compliance"],
        "decision":   ["decision"],
    }[phase]

    agent_map = {
        "document":   (DocumentProcessingAgent,    "document_processing"),
        "credit":     (CreditAnalysisAgent,         "credit_analysis"),
        "fraud":      (FraudDetectionAgent,         "fraud_detection"),
        "compliance": (ComplianceAgent,             "compliance"),
        "decision":   (DecisionOrchestratorAgent,   "decision_orchestrator"),
    }

    for p in phases_to_run:
        cls, atype = agent_map[p]
        await _agent_run(cls.__name__, cls, atype)

    total_elapsed = time.perf_counter() - t_start

    # ── Print results ─────────────────────────────────────────────────────────
    if not args.postgres:
        _print_event_log(store, app_id)
        _print_summary(app_id, store, agent_timings, total_elapsed)
    else:
        print(f"\n[pipeline] complete in {total_elapsed:.2f}s")
        for name, elapsed in agent_timings:
            print(f"  {name}: {elapsed:.2f}s")

    # ── Cleanup ───────────────────────────────────────────────────────────────
    if args.postgres:
        await store.close()


def _suppress_httpx_cleanup_noise(loop, context):
    # AsyncHttpxClientWrapper raises AttributeError on aclose() during GC;
    # suppress the full traceback and show a single terse line instead.
    exc = context.get("exception")
    if isinstance(exc, AttributeError) and "_state" in str(exc):
        print("[pipeline] note: httpx client cleanup warning (harmless)")
        return
    loop.default_exception_handler(context)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.set_exception_handler(_suppress_httpx_cleanup_noise)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
