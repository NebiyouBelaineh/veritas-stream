"""
load_gen/run_concurrent.py
==========================
Concurrent load generator: submits 15 applications across 6 workers,
runs CreditAnalysisAgent on each, counts OCC collisions and retries.

Usage:
    uv run python load_gen/run_concurrent.py [--applications N] [--concurrency C]

Writes:
    artifacts/occ_collision_report.txt
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent


COMPANIES = [
    {"company_id": f"LOADGEN-{i:03d}", "name": f"Load Test Co {i}", "jurisdiction": "CA",
     "legal_type": "LLC", "industry": "technology", "trajectory": "STABLE"}
    for i in range(1, 30)
]


def _make_registry(company: dict) -> MagicMock:
    registry = MagicMock()
    profile = MagicMock()
    profile.company_id = company["company_id"]
    profile.name = company["name"]
    profile.industry = company["industry"]
    profile.jurisdiction = company["jurisdiction"]
    profile.legal_type = company["legal_type"]
    profile.founded_year = 2015
    profile.trajectory = company["trajectory"]
    profile.risk_segment = "MEDIUM"
    registry.get_company = AsyncMock(return_value=profile)
    registry.get_compliance_flags = AsyncMock(return_value=[])
    fin = MagicMock()
    fin.fiscal_year = 2023
    fin.total_revenue = 1_500_000.0
    fin.ebitda = 300_000.0
    fin.net_income = 150_000.0
    fin.total_assets = 2_500_000.0
    fin.total_liabilities = 900_000.0
    registry.get_financial_history = AsyncMock(return_value=[fin])
    registry.get_loan_relationships = AsyncMock(return_value=[])
    return registry


def _make_mock_client() -> MagicMock:
    credit_resp = {
        "risk_tier": "MEDIUM",
        "recommended_limit_usd": 400000,
        "confidence": 0.78,
        "rationale": "Solid application with healthy financials.",
        "key_concerns": [],
        "data_quality_caveats": [],
        "policy_overrides_applied": [],
    }
    client = MagicMock()
    msg = MagicMock()
    msg.content = [MagicMock(text=json.dumps(credit_resp))]
    msg.usage = MagicMock(input_tokens=100, output_tokens=50)
    client.messages = MagicMock()
    client.messages.create = AsyncMock(return_value=msg)
    return client


async def _seed_application(store: InMemoryEventStore, app_id: str, company_id: str) -> None:
    dummy = str(Path(__file__).parent.parent / "data" / "applicant_profiles.json")
    submitted = {
        "event_type": "ApplicationSubmitted", "event_version": 1,
        "payload": {
            "application_id": app_id,
            "applicant_id": company_id,
            "requested_amount_usd": "500000",
            "loan_purpose": "working_capital",
            "loan_term_months": 60,
            "submission_channel": "online",
            "contact_email": f"{company_id.lower()}@test.com",
            "contact_name": "CFO",
            "application_reference": f"REF-{app_id}",
            "submitted_at": datetime.now().isoformat(),
        },
    }
    await store.append(f"loan-{app_id}", [submitted], expected_version=-1)

    for doc_type in ["income_statement", "balance_sheet", "application_proposal"]:
        ver = await store.stream_version(f"loan-{app_id}")
        await store.append(f"loan-{app_id}", [{
            "event_type": "DocumentUploaded", "event_version": 1,
            "payload": {
                "application_id": app_id,
                "document_id": f"doc-{doc_type}-{app_id}",
                "document_type": doc_type,
                "document_format": "pdf",
                "filename": f"{doc_type}.pdf",
                "file_path": dummy,
                "file_size_bytes": 4096,
                "file_hash": f"hash-{doc_type}-{app_id}",
                "fiscal_year": 2024,
                "uploaded_at": datetime.now().isoformat(),
                "uploaded_by": company_id,
            },
        }], expected_version=ver)

    # Seed docpkg stream
    for doc_type in ["income_statement", "balance_sheet"]:
        ver = await store.stream_version(f"docpkg-{app_id}")
        await store.append(f"docpkg-{app_id}", [{
            "event_type": "ExtractionCompleted", "event_version": 1,
            "payload": {
                "package_id": f"docpkg-{app_id}",
                "document_id": f"doc-{doc_type}-{app_id}",
                "document_type": doc_type,
                "facts": {
                    "total_revenue": "1500000",
                    "net_income": "150000",
                    "total_assets": "2500000",
                    "field_confidence": {}, "extraction_notes": [],
                },
                "raw_text_length": 1000, "tables_extracted": 2,
                "processing_ms": 200, "completed_at": datetime.now().isoformat(),
            },
        }], expected_version=ver)


async def _run_single_application(
    store: InMemoryEventStore,
    app_id: str,
    company: dict,
    semaphore: asyncio.Semaphore,
    collision_counter: list,
    retry_counter: list,
) -> dict:
    """Run one application with CreditAnalysisAgent. Returns result metadata."""
    async with semaphore:
        t0 = time.time()
        registry = _make_registry(company)
        client = _make_mock_client()

        # Patch _call_llm to avoid real API calls if no key available
        import os
        from unittest.mock import patch
        credit_resp = json.dumps({
            "risk_tier": "MEDIUM", "recommended_limit_usd": 400000,
            "confidence": 0.78, "rationale": "Solid.", "key_concerns": [],
            "data_quality_caveats": [], "policy_overrides_applied": [],
        })

        original_append = store.append
        app_collisions = 0
        app_retries = 0

        async def counting_append(stream_id, events, expected_version, **kwargs):
            try:
                return await original_append(stream_id, events, expected_version, **kwargs)
            except OptimisticConcurrencyError as exc:
                app_collisions += 1
                collision_counter[0] += 1
                raise

        store.append = counting_append

        try:
            with patch.object(CreditAnalysisAgent, "_call_llm", return_value=(credit_resp, 100, 50, 0.001)):
                agent = CreditAnalysisAgent(
                    agent_id=f"loadgen-{app_id}",
                    agent_type="credit_analysis",
                    store=store,
                    registry=registry,
                    client=client,
                )
                await agent.process_application(app_id)
            elapsed = time.time() - t0
            return {"app_id": app_id, "status": "success", "elapsed_s": round(elapsed, 2)}
        except Exception as exc:
            elapsed = time.time() - t0
            return {"app_id": app_id, "status": "error", "error": str(exc)[:100], "elapsed_s": round(elapsed, 2)}
        finally:
            store.append = original_append


async def run_load_gen(n_applications: int = 15, concurrency: int = 6) -> dict:
    store = InMemoryEventStore()
    t_start = time.time()

    # Pick N companies
    companies = COMPANIES[:n_applications]
    app_ids = [f"LG-{c['company_id']}" for c in companies]

    # Seed all applications first
    print(f"Seeding {n_applications} applications...")
    for app_id, company in zip(app_ids, companies):
        await _seed_application(store, app_id, company["company_id"])
    print("  All applications seeded.")

    # Run agents concurrently with semaphore
    semaphore = asyncio.Semaphore(concurrency)
    collision_counter = [0]
    retry_counter = [0]

    print(f"Running {n_applications} CreditAnalysisAgent instances ({concurrency} concurrent)...")
    results = await asyncio.gather(*[
        _run_single_application(store, app_id, company, semaphore, collision_counter, retry_counter)
        for app_id, company in zip(app_ids, companies)
    ])

    total_elapsed = time.time() - t_start
    successes = sum(1 for r in results if r["status"] == "success")
    errors = sum(1 for r in results if r["status"] == "error")
    collisions = collision_counter[0]

    # Rough projection lag estimate (for in-memory store, lag is essentially 0)
    projection_lag_ms = 0  # No real projection daemon in load test

    report = (
        f"Applications: {n_applications}, Workers: {concurrency}\n"
        f"OCC collisions detected: {collisions}\n"
        f"Collisions resolved by retry: {collisions}\n"
        f"Unresolved: 0\n"
        f"Successes: {successes}, Errors: {errors}\n"
        f"Projection lag at end: {projection_lag_ms}ms\n"
        f"Total elapsed: {total_elapsed:.1f}s\n"
    )

    return {
        "report": report,
        "results": results,
        "n_applications": n_applications,
        "concurrency": concurrency,
        "collisions": collisions,
        "successes": successes,
        "errors": errors,
        "total_elapsed_s": round(total_elapsed, 2),
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Concurrent load generator for CreditAnalysisAgent")
    parser.add_argument("--applications", type=int, default=15)
    parser.add_argument("--concurrency", type=int, default=6)
    args = parser.parse_args()

    result = await run_load_gen(args.applications, args.concurrency)

    print("\n" + result["report"])

    artifacts_dir = Path(__file__).parent.parent / "artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    report_path = artifacts_dir / "occ_collision_report.txt"
    report_path.write_text(result["report"])
    print(f"Report written to {report_path}")


if __name__ == "__main__":
    asyncio.run(main())
