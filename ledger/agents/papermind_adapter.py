"""
ledger/agents/papermind_adapter.py
====================================
HTTP client adapter for the paperMind-ai extraction microservice.

The paperMind-ai pipeline runs under Python 3.11 (its own venv) and is
exposed as a FastAPI service on PAPERMIND_API_URL (default http://localhost:7861).
This adapter calls that service over HTTP, avoiding the Python 3.11/3.12 ABI
mismatch that made the previous sys.path approach fail silently.

Interface is unchanged:
    adapter = PaperMindAdapter()
    facts, confidence = await adapter.extract(pdf_path, doc_id)
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import httpx

from ledger.schema.events import FinancialFacts

PAPERMIND_API_URL = os.environ.get("PAPERMIND_API_URL", "http://localhost:7861")
# PDF processing is CPU-bound and can take several seconds.
_TIMEOUT = httpx.Timeout(120.0, connect=5.0)

# Field names the service returns; used to build field_confidence.
_FIELDS = [
    "total_revenue", "gross_profit", "operating_expenses", "operating_income",
    "ebitda", "net_income", "total_assets", "current_assets", "cash_and_equivalents",
    "accounts_receivable", "inventory", "total_liabilities", "current_liabilities",
    "long_term_debt", "total_equity", "operating_cash_flow", "investing_cash_flow",
    "financing_cash_flow",
]


class PaperMindAdapter:
    """
    Async HTTP client for the paperMind-ai extraction service.

    Precondition: the service is running at PAPERMIND_API_URL.
    If the service is unreachable or returns an error, extract() returns
    an empty FinancialFacts with extraction_notes describing the failure.
    Callers (document processing agent) surface this as a quality flag,
    not a silent zero.
    """

    async def extract(
        self, pdf_path: str, doc_id: str
    ) -> tuple[FinancialFacts, float]:
        """
        Extract financial facts from a PDF via the paperMind service.

        Returns:
            (FinancialFacts, overall_confidence) where any field not found
            is None and field_confidence[field] == 0.0 for missing fields.

        Raises:
            Never. All failures are captured in extraction_notes so the
            caller can decide whether to defer or proceed with partial data.
        """
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(
                    f"{PAPERMIND_API_URL}/extract",
                    json={"pdf_path": pdf_path, "doc_id": doc_id},
                )
                resp.raise_for_status()
                data = resp.json()
        except httpx.ConnectError:
            return self._empty(f"paperMind service unavailable at {PAPERMIND_API_URL}")
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text[:200]
            return self._empty(f"paperMind service error {exc.response.status_code}: {detail}")
        except Exception as exc:
            return self._empty(f"paperMind request failed: {exc!s:.200}")

        return self._build_facts(data)

    def _build_facts(self, data: dict[str, Any]) -> tuple[FinancialFacts, float]:
        """Convert the service response into a FinancialFacts instance."""
        raw_facts: dict[str, float | None] = data.get("facts", {})
        pipeline_confidence: float = float(data.get("confidence", 0.0))
        service_notes: list[str] = data.get("extraction_notes", [])

        field_values: dict[str, Any] = {}
        field_confidence: dict[str, float] = {}
        extraction_notes: list[str] = list(service_notes)

        for field_name in _FIELDS:
            raw = raw_facts.get(field_name)
            if raw is not None:
                field_values[field_name] = Decimal(str(raw))
                field_confidence[field_name] = pipeline_confidence
            else:
                field_values[field_name] = None
                field_confidence[field_name] = 0.0

        facts = FinancialFacts(
            **field_values,
            field_confidence=field_confidence,
            extraction_notes=extraction_notes,
        )
        return facts, pipeline_confidence

    @staticmethod
    def _empty(reason: str) -> tuple[FinancialFacts, float]:
        facts = FinancialFacts(
            field_confidence={f: 0.0 for f in _FIELDS},
            extraction_notes=[reason],
        )
        return facts, 0.0
