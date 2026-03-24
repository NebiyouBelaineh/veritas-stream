"""
ledger/agents/papermind_adapter.py
====================================
Async adapter wrapping the synchronous paperMind-ai extraction pipeline.

Exposes one async method:
    adapter = PaperMindAdapter()
    facts, confidence = await adapter.extract(pdf_path, doc_id)

The adapter prepends the paperMind-ai root to sys.path and calls the sync
pipeline components via asyncio.to_thread() to avoid blocking the event loop.
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any

# Prepend paperMind-ai root so its imports resolve
_PM_ROOT = Path(__file__).resolve().parent.parent.parent / "paperMind-ai"
if str(_PM_ROOT) not in sys.path:
    sys.path.insert(0, str(_PM_ROOT))

# Import lazily inside methods to avoid top-level import errors if paperMind-ai
# is missing from the environment at import time.


# Keyword mapping from FinancialFacts field names to search terms
FIELD_KEYWORDS: dict[str, list[str]] = {
    "total_revenue":        ["total revenue", "net sales", "revenue", "net revenue"],
    "gross_profit":         ["gross profit", "gross margin"],
    "operating_expenses":   ["operating expenses", "opex", "total operating expenses"],
    "operating_income":     ["operating income", "income from operations", "operating profit"],
    "ebitda":               ["ebitda", "earnings before interest", "ebit"],
    "net_income":           ["net income", "net earnings", "net profit", "net loss"],
    "total_assets":         ["total assets"],
    "current_assets":       ["current assets", "total current assets"],
    "cash_and_equivalents": ["cash", "cash and cash equivalents", "cash equivalents"],
    "accounts_receivable":  ["accounts receivable", "trade receivables", "receivables"],
    "inventory":            ["inventory", "inventories"],
    "total_liabilities":    ["total liabilities"],
    "current_liabilities":  ["current liabilities", "total current liabilities"],
    "long_term_debt":       ["long-term debt", "long term debt", "notes payable", "long-term liabilities"],
    "total_equity":         [
        "total equity", "shareholders equity", "stockholders equity",
        "total stockholders", "total shareholders",
    ],
    "operating_cash_flow":  ["operating activities", "cash from operations", "net cash from operating"],
    "investing_cash_flow":  ["investing activities", "cash from investing"],
    "financing_cash_flow":  ["financing activities", "cash from financing"],
}


def _scale_value(value: float, unit: str) -> float:
    """Apply unit scaling (millions, thousands, etc.)."""
    u = (unit or "").lower()
    if any(x in u for x in ("million", "mn", "m ")):
        return value * 1_000_000
    if any(x in u for x in ("billion", "bn", "b ")):
        return value * 1_000_000_000
    if any(x in u for x in ("thousand", "k")):
        return value * 1_000
    return value


def _run_pipeline_sync(pdf_path: str, doc_id: str, tmp_dir: str) -> dict[str, Any]:
    """
    Run the synchronous paperMind-ai pipeline.
    Called via asyncio.to_thread() to avoid blocking.
    Returns a dict with keys: facts_raw, confidence_score.
    """
    from src.agents.triage import TriageAgent
    from src.agents.extractor import ExtractionRouter
    from src.agents.chunker import ChunkingEngine
    from src.agents.fact_table import FactTableExtractor

    path = Path(pdf_path)
    db_path = Path(tmp_dir) / f"{doc_id}_facts.db"

    # Step 1: Profile document
    profile = TriageAgent().profile(path)

    # Step 2: Extract document
    router = ExtractionRouter()
    extracted = router.extract(path, profile)
    confidence = extracted.confidence_score or 0.5

    # Step 3: Chunk into LDUs
    chunks = ChunkingEngine().chunk(extracted)

    # Step 4: Extract and store facts
    extractor = FactTableExtractor(db_path=db_path)
    extractor.extract_and_store(doc_id, chunks)

    # Step 5: Query facts for each FinancialFacts field
    facts_raw: dict[str, Any] = {}
    for field_name, keywords in FIELD_KEYWORDS.items():
        best_value = None
        best_unit = ""
        for kw in keywords:
            rows = extractor.query_facts(doc_id=doc_id, keyword=kw, limit=5)
            if rows:
                row = rows[0]
                best_value = row["fact_value"]
                best_unit = row.get("unit", "")
                break
        facts_raw[field_name] = (best_value, best_unit)

    extractor.close()
    return {"facts_raw": facts_raw, "confidence_score": confidence}


class PaperMindAdapter:
    """
    Async wrapper for the synchronous paperMind-ai extraction pipeline.
    Each call runs the pipeline in a thread pool to avoid blocking the event loop.
    """

    async def extract(
        self, pdf_path: str, doc_id: str
    ) -> tuple["FinancialFacts", float]:
        """
        Extract financial facts from a PDF using the paperMind-ai pipeline.

        Returns:
            (FinancialFacts, overall_confidence) where any field not found is None
            and field_confidence[field] == 0.0 for missing fields.
        """
        from ledger.schema.events import FinancialFacts

        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                result = await asyncio.to_thread(
                    _run_pipeline_sync, pdf_path, doc_id, tmp_dir
                )
                facts_raw = result["facts_raw"]
                pipeline_confidence = result["confidence_score"]
            except Exception as exc:
                # Pipeline failure: return empty facts with zero confidence
                empty = FinancialFacts(
                    field_confidence={f: 0.0 for f in FIELD_KEYWORDS},
                    extraction_notes=[f"Pipeline error: {exc!s:.200}"],
                )
                return empty, 0.0

        # Build FinancialFacts from raw values
        field_values: dict[str, Any] = {}
        field_confidence: dict[str, float] = {}
        extraction_notes: list[str] = []

        for field_name in FIELD_KEYWORDS:
            raw_value, raw_unit = facts_raw.get(field_name, (None, ""))
            if raw_value is not None:
                try:
                    scaled = _scale_value(float(raw_value), raw_unit)
                    field_values[field_name] = Decimal(str(scaled))
                    field_confidence[field_name] = pipeline_confidence
                except (ValueError, TypeError):
                    field_values[field_name] = None
                    field_confidence[field_name] = 0.0
                    extraction_notes.append(f"Parse error for {field_name}: {raw_value}")
            else:
                field_values[field_name] = None
                field_confidence[field_name] = 0.0
                extraction_notes.append(field_name)

        facts = FinancialFacts(
            **field_values,
            field_confidence=field_confidence,
            extraction_notes=extraction_notes,
        )
        return facts, pipeline_confidence
