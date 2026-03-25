"""
api/services/whatif_service.py — Shadow pipeline runner for what-if analysis.

Runs the full agent pipeline against an InMemoryEventStore with overridden
registry data. No events are persisted to the database.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from run_pipeline import _seed_streams, DOCUMENTS_DIR, DATA_DIR
from ledger.event_store import InMemoryEventStore
from api.models.requests import WhatIfRequest
from api.services.pipeline_service import run_pipeline, PipelineResult, PHASES_FOR


class _OverrideRegistryAdapter:
    """
    Wraps an existing registry and patches fields specified in WhatIfRequest.
    """

    def __init__(self, inner, req: WhatIfRequest) -> None:
        self._inner = inner
        self._req = req

    async def get_company(self, company_id: str):
        return await self._inner.get_company(company_id)

    async def get_financial_history(self, company_id: str, years=None):
        history = await self._inner.get_financial_history(company_id, years)
        if not history:
            return history
        # Patch financial fields on the most recent year
        patched = list(history)
        most_recent = patched[-1]

        # Convert to dict for mutation
        if hasattr(most_recent, "__dict__"):
            d = most_recent.__dict__.copy()
        else:
            d = dict(most_recent)

        req = self._req
        if req.annual_revenue_usd is not None:
            d["total_revenue"] = req.annual_revenue_usd
        if req.ebitda_usd is not None:
            d["ebitda"] = req.ebitda_usd
        if req.total_assets_usd is not None:
            d["total_assets"] = req.total_assets_usd
        if req.total_debt_usd is not None:
            d["long_term_debt"] = req.total_debt_usd

        # Reconstruct the same type
        if hasattr(most_recent, "__class__") and hasattr(most_recent.__class__, "__dataclass_fields__"):
            patched[-1] = most_recent.__class__(**d)
        else:
            patched[-1] = d

        return patched

    async def get_compliance_flags(self, company_id: str, active_only: bool = False):
        flags = await self._inner.get_compliance_flags(company_id, active_only)

        req = self._req
        if req.inject_compliance_flags:
            flags = list(flags) + req.inject_compliance_flags

        if active_only:
            flags = [f for f in flags if (f.get("is_active", True) if isinstance(f, dict) else getattr(f, "is_active", True))]

        return flags

    async def get_loan_relationships(self, company_id: str):
        rels = await self._inner.get_loan_relationships(company_id)

        req = self._req
        if req.prior_default is True:
            # Inject a prior default record
            rels = list(rels) + [{"has_default": True, "default_count": 1}]

        return rels


async def run_whatif(
    req: WhatIfRequest,
    registry,
    broker=None,
    run_id: str | None = None,
) -> PipelineResult:
    """
    Run a shadow pipeline with overrides. Returns PipelineResult without
    persisting any events to the database.
    """
    from api.sse.progress_broker import InstrumentedEventStore

    shadow_store = InMemoryEventStore()

    if broker and run_id:
        effective_store = InstrumentedEventStore(shadow_store, broker, run_id)
    else:
        effective_store = shadow_store

    override_registry = _OverrideRegistryAdapter(registry, req)

    # Seed streams from original documents
    profile = await registry.get_company(req.application_id)
    await _seed_streams(effective_store, req.application_id, req.application_id, profile)

    result = await run_pipeline(
        app_id=req.application_id,
        phase=req.phase,
        store=effective_store,
        registry=override_registry,
        seed_streams=False,  # Already seeded above
    )

    return result
