"""
api/routers/registry.py — Read-only access to the Applicant Registry.

Used by the UI to look up existing company profiles so users can submit
a loan application for a company that already has documents on file.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_pool
from api.services.extraction_service import extract_proposal

router = APIRouter(tags=["registry"])


@router.get("/registry/companies/{company_id}")
async def get_company_profile(company_id: str, pool=Depends(get_pool)):
    """Return company info + latest financial history row for an existing registry entry."""
    async with pool.acquire() as conn:
        company = await conn.fetchrow(
            "SELECT * FROM applicant_registry.companies WHERE company_id = $1",
            company_id,
        )
        if not company:
            raise HTTPException(status_code=404, detail=f"Company {company_id} not found")

        financials = await conn.fetchrow(
            """SELECT * FROM applicant_registry.financial_history
               WHERE company_id = $1
               ORDER BY fiscal_year DESC LIMIT 1""",
            company_id,
        )

    result = dict(company)
    result["financials"] = dict(financials) if financials else {}
    return result


@router.get("/registry/extract/{company_id}")
async def extract_company_proposal(company_id: str):
    """Parse application_proposal.pdf for company_id and return pre-filled form fields."""
    try:
        return extract_proposal(company_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/registry/companies")
async def list_companies(pool=Depends(get_pool)):
    """Return all companies in the registry (id + name only for dropdowns)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT company_id, name, industry, risk_segment FROM applicant_registry.companies ORDER BY company_id"
        )
    return [dict(r) for r in rows]
