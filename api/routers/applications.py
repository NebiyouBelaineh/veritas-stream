"""
api/routers/applications.py — Application CRUD and query endpoints.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import (
    get_store, get_registry, get_daemon,
    get_summary_projection, get_compliance_projection, get_pool,
)
from api.models.requests import SubmitApplicationRequest
from api.models.responses import (
    ApplicationSummaryResponse, EventResponse,
    ComplianceSnapshotResponse,
)

router = APIRouter(tags=["applications"])


def _row_to_response(row) -> ApplicationSummaryResponse:
    """Convert a projection row (ApplicationSummaryRow dataclass) to the response model."""
    def _get(obj, key, default=None):
        if hasattr(obj, key):
            return getattr(obj, key, default)
        if isinstance(obj, dict):
            return obj.get(key, default)
        return default

    # last_event_at comes back as a str from the projection; parse it if present
    last_event_at_raw = _get(row, "last_event_at")
    last_event_at = None
    if last_event_at_raw:
        try:
            last_event_at = datetime.fromisoformat(str(last_event_at_raw))
        except (ValueError, TypeError):
            last_event_at = None

    final_decision_at_raw = _get(row, "final_decision_at")
    final_decision_at = None
    if final_decision_at_raw:
        try:
            final_decision_at = datetime.fromisoformat(str(final_decision_at_raw))
        except (ValueError, TypeError):
            final_decision_at = None

    return ApplicationSummaryResponse(
        application_id=_get(row, "application_id", ""),
        state=_get(row, "state", "UNKNOWN"),
        applicant_id=_get(row, "applicant_id", "") or "",
        requested_amount_usd=str(_get(row, "requested_amount_usd", "")) or None,
        approved_amount_usd=str(_get(row, "approved_amount_usd", "")) if _get(row, "approved_amount_usd") else None,
        risk_tier=_get(row, "risk_tier"),
        fraud_score=_get(row, "fraud_score"),
        compliance_status=_get(row, "compliance_status"),
        decision=_get(row, "decision"),
        agent_sessions_completed=list(_get(row, "agent_sessions_completed", []) or []),
        last_event_type=_get(row, "last_event_type"),
        last_event_at=last_event_at,
        human_reviewer_id=_get(row, "human_reviewer_id"),
        final_decision_at=final_decision_at,
    )


@router.get("/applications", response_model=list[ApplicationSummaryResponse])
async def list_applications(
    state: str | None = Query(None, description="Filter by state"),
    risk_tier: str | None = Query(None, description="Filter by risk tier"),
    summary_proj=Depends(get_summary_projection),
    daemon=Depends(get_daemon),
):
    await daemon.run_once()
    rows = summary_proj.all()
    result = [_row_to_response(r) for r in rows]
    if state:
        result = [r for r in result if r.state == state.upper()]
    if risk_tier:
        result = [r for r in result if r.risk_tier == risk_tier.upper()]
    return result


@router.get("/applications/{application_id}", response_model=ApplicationSummaryResponse)
async def get_application(
    application_id: str,
    summary_proj=Depends(get_summary_projection),
    daemon=Depends(get_daemon),
):
    await daemon.run_once()
    row = summary_proj.get(application_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Application {application_id} not found")
    return _row_to_response(row)


@router.get("/applications/{application_id}/events", response_model=list[EventResponse])
async def get_application_events(
    application_id: str,
    store=Depends(get_store),
):
    events = await store.load_stream(f"loan-{application_id}")
    if not events:
        raise HTTPException(status_code=404, detail=f"Application {application_id} not found")

    result = []
    for e in events:
        recorded_at = e.get("recorded_at")
        if isinstance(recorded_at, str):
            try:
                recorded_at = datetime.fromisoformat(recorded_at)
            except (ValueError, TypeError):
                recorded_at = datetime.now()
        elif not isinstance(recorded_at, datetime):
            recorded_at = datetime.now()

        result.append(EventResponse(
            event_id=str(e.get("event_id", "")),
            stream_id=e.get("stream_id", ""),
            stream_position=e.get("stream_position", 0),
            global_position=e.get("global_position", 0),
            event_type=e.get("event_type", ""),
            event_version=e.get("event_version", 1),
            payload=e.get("payload", {}),
            metadata=e.get("metadata", {}),
            recorded_at=recorded_at,
        ))
    return result


@router.get("/applications/{application_id}/compliance", response_model=ComplianceSnapshotResponse)
async def get_application_compliance(
    application_id: str,
    as_of: str | None = Query(None, description="ISO-8601 timestamp for time-travel query"),
    compliance_proj=Depends(get_compliance_projection),
    daemon=Depends(get_daemon),
):
    await daemon.run_once()

    try:
        if as_of:
            ts = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            snapshot = compliance_proj.get_compliance_at(application_id, ts)
        else:
            snapshot = compliance_proj.get_current(application_id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"No compliance data for {application_id}")

    def _get(obj, key, default=None):
        if hasattr(obj, key):
            return getattr(obj, key, default)
        if isinstance(obj, dict):
            return obj.get(key, default)
        return default

    return ComplianceSnapshotResponse(
        application_id=_get(snapshot, "application_id", application_id),
        rules_passed=list(_get(snapshot, "rules_passed", set()) or []),
        rules_failed=list(_get(snapshot, "rules_failed", set()) or []),
        hard_blocked=bool(_get(snapshot, "hard_blocked", False)),
        verdict=_get(snapshot, "verdict"),
        as_of=datetime.fromisoformat(as_of.replace("Z", "+00:00")) if as_of else None,
    )


@router.post("/applications", response_model=ApplicationSummaryResponse, status_code=201)
async def submit_application(
    req: SubmitApplicationRequest,
    store=Depends(get_store),
    pool=Depends(get_pool),
    daemon=Depends(get_daemon),
    summary_proj=Depends(get_summary_projection),
):
    # Generate IDs
    suffix = uuid.uuid4().hex[:8].upper()
    application_id = f"APP-{suffix}"
    now = datetime.now()

    if req.existing_company_id:
        # Use an existing registry entry — just verify it exists
        company_id = req.existing_company_id
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT company_id FROM applicant_registry.companies WHERE company_id = $1",
                company_id,
            )
            if not row:
                raise HTTPException(status_code=404, detail=f"Company {company_id} not found in registry")
    else:
        # Create a new registry entry from the form data
        company_id = application_id
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO applicant_registry.companies (
                    company_id, name, industry, naics, jurisdiction, legal_type,
                    founded_year, employee_count, risk_segment, trajectory,
                    submission_channel, ip_region
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (company_id) DO NOTHING
            """,
                company_id, req.company_name, req.industry, "",
                req.jurisdiction, req.legal_type,
                req.founded_year, req.employee_count,
                req.risk_segment, req.trajectory,
                "online", "US",
            )

            await conn.execute("""
                INSERT INTO applicant_registry.financial_history (
                    company_id, fiscal_year, total_revenue, gross_profit,
                    operating_income, ebitda, net_income,
                    total_assets, total_liabilities, total_equity,
                    long_term_debt, cash_and_equivalents,
                    current_assets, current_liabilities,
                    accounts_receivable, inventory
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                ON CONFLICT DO NOTHING
            """,
                company_id, now.year,
                req.annual_revenue_usd, req.annual_revenue_usd * 0.4,
                req.ebitda_usd, req.ebitda_usd, req.ebitda_usd * 0.7,
                req.total_assets_usd, req.total_liabilities_usd, req.total_equity_usd,
                req.total_liabilities_usd * 0.6, req.total_assets_usd * 0.1,
                req.total_assets_usd * 0.3, req.total_liabilities_usd * 0.4,
                req.total_assets_usd * 0.15, req.total_assets_usd * 0.05,
            )

            for flag in req.compliance_flags:
                await conn.execute("""
                    INSERT INTO applicant_registry.compliance_flags (
                        company_id, flag_type, severity, is_active, note
                    ) VALUES ($1,$2,$3,$4,$5)
                    ON CONFLICT DO NOTHING
                """,
                    company_id,
                    flag.get("flag_type", "UNKNOWN"),
                    flag.get("severity", "LOW"),
                    flag.get("is_active", True),
                    flag.get("note", ""),
                )

    # 2. Append ApplicationSubmitted event
    event = {
        "event_type": "ApplicationSubmitted",
        "event_version": 1,
        "payload": {
            "application_id": application_id,
            "applicant_id": company_id,
            "requested_amount_usd": str(req.requested_amount_usd),
            "loan_purpose": req.loan_purpose,
            "loan_term_months": req.loan_term_months,
            "submission_channel": "online",
            "contact_email": req.contact_email,
            "contact_name": req.contact_name or req.company_name,
            "application_reference": f"REF-{application_id}",
            "submitted_at": now.isoformat(),
        },
    }
    await store.append(f"loan-{application_id}", [event], expected_version=-1)

    # 3. Update projections
    await daemon.run_once()

    # 4. Return summary
    row = summary_proj.get(application_id)
    if not row:
        # Return a minimal response if projection not yet updated
        return ApplicationSummaryResponse(
            application_id=application_id,
            state="SUBMITTED",
            applicant_id=company_id,
            requested_amount_usd=str(req.requested_amount_usd),
        )
    return _row_to_response(row)
