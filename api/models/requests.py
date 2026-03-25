from __future__ import annotations
from pydantic import BaseModel, Field


class SubmitApplicationRequest(BaseModel):
    # When set, skip registry insert and use this existing company_id
    existing_company_id: str | None = None

    # Registry entry (step 1) — required only when existing_company_id is None
    company_name: str = ""
    industry: str = "Other"
    jurisdiction: str = "TX"
    legal_type: str = "LLC"
    founded_year: int = 2015
    employee_count: int = 50
    risk_segment: str = "SME"
    trajectory: str = "stable"

    # Financial data — required only when existing_company_id is None
    annual_revenue_usd: float | None = None
    ebitda_usd: float | None = None
    total_assets_usd: float | None = None
    total_liabilities_usd: float | None = None
    total_equity_usd: float | None = None

    # Compliance flags (optional)
    compliance_flags: list[dict] = Field(default_factory=list)

    # Loan request (step 2)
    requested_amount_usd: float
    loan_purpose: str = "working_capital"
    loan_term_months: int = 60
    contact_email: str = ""
    contact_name: str = ""


class PipelineRunRequest(BaseModel):
    phase: str = "all"  # all | document | credit | fraud | compliance | decision


class WhatIfRequest(BaseModel):
    application_id: str
    phase: str = "all"

    # Financial overrides
    annual_revenue_usd: float | None = None
    ebitda_usd: float | None = None
    total_assets_usd: float | None = None
    total_debt_usd: float | None = None

    # Loan parameters
    requested_amount_usd: float | None = None

    # Risk signals
    prior_default: bool | None = None
    fraud_score_override: float | None = None
    confidence_override: float | None = None

    # Compliance
    compliance_verdict_override: str | None = None
    inject_compliance_flags: list[dict] | None = None

    # Analysis overrides
    risk_tier_override: str | None = None
