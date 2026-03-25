"""
api/services/extraction_service.py — Parse application_proposal.pdf with pdfplumber.

Extracts structured loan application fields using regex on the known PDF layout
produced by datagen/pdf_generator.py.
"""
from __future__ import annotations

import re
from pathlib import Path


def _parse_usd(text: str) -> float | None:
    """Extract a numeric value from a USD string like '$2,152,226' or '6,376,032'."""
    cleaned = re.sub(r"[$,]", "", text.strip())
    try:
        return float(cleaned)
    except ValueError:
        return None


def _find(pattern: str, text: str, group: int = 1) -> str | None:
    m = re.search(pattern, text)
    return m.group(group).strip() if m else None


def extract_proposal(company_id: str, documents_root: Path | None = None) -> dict:
    """
    Parse application_proposal.pdf for the given company_id.

    Returns a dict with pre-filled application fields, or raises FileNotFoundError
    if the document does not exist.
    """
    root = documents_root or Path(__file__).parent.parent.parent / "documents"
    pdf_path = root / company_id / "application_proposal.pdf"

    if not pdf_path.exists():
        raise FileNotFoundError(f"No application_proposal.pdf found for {company_id}")

    import pdfplumber

    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += (page.extract_text() or "") + "\n"

    result: dict = {"company_id": company_id, "_source": "application_proposal.pdf"}

    # Company identity
    result["company_name"] = _find(r"Legal Entity Name:\s*(.+)", text)
    result["legal_type"] = _find(r"Business Type:\s*(.+)", text)
    result["industry"] = _find(r"Industry:\s*(.+)", text)
    result["jurisdiction"] = _find(r"Jurisdiction:\s*(.+)", text)

    founded = _find(r"Founded:\s*(\d{4})", text)
    result["founded_year"] = int(founded) if founded else None

    employees = _find(r"Employees:\s*(\d+)", text)
    result["employee_count"] = int(employees) if employees else None

    # Loan request
    amount_str = _find(r"Requested Amount:\s*\$?([\d,]+)", text)
    result["requested_amount_usd"] = _parse_usd(amount_str) if amount_str else None

    result["loan_purpose"] = _find(r"Purpose:\s*(.+)", text)

    # Financial highlights — FY2024 column (first numeric after label)
    revenue_str = _find(r"Revenue\s+\$([\d,]+)", text)
    result["annual_revenue_usd"] = _parse_usd(revenue_str) if revenue_str else None

    ebitda_str = _find(r"EBITDA\s+\$([\d,]+)", text)
    result["ebitda_usd"] = _parse_usd(ebitda_str) if ebitda_str else None

    assets_str = _find(r"Total Assets\s+\$([\d,]+)", text)
    result["total_assets_usd"] = _parse_usd(assets_str) if assets_str else None

    return result
