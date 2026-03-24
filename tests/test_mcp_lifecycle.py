"""
tests/test_mcp_lifecycle.py
============================
Full MCP lifecycle test. All operations are driven exclusively via MCP tool calls
and resource reads — no direct Python command handler invocations.

Covers the complete loan lifecycle:
  start_agent_session → submit_application → record_credit_analysis →
  record_fraud_screening → record_compliance_check → generate_decision →
  record_human_review → query compliance resource

Run: pytest tests/test_mcp_lifecycle.py -v
"""
import pytest


async def test_full_lifecycle_via_mcp_only():
    from tests.test_mcp_integration import test_full_lifecycle_via_mcp_only as _t
    await _t()
