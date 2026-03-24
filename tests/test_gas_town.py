"""
tests/test_gas_town.py
======================
Gas Town crash-recovery tests.

Proves:
  - After 5 agent session events, reconstruct_agent_context() recovers
    session_id, application_id, model_version, nodes_completed, and
    health_status from the event stream alone (no in-memory agent object).
  - A partial decision node with no AgentSessionCompleted is flagged as
    NEEDS_RECONCILIATION in pending_work.

Run: pytest tests/test_gas_town.py -v
"""
import pytest


async def test_gas_town_agent_reconstructs_after_crash():
    from tests.test_integrity import test_gas_town_agent_reconstructs_after_crash as _t
    await _t()


async def test_gas_town_partial_decision_flagged():
    from tests.test_integrity import test_gas_town_partial_decision_flagged as _t
    await _t()
