"""
api/dependencies.py — Shared FastAPI dependencies.

Provides get_store(), get_registry(), get_daemon(), get_projections()
as FastAPI dependency functions backed by module-level singletons
initialised in the lifespan context.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Make ledger/ importable
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncpg
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# These will be set during lifespan startup
_store = None
_pool = None
_registry = None
_daemon = None
_summary_projection = None
_compliance_projection = None
_performance_projection = None


async def startup() -> None:
    global _store, _pool, _registry, _daemon
    global _summary_projection, _compliance_projection, _performance_projection

    from ledger.event_store import EventStore
    from ledger.registry.client import ApplicantRegistryClient
    from ledger.projections.daemon import ProjectionDaemon
    from ledger.projections.application_summary import ApplicationSummaryProjection
    from ledger.projections.compliance_audit import ComplianceAuditViewProjection
    from ledger.projections.agent_performance import AgentPerformanceLedgerProjection

    db_url = os.environ["DATABASE_URL"]

    _pool = await asyncpg.create_pool(db_url, min_size=2, max_size=10)
    _store = EventStore(db_url)
    await _store.connect()

    _registry = ApplicantRegistryClient(_pool)

    _summary_projection = ApplicationSummaryProjection()
    _compliance_projection = ComplianceAuditViewProjection()
    _performance_projection = AgentPerformanceLedgerProjection()

    _daemon = ProjectionDaemon(
        store=_store,
        projections=[_summary_projection, _compliance_projection, _performance_projection],
        pool=_pool,
    )


async def shutdown() -> None:
    global _store, _pool, _daemon
    if _daemon:
        _daemon.stop()
    if _store:
        await _store.close()
    if _pool:
        await _pool.close()


def get_store():
    return _store


def get_registry():
    return _registry


def get_daemon():
    return _daemon


def get_summary_projection():
    return _summary_projection


def get_compliance_projection():
    return _compliance_projection


def get_performance_projection():
    return _performance_projection


def get_pool():
    return _pool
