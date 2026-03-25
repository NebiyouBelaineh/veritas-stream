"""
scripts/rebuild_projections.py
Truncates all projection tables, resets checkpoints, and replays
all events from scratch. Run with the FastAPI server stopped.
"""
import asyncio, os, sys, logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
logging.basicConfig(level=logging.WARNING)

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")


async def main():
    import asyncpg
    from ledger.event_store import EventStore
    from ledger.projections.application_summary import ApplicationSummaryProjection
    from ledger.projections.compliance_audit import ComplianceAuditViewProjection
    from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
    from ledger.projections.daemon import ProjectionDaemon

    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    store = EventStore(os.environ["DATABASE_URL"])
    await store.connect()

    print("Truncating projection tables and resetting checkpoints...")
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE application_summary")
        await conn.execute("TRUNCATE compliance_audit_view")
        await conn.execute("TRUNCATE compliance_audit_snapshots")
        await conn.execute("TRUNCATE agent_performance_ledger")
        await conn.execute("DELETE FROM projection_checkpoints WHERE projection_name LIKE 'proj_%'")

    daemon = ProjectionDaemon(
        store=store,
        projections=[
            ApplicationSummaryProjection(),
            ComplianceAuditViewProjection(),
            AgentPerformanceLedgerProjection(),
        ],
        pool=pool,
    )

    print("Replaying all events...")
    n = await daemon.run_once()
    print(f"Dispatched {n} events")

    async with pool.acquire() as conn:
        for t in ["application_summary", "compliance_audit_view", "compliance_audit_snapshots", "agent_performance_ledger"]:
            c = await conn.fetchval(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t}: {c} rows")

    await store.close()
    await pool.close()
    print("Done.")


asyncio.run(main())
