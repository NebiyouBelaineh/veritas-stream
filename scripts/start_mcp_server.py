"""
scripts/start_mcp_server.py
============================
Start the VeritasStream MCP server backed by PostgreSQL.

Wires the PostgreSQL EventStore, three projections, and a ProjectionDaemon,
then starts the FastMCP server. The daemon runs as a background task and
keeps projections up to date while the server handles MCP requests.

Usage:
    uv run python scripts/start_mcp_server.py
    uv run python scripts/start_mcp_server.py --transport sse

Transports:
    stdio  (default) — communicate over stdin/stdout; used by MCP clients
    sse              — HTTP server-sent events on http://localhost:8001

Press Ctrl-C to stop.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import asyncpg

from ledger.event_store import EventStore
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.mcp.server import create_mcp_server


async def start(transport: str) -> None:
    db_url = os.environ["DATABASE_URL"]

    pool  = await asyncpg.create_pool(db_url, min_size=2, max_size=10)
    store = EventStore(db_url)
    await store.connect()

    summary_proj    = ApplicationSummaryProjection()
    compliance_proj = ComplianceAuditViewProjection()
    performance_proj = AgentPerformanceLedgerProjection()

    daemon = ProjectionDaemon(
        store=store,
        projections=[summary_proj, compliance_proj, performance_proj],
        pool=pool,
    )

    daemon_task = asyncio.create_task(daemon.run())

    mcp = create_mcp_server(
        store=store,
        daemon=daemon,
        projections={
            "summary":           summary_proj,
            "compliance":        compliance_proj,
            "agent_performance": performance_proj,
        },
    )

    print(f"VeritasStream MCP server starting (transport={transport})")
    print("Press Ctrl-C to stop.")

    try:
        await mcp.run_async(transport=transport)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        daemon_task.cancel()
        try:
            await daemon_task
        except asyncio.CancelledError:
            pass
        daemon.stop()
        await store.close()
        await pool.close()
        print("Server stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the VeritasStream MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="MCP transport (default: stdio)",
    )
    args = parser.parse_args()
    asyncio.run(start(args.transport))


if __name__ == "__main__":
    main()
