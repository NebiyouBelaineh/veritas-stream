"""
ledger/mcp/server.py
=====================
FastMCP server factory for The Ledger.

Usage (in production):
    from ledger.mcp.server import create_mcp_server
    mcp = create_mcp_server(store, daemon, projections)
    mcp.run()

Usage (in tests — in-process via FastMCP Client):
    from fastmcp import Client
    mcp = create_mcp_server(store, daemon, projections)
    async with Client(mcp) as client:
        result = await client.call_tool("submit_application", {...})
"""
from __future__ import annotations

from fastmcp import FastMCP

from ledger.mcp.tools import register_tools
from ledger.mcp.resources import register_resources


def create_mcp_server(store, daemon, projections: dict) -> FastMCP:
    """
    Build and return the fully wired FastMCP server.

    Args:
        store:       EventStore or InMemoryEventStore instance.
        daemon:      ProjectionDaemon instance (for health reporting).
        projections: Dict mapping names to projection instances:
                       "summary"           → ApplicationSummaryProjection
                       "compliance"        → ComplianceAuditViewProjection
                       "agent_performance" → AgentPerformanceLedgerProjection

    Returns:
        A FastMCP server instance with all 8 tools and 7 resources registered.
    """
    mcp = FastMCP(
        "VeritasStream Ledger",
        instructions=(
            "The Ledger is Apex Financial Services' event-sourcing infrastructure "
            "for AI-driven loan processing. Use tools to issue commands (write) "
            "and resources to query read models (read-only)."
        ),
    )
    register_tools(mcp, store)
    register_resources(mcp, store, daemon, projections)
    return mcp
