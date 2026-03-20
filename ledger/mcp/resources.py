"""
ledger/mcp/resources.py
========================
MCP resources — 6 read-only projection-backed resources.

Resource URIs:
  ledger://applications/{id}                  — ApplicationSummaryRow
  ledger://applications/{id}/compliance       — current ComplianceSnapshot
  ledger://applications/{id}/compliance/{as_of} — temporal ComplianceSnapshot
  ledger://applications/{id}/audit-trail      — raw event list (justified replay)
  ledger://agents/{agent_id}/performance      — AgentPerformanceRow(s)
  ledger://agents/{agent_id}/sessions/{session_id} — AgentContext (crash recovery)
  ledger://ledger/health                      — projection lags

Resources read ONLY from projections (except audit-trail and agent sessions,
which are justified stream-replay exceptions per the architecture spec).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from fastmcp import FastMCP

from ledger.integrity.gas_town import reconstruct_agent_context


def _json(obj) -> str:
    """Serialise `obj` to JSON, converting sets/datetimes to JSON-safe types."""
    def default(o):
        if isinstance(o, set):
            return sorted(o)
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(f"Not serialisable: {type(o)}")
    return json.dumps(obj, default=default)


def _row_to_dict(row) -> dict:
    """Convert a dataclass row to a plain dict (no deep copy needed — read only)."""
    import dataclasses
    if dataclasses.is_dataclass(row):
        d = {}
        for f in dataclasses.fields(row):
            val = getattr(row, f.name)
            if isinstance(val, set):
                val = sorted(val)
            d[f.name] = val
        return d
    return dict(row)


def register_resources(mcp: FastMCP, store, daemon, projections: dict) -> None:
    """
    Register all 6 projection resources on `mcp`, closing over dependencies.

    projections keys expected:
      "summary"          → ApplicationSummaryProjection
      "compliance"       → ComplianceAuditViewProjection
      "agent_performance" → AgentPerformanceLedgerProjection
    """
    summary_proj = projections.get("summary")
    compliance_proj = projections.get("compliance")
    perf_proj = projections.get("agent_performance")

    # ── 1. Application summary ────────────────────────────────────────────────

    @mcp.resource(
        "ledger://applications/{id}",
        description=(
            "Denormalised summary of a loan application. "
            "Reads from in-memory projection — no stream replay. "
            "P99 SLO: < 50 ms."
        ),
    )
    async def get_application_summary(id: str) -> str:
        if summary_proj is None:
            return _json({"error": "summary projection not available"})
        row = summary_proj.get(id)
        if row is None:
            return _json({"error": f"Application '{id}' not found"})
        return _json(_row_to_dict(row))

    # ── 2. Current compliance snapshot ────────────────────────────────────────

    @mcp.resource(
        "ledger://applications/{id}/compliance",
        description=(
            "Current compliance state for a loan application. "
            "Use ledger://applications/{id}/compliance/{as_of} for temporal queries."
        ),
    )
    async def get_compliance_current(id: str) -> str:
        if compliance_proj is None:
            return _json({"error": "compliance projection not available"})
        snapshot = compliance_proj.get_current(id)
        return _json({
            "application_id": snapshot.application_id,
            "rules_passed": sorted(snapshot.rules_passed),
            "rules_failed": sorted(snapshot.rules_failed),
            "hard_blocked": snapshot.hard_blocked,
            "verdict": snapshot.verdict,
        })

    # ── 3. Temporal compliance snapshot ───────────────────────────────────────

    @mcp.resource(
        "ledger://applications/{id}/compliance/{as_of}",
        description=(
            "Compliance state for application `id` as it existed at `as_of` "
            "(ISO-8601 timestamp). Proves regulatory time-travel requirement."
        ),
    )
    async def get_compliance_at(id: str, as_of: str) -> str:
        if compliance_proj is None:
            return _json({"error": "compliance projection not available"})
        try:
            dt = datetime.fromisoformat(as_of)
        except ValueError:
            return _json({"error": f"Invalid as_of timestamp: {as_of!r}"})
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        snapshot = compliance_proj.get_compliance_at(id, dt)
        return _json({
            "application_id": snapshot.application_id,
            "as_of": as_of,
            "rules_passed": sorted(snapshot.rules_passed),
            "rules_failed": sorted(snapshot.rules_failed),
            "hard_blocked": snapshot.hard_blocked,
            "verdict": snapshot.verdict,
        })

    # ── 4. Audit trail (justified stream replay) ──────────────────────────────

    @mcp.resource(
        "ledger://applications/{id}/audit-trail",
        description=(
            "Full event history for a loan application stream. "
            "This resource performs a stream replay — use sparingly. "
            "Returns events in stream_position order."
        ),
    )
    async def get_audit_trail(id: str) -> str:
        events = await store.load_stream(f"loan-{id}")
        serialisable = []
        for e in events:
            serialisable.append({
                "event_type": e.get("event_type"),
                "event_version": e.get("event_version"),
                "stream_position": e.get("stream_position"),
                "global_position": e.get("global_position"),
                "recorded_at": str(e.get("recorded_at", "")),
                "payload": e.get("payload", {}),
            })
        return _json(serialisable)

    # ── 5. Agent performance ──────────────────────────────────────────────────

    @mcp.resource(
        "ledger://agents/{agent_id}/performance",
        description=(
            "Aggregate performance statistics for agent `agent_id`, partitioned "
            "by model_version. Returns all (agent_id, model_version) rows."
        ),
    )
    async def get_agent_performance(agent_id: str) -> str:
        if perf_proj is None:
            return _json({"error": "agent_performance projection not available"})
        rows = [r for r in perf_proj.all() if r.agent_id == agent_id]
        result = []
        for r in rows:
            result.append({
                "agent_id": r.agent_id,
                "model_version": r.model_version,
                "analyses_completed": r.analyses_completed,
                "decisions_generated": r.decisions_generated,
                "avg_confidence_score": r.avg_confidence_score,
                "avg_duration_ms": r.avg_duration_ms,
                "approve_rate": r.approve_rate,
                "decline_rate": r.decline_rate,
                "refer_rate": r.refer_rate,
                "human_override_rate": r.human_override_rate,
                "first_seen_at": r.first_seen_at,
                "last_seen_at": r.last_seen_at,
            })
        return _json(result)

    # ── 6. Agent session context (justified stream replay) ────────────────────

    @mcp.resource(
        "ledger://agents/{agent_id}/sessions/{session_id}",
        description=(
            "Reconstruct an agent session context from its event stream. "
            "Used by the Gas Town pattern for crash recovery. "
            "This resource performs a stream replay — required for recovery."
        ),
    )
    async def get_agent_session(agent_id: str, session_id: str) -> str:
        ctx = await reconstruct_agent_context(
            store,
            agent_id=agent_id,
            session_id=session_id,
        )
        return _json({
            "session_id": ctx.session_id,
            "agent_type": ctx.agent_type,
            "agent_id": ctx.agent_id,
            "application_id": ctx.application_id,
            "model_version": ctx.model_version,
            "context_text": ctx.context_text,
            "last_position": ctx.last_position,
            "nodes_completed": ctx.nodes_completed,
            "pending_work": ctx.pending_work,
            "health_status": ctx.health_status,
            "session_started": ctx.session_started,
            "session_completed": ctx.session_completed,
            "session_failed": ctx.session_failed,
            "tools_called": ctx.tools_called,
        })

    # ── 7. Ledger health ──────────────────────────────────────────────────────

    @mcp.resource(
        "ledger://ledger/health",
        description=(
            "Health report for all projection daemons. "
            "Reports lag (ms since last event processed) for every projection. "
            "Lag = 0.0 means no events have been processed yet."
        ),
    )
    async def get_ledger_health() -> str:
        lags = {}
        for name in daemon.projections:
            lags[name] = await daemon.get_lag(name)
        return _json({
            "status": "ok",
            "projection_lags_ms": lags,
            "projection_count": len(lags),
        })
