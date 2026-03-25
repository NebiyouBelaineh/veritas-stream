"""
api/routers/agents.py — Agent performance and session endpoints.
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_store, get_daemon, get_performance_projection
from api.models.responses import AgentPerformanceResponse, AgentSessionResponse

router = APIRouter(tags=["agents"])


def _perf_row_to_response(row) -> AgentPerformanceResponse:
    def _get(obj, key, default=None):
        if hasattr(obj, key):
            return getattr(obj, key, default)
        if isinstance(obj, dict):
            return obj.get(key, default)
        return default

    # first_seen_at / last_seen_at are stored as strings in the projection
    def _parse_dt(val) -> datetime | None:
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        try:
            return datetime.fromisoformat(str(val))
        except (ValueError, TypeError):
            return None

    return AgentPerformanceResponse(
        agent_id=_get(row, "agent_id", ""),
        model_version=_get(row, "model_version", ""),
        analyses_completed=_get(row, "analyses_completed", 0) or 0,
        decisions_generated=_get(row, "decisions_generated", 0) or 0,
        avg_confidence_score=_get(row, "avg_confidence_score"),
        avg_duration_ms=_get(row, "avg_duration_ms"),
        approve_rate=_get(row, "approve_rate"),
        decline_rate=_get(row, "decline_rate"),
        refer_rate=_get(row, "refer_rate"),
        human_override_rate=_get(row, "human_override_rate"),
        first_seen_at=_parse_dt(_get(row, "first_seen_at")),
        last_seen_at=_parse_dt(_get(row, "last_seen_at")),
    )


@router.get("/agents/performance", response_model=list[AgentPerformanceResponse])
async def get_agent_performance(
    perf_proj=Depends(get_performance_projection),
    daemon=Depends(get_daemon),
):
    await daemon.run_once()
    rows = perf_proj.all()
    return [_perf_row_to_response(r) for r in rows]


@router.get("/agents/{agent_id}/sessions/{session_id}", response_model=AgentSessionResponse)
async def get_agent_session(
    agent_id: str,
    session_id: str,
    store=Depends(get_store),
):
    # Agent stream ID: agent-{agent_type}-{session_id}
    # Try the combined format first
    stream_id = f"agent-{agent_id}-{session_id}"
    events = await store.load_stream(stream_id)

    if not events:
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id} for agent {agent_id} not found",
        )

    # Reconstruct session from events
    session_started = None
    session_completed = None
    session_failed = None
    nodes_completed = []
    tools_called = []
    application_id = ""
    agent_type = agent_id
    model_version = ""
    context_text = None
    health_status = "UNKNOWN"

    def _parse_dt(val) -> datetime | None:
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        try:
            return datetime.fromisoformat(str(val))
        except (ValueError, TypeError):
            return None

    for e in events:
        etype = e.get("event_type", "")
        payload = e.get("payload", {})
        recorded_at = _parse_dt(e.get("recorded_at"))

        if etype == "AgentSessionStarted":
            session_started = recorded_at
            application_id = payload.get("application_id", "")
            agent_type = payload.get("agent_type", agent_id)
            model_version = payload.get("model_version", "")
            health_status = "RUNNING"
        elif etype == "AgentNodeExecuted":
            nodes_completed.append(payload.get("node_name", ""))
        elif etype == "AgentToolCalled":
            tools_called.append(payload)
        elif etype == "AgentSessionCompleted":
            session_completed = recorded_at
            health_status = "COMPLETED"
        elif etype == "AgentSessionFailed":
            session_failed = recorded_at
            health_status = "FAILED"

    return AgentSessionResponse(
        session_id=session_id,
        agent_type=agent_type,
        agent_id=agent_id,
        application_id=application_id,
        model_version=model_version,
        nodes_completed=nodes_completed,
        health_status=health_status,
        session_started=session_started,
        session_completed=session_completed,
        session_failed=session_failed,
        tools_called=tools_called,
        context_text=context_text,
    )
