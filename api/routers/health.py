"""
api/routers/health.py — System health endpoint.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends

from api.dependencies import get_store, get_daemon
from api.models.responses import HealthResponse

router = APIRouter(tags=["system"])


@router.get("/health", response_model=HealthResponse)
async def get_health(
    store=Depends(get_store),
    daemon=Depends(get_daemon),
):
    db_connected = False
    event_count = 0
    lags: dict[str, float] = {}

    try:
        # Count total events via store
        count = 0
        async for _ in store.load_all(from_position=0, batch_size=1000):
            count += 1
        event_count = count
        db_connected = True
    except Exception:
        pass

    try:
        for proj_name in ["application_summary", "compliance_audit_view", "agent_performance_ledger"]:
            try:
                lag = await daemon.get_lag(proj_name)
                lags[proj_name] = round(lag, 2)
            except Exception:
                lags[proj_name] = -1.0
    except Exception:
        pass

    return HealthResponse(
        status="ok" if db_connected else "degraded",
        projection_lags_ms=lags,
        event_count=event_count,
        db_connected=db_connected,
    )
