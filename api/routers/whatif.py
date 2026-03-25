"""
api/routers/whatif.py — What-if shadow pipeline endpoint.
"""
from __future__ import annotations

import asyncio
from uuid import uuid4

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from api.dependencies import get_registry
from api.models.requests import WhatIfRequest
from api.models.responses import PipelineRunResponse
from api.sse.progress_broker import ProgressBroker
from api.services.whatif_service import run_whatif

router = APIRouter(tags=["whatif"])

_broker = ProgressBroker()
_runs: dict[str, asyncio.Task] = {}


@router.post("/whatif/run", response_model=PipelineRunResponse)
async def start_whatif_run(
    req: WhatIfRequest,
    registry=Depends(get_registry),
):
    run_id = str(uuid4())
    _broker.create_run(run_id)

    async def _run():
        try:
            result = await run_whatif(req, registry, _broker, run_id)
            _broker.push(run_id, "done", {
                "outcome": result.outcome,
                "approved_amount": result.approved_amount,
                "risk_tier": result.risk_tier,
                "fraud_score": str(result.fraud_score) if result.fraud_score is not None else None,
                "compliance_verdict": result.compliance_verdict,
                "total_elapsed_s": result.total_elapsed_s,
                "is_shadow": True,
            })
        except Exception as exc:
            _broker.push(run_id, "run_error", {"reason": str(exc), "recoverable": False})
        finally:
            _broker.close(run_id)
            _runs.pop(run_id, None)

    task = asyncio.create_task(_run())
    _runs[run_id] = task

    return PipelineRunResponse(
        run_id=run_id,
        application_id=req.application_id,
        phase=req.phase,
    )


@router.get("/whatif/progress")
async def whatif_progress(run_id: str):
    async def _generate():
        async for chunk in _broker.stream(run_id):
            yield chunk

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
