"""
api/routers/pipeline.py — Pipeline trigger and SSE progress stream.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from api.dependencies import get_store, get_registry, get_daemon, get_summary_projection
from api.models.requests import PipelineRunRequest
from api.models.responses import PipelineRunResponse
from api.sse.progress_broker import ProgressBroker, InstrumentedEventStore
from api.services.pipeline_service import run_pipeline

router = APIRouter(tags=["pipeline"])

# Module-level broker shared across requests
_broker = ProgressBroker()
# Track in-progress runs: run_id → asyncio.Task
_runs: dict[str, asyncio.Task] = {}


@router.post("/pipeline/{application_id}/run", response_model=PipelineRunResponse)
async def start_pipeline_run(
    application_id: str,
    req: PipelineRunRequest,
    store=Depends(get_store),
    registry=Depends(get_registry),
    daemon=Depends(get_daemon),
):
    run_id = str(uuid4())
    _broker.create_run(run_id)
    instrumented = InstrumentedEventStore(store, _broker, run_id)

    async def _run():
        try:
            result = await run_pipeline(
                app_id=application_id,
                phase=req.phase,
                store=instrumented,
                registry=registry,
                seed_streams=False,
            )
            _broker.push(run_id, "done", {
                "outcome": result.outcome,
                "approved_amount": result.approved_amount,
                "risk_tier": result.risk_tier,
                "fraud_score": str(result.fraud_score) if result.fraud_score is not None else None,
                "compliance_verdict": result.compliance_verdict,
                "total_elapsed_s": result.total_elapsed_s,
            })
        except Exception as exc:
            _broker.push(run_id, "run_error", {"reason": str(exc), "recoverable": False})
        finally:
            _broker.close(run_id)
            await daemon.run_once()
            _runs.pop(run_id, None)

    task = asyncio.create_task(_run())
    _runs[run_id] = task

    return PipelineRunResponse(
        run_id=run_id,
        application_id=application_id,
        phase=req.phase,
    )


@router.get("/pipeline/{application_id}/progress")
async def pipeline_progress(
    application_id: str,
    run_id: str,
):
    async def _generate():
        async for chunk in _broker.stream(run_id):
            yield chunk

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
