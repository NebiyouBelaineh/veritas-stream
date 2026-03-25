"""
api/main.py — FastAPI application factory with lifespan management.

Start with:
    uv run uvicorn api.main:app --reload --port 8000
    (from veritas-stream root)
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.dependencies import startup, shutdown, get_daemon
from api.routers import applications, health, pipeline, documents, agents, whatif, registry


@asynccontextmanager
async def lifespan(app: FastAPI):
    await startup()
    daemon = get_daemon()
    daemon_task = asyncio.create_task(daemon.run())
    yield
    daemon_task.cancel()
    try:
        await daemon_task
    except asyncio.CancelledError:
        pass
    await shutdown()


app = FastAPI(
    title="VeritasStream Ledger API",
    description="REST API for the VeritasStream loan ledger system",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(applications.router, prefix="/api")
app.include_router(health.router, prefix="/api")
app.include_router(pipeline.router, prefix="/api")
app.include_router(documents.router, prefix="/api")
app.include_router(agents.router, prefix="/api")
app.include_router(whatif.router, prefix="/api")
app.include_router(registry.router, prefix="/api")
