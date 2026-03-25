from __future__ import annotations
from datetime import datetime
from typing import Any
from pydantic import BaseModel


class ApplicationSummaryResponse(BaseModel):
    application_id: str
    state: str
    applicant_id: str
    requested_amount_usd: str | None = None
    approved_amount_usd: str | None = None
    risk_tier: str | None = None
    fraud_score: float | None = None
    compliance_status: str | None = None
    decision: str | None = None
    agent_sessions_completed: list[str] = []
    last_event_type: str | None = None
    last_event_at: datetime | None = None
    human_reviewer_id: str | None = None
    final_decision_at: datetime | None = None


class EventResponse(BaseModel):
    event_id: str
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: dict[str, Any]
    metadata: dict[str, Any]
    recorded_at: datetime


class ComplianceRuleResponse(BaseModel):
    rule_id: str
    verdict: str  # PASS | FAIL | NOTE
    failure_reason: str | None = None
    regulation_set: str | None = None
    evaluated_at: datetime | None = None


class ComplianceSnapshotResponse(BaseModel):
    application_id: str
    rules_passed: list[str]
    rules_failed: list[str]
    hard_blocked: bool
    verdict: str | None = None
    as_of: datetime | None = None


class AgentPerformanceResponse(BaseModel):
    agent_id: str
    model_version: str
    analyses_completed: int = 0
    decisions_generated: int = 0
    avg_confidence_score: float | None = None
    avg_duration_ms: float | None = None
    approve_rate: float | None = None
    decline_rate: float | None = None
    refer_rate: float | None = None
    human_override_rate: float | None = None
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None


class AgentSessionResponse(BaseModel):
    session_id: str
    agent_type: str
    agent_id: str
    application_id: str
    model_version: str
    nodes_completed: list[str]
    health_status: str
    session_started: datetime | None = None
    session_completed: datetime | None = None
    session_failed: datetime | None = None
    tools_called: list[dict[str, Any]] = []
    context_text: str | None = None


class DocumentFileResponse(BaseModel):
    filename: str
    app_id: str
    file_path: str
    file_size_bytes: int
    last_modified: datetime


class AppDocumentIndex(BaseModel):
    app_id: str
    document_count: int
    files: list[DocumentFileResponse]


class HealthResponse(BaseModel):
    status: str
    projection_lags_ms: dict[str, float]
    event_count: int
    db_connected: bool


class PipelineRunResponse(BaseModel):
    run_id: str
    application_id: str
    phase: str


class WhatIfResult(BaseModel):
    run_id: str
    application_id: str
    original_decision: dict[str, Any] | None
    shadow_decision: dict[str, Any] | None
    overrides_applied: dict[str, Any]
