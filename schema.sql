-- schema.sql — The Ledger: Event Store Schema
-- Apply with: psql $DATABASE_URL -f schema.sql
-- Idempotent: safe to run on an existing database.

-- ─── CORE EVENT STORE ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id       TEXT NOT NULL,
    stream_position BIGINT NOT NULL,
    global_position BIGINT GENERATED ALWAYS AS IDENTITY,
    event_type      TEXT NOT NULL,
    event_version   SMALLINT NOT NULL DEFAULT 1,
    payload         JSONB NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    -- Enforces append-only semantics at the DB level:
    -- two concurrent writes at the same position are rejected even if
    -- application-level OCC is bypassed.
    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

CREATE INDEX IF NOT EXISTS idx_events_stream_id  ON events (stream_id, stream_position);
CREATE INDEX IF NOT EXISTS idx_events_global_pos ON events (global_position);
CREATE INDEX IF NOT EXISTS idx_events_type       ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_recorded   ON events (recorded_at);

-- ─── STREAM REGISTRY ─────────────────────────────────────────────────────────

-- Tracks current version per stream to enable optimistic concurrency control
-- (SELECT FOR UPDATE on this row serialises concurrent appends without
-- requiring application-level distributed locks).
CREATE TABLE IF NOT EXISTS event_streams (
    stream_id       TEXT PRIMARY KEY,
    aggregate_type  TEXT NOT NULL,
    current_version BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at     TIMESTAMPTZ,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- ─── PROJECTION CHECKPOINTS ──────────────────────────────────────────────────

-- Stores the last global_position processed by each projection.
-- The ProjectionDaemon reads and updates these to enable fault-tolerant
-- resumable replay without reprocessing the full event history.
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    last_position   BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── OUTBOX ──────────────────────────────────────────────────────────────────

-- Transactional outbox for guaranteed event delivery to external systems.
-- Written in the same transaction as the event insert, so an outbox entry
-- cannot exist without its corresponding event (enforced by FK below).
-- A background publisher polls this table and delivers to message brokers.
CREATE TABLE IF NOT EXISTS outbox (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id     UUID NOT NULL REFERENCES events(event_id),
    destination  TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    attempts     SMALLINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (created_at)
    WHERE published_at IS NULL;

-- ─── PROJECTION READ MODELS ──────────────────────────────────────────────────
-- Added in Branch 4 (feat/projections). Kept here so schema.sql is the
-- single source of truth for all DDL.

CREATE TABLE IF NOT EXISTS application_summary (
    application_id          TEXT PRIMARY KEY,
    state                   TEXT NOT NULL,
    applicant_id            TEXT,
    requested_amount_usd    NUMERIC,
    approved_amount_usd     NUMERIC,
    risk_tier               TEXT,
    fraud_score             NUMERIC,
    compliance_status       TEXT,
    decision                TEXT,
    agent_sessions_completed TEXT[] NOT NULL DEFAULT '{}',
    last_event_type         TEXT,
    last_event_at           TIMESTAMPTZ,
    human_reviewer_id       TEXT,
    final_decision_at       TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS agent_performance_ledger (
    agent_id            TEXT NOT NULL,
    model_version       TEXT NOT NULL,
    analyses_completed  INTEGER NOT NULL DEFAULT 0,
    decisions_generated INTEGER NOT NULL DEFAULT 0,
    avg_confidence_score NUMERIC,
    avg_duration_ms     NUMERIC,
    approve_rate        NUMERIC,
    decline_rate        NUMERIC,
    refer_rate          NUMERIC,
    human_override_rate NUMERIC,
    first_seen_at       TIMESTAMPTZ,
    last_seen_at        TIMESTAMPTZ,
    PRIMARY KEY (agent_id, model_version)
);

CREATE TABLE IF NOT EXISTS compliance_audit_view (
    application_id      TEXT NOT NULL,
    rule_id             TEXT NOT NULL,
    rule_version        TEXT,
    verdict             TEXT NOT NULL,
    failure_reason      TEXT,
    regulation_set      TEXT,
    evaluated_at        TIMESTAMPTZ,
    evidence_hash       TEXT,
    recorded_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (application_id, rule_id)
);

-- Snapshot table for ComplianceAuditView temporal queries.
-- A snapshot is written periodically so that get_compliance_at(id, ts)
-- can start from the nearest snapshot rather than replaying from position 0.
CREATE TABLE IF NOT EXISTS compliance_audit_snapshots (
    application_id   TEXT NOT NULL,
    snapshot_at      TIMESTAMPTZ NOT NULL,
    global_position  BIGINT NOT NULL,
    state_json       JSONB NOT NULL,
    snapshot_version INT NOT NULL DEFAULT 1,
    PRIMARY KEY (application_id, snapshot_at)
);
COMMENT ON COLUMN compliance_audit_snapshots.snapshot_version IS
    'Projection logic version. Snapshots with a version lower than the current '
    'projection version are stale and must be ignored during temporal queries.';
