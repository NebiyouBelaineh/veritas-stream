# Domain Notes: VeritasStream

Six design decisions and trade-offs documented for the TRP architectural review.

---

## 1. EDA vs Event Sourcing: Why They Are Different

**Event-Driven Architecture (EDA)** uses events as a communication mechanism between services. Events are published, subscribers react, and the event may be discarded after delivery. The *current state* of an entity is stored directly (e.g., a database row) and events are a side-effect of state changes, not their source of truth.

**Event Sourcing (ES)** inverts this: the *event log is the source of truth*. Current state is derived by replaying the event stream from the beginning. Events are immutable facts about what happened; they are never updated or deleted.

**Key practical differences in The Ledger:**

| Concern | EDA | Event Sourcing (our approach) |
|---------|-----|-------------------------------|
| State storage | Mutable DB row | Append-only event stream |
| Audit trail | Separate audit table (secondary) | Built in: every state change is an event |
| Temporal queries | Requires snapshotting | Free: replay up to any timestamp |
| Concurrency | Last-write-wins common | OCC with `expected_version` |
| Upcasting | N/A (message schema versioning) | Required: old events must be readable by new code |

The regulatory requirement that drove this choice: "What was the compliance status of application X at time T?" is trivially answered by replaying `ComplianceRulePassed/Failed` events up to T. With a mutable state row, this would require a separately maintained audit table that might drift from the live state.

---

## 2. Aggregate Boundary Choice and Rejected Alternatives

**What we chose:** Four aggregates, each owning a distinct stream.

| Aggregate | Stream | Owns |
|-----------|--------|------|
| `LoanApplicationAggregate` | `loan-{id}` | State machine, all per-application business rules |
| `AgentSessionAggregate` | `agent-{type}-{session_id}` | Gas Town context, model version locking |
| `ComplianceRecordAggregate` | `compliance-{id}` | Rule verdicts, hard-block detection |
| `AuditLedgerAggregate` | `audit-{entity_type}-{entity_id}` | Integrity hash chain |

**Why four streams, not one?**

Compliance checks are written by multiple agents concurrently (AML agent, KYC agent). If they wrote to the loan stream, they would contend on the same OCC version and all but one would fail. Separating them onto `compliance-{id}` means each compliance check appends at its own stream version with no contention from credit/fraud agents.

**Rejected alternative: one mega-stream per application:**

All events (loan + compliance + agent sessions) in a single stream. Pros: simple queries. Cons: (a) every concurrent writer (5 agents) contends on the same version, making the Double-Decision OCC guarantee very expensive to enforce; (b) aggregate replay becomes O(all events) even for a simple credit check lookup; (c) Gas Town context reconstruction requires filtering out non-agent events.

**Rejected alternative: no aggregates, handlers only:**

Skip aggregates entirely; handlers read raw events and enforce rules procedurally. Rejected because business rules become scattered, untestable in isolation, and the six named rules (state machine, confidence floor, compliance dependency, etc.) become impossible to reason about without loading the full event history on every handler call.

---

## 3. Concurrency Trace: Two Agents, Same Stream, Same `expected_version`

Scenario: Two credit analysis agents both load `loan-APEX-001` at version 3 and race to append `CreditAnalysisCompleted` at `expected_version=3`.

**PostgreSQL path (production):**

```
Agent A:  SELECT current_version FROM event_streams WHERE stream_id='loan-APEX-001'
          → 3
Agent B:  SELECT current_version FROM event_streams WHERE stream_id='loan-APEX-001'
          → 3

Agent A:  BEGIN
Agent A:  INSERT INTO events (...) VALUES (...)
Agent A:  UPDATE event_streams SET current_version=4 WHERE stream_id='...' AND current_version=3
          → 1 row updated (wins)
Agent A:  COMMIT

Agent B:  BEGIN
Agent B:  INSERT INTO events (...)
Agent B:  UPDATE event_streams SET current_version=4 WHERE stream_id='...' AND current_version=3
          → 0 rows updated (current_version is now 4, not 3)
Agent B:  ROLLBACK → raises OptimisticConcurrencyError(expected=3, actual=4)
```

**Result:** Exactly one event is written. The loser receives a typed `OptimisticConcurrencyError` with `expected_version=3` and `actual_version=4`. The MCP tool returns `{"success": False, "error_type": "OptimisticConcurrencyError", "suggested_action": "Reload state and retry."}`.

**InMemoryEventStore path (tests):**

Uses an `asyncio.Lock` instead of a DB transaction. The lock ensures the check-then-append is atomic even within a single Python process.

---

## 4. Projection Lag and UI Communication Strategy

**How lag accumulates:**

The `ProjectionDaemon` polls `load_all(from_position=checkpoint)` every 100 ms (default). Between polls, new events sit in the `events` table but are not yet reflected in projections. Maximum observable lag ≈ poll interval + handler time per batch.

**How we measure it:**

`daemon.get_lag(projection_name)` returns milliseconds since the last event was processed by that projection. A loan officer's dashboard reading `ledger://ledger/health` sees per-projection lag in real time.

**UI communication strategy:**

1. **Optimistic UI:** The client that issued a command immediately shows the expected outcome (e.g., "Submitted, processing...") without waiting for the projection to catch up.
2. **Polling with backoff:** The UI polls `ledger://applications/{id}` every 500 ms until `last_event_type` reflects the expected change, with exponential backoff after 5 s.
3. **Read-your-writes bypass:** Commands return the `stream_id` and `version` of the appended event. The UI can pass this as a hint to a dedicated read endpoint that replays only up to that position, giving immediate consistency for the submitter without blocking the projection pipeline.
4. **Health watchdog:** Operations dashboards alert if `projection_lags_ms[name] > 5000` (5 s), indicating the daemon has stalled.

---

## 5. Upcaster Design for `CreditAnalysisCompleted` v1 → v2

**What changed between versions:**

| Field | v1 | v2 |
|-------|----|----|
| `regulatory_basis` | absent | `list[str]` (EU AI Act article citations) |
| `confidence_score` | absent | `float \| None` (explicit model confidence) |
| `model_version` | absent | `str` (inferred from deployment timeline) |

**Inference strategy:**

```python
_MODEL_VERSION_TIMELINE = [
    (datetime(2026, 3, 1), "v2.4"),
    (datetime(2026, 1, 1), "v2.3"),
    (datetime(2025, 7, 1), "v2.2"),
    (datetime(2025, 1, 1), "v2.1"),
]
```

For `model_version`: we know from deployment records which model was live at each date. `recorded_at` is a reliable proxy because it is set by the store at append time and cannot be spoofed by the event payload.

For `confidence_score`: we explicitly set `None`. The requirement is *"do not fabricate unknowns"*. A fabricated confidence score would mislead downstream risk analyses about the reliability of historical decisions. Presence of the field as `None` signals v2 format to consumers without inventing data.

For `regulatory_basis`: we set `[]` (empty list). Pre-2026 events predated EU AI Act citation requirements. An empty list correctly models "no citations recorded" without implying compliance was not checked.

**Immutability contract:**

The upcaster always returns a *new dict* (`{**event, "payload": dict(event["payload"])}`). The stored event is never touched. This is verified by `test_upcasting_does_not_modify_stored_event`, which snapshots the raw payload before upcasting and asserts byte-for-byte identity after.

---

## 6. Marten Async Daemon Parallel: Python Equivalent

**Marten's Async Daemon** (C#/.NET) is a background process that projects events into read models by reading from the `mt_events` table in order of `sequence` (global position). It supports:
- Multiple projections running in parallel
- Per-projection checkpoints (resumable after restart)
- Retry/skip on bad events
- Inline (synchronous) and async (background) modes

**Our Python equivalent: `ProjectionDaemon`:**

```python
class ProjectionDaemon:
    async def run_once(self) -> int:
        # Loads events past the lowest checkpoint
        async for event in self.store.load_all(from_position=min_checkpoint):
            for name, projection in self.projections.items():
                if gpos <= await self._load_checkpoint(name):
                    continue  # already processed
                await projection.handle(event)  # with retry/skip
                await self._save_checkpoint(name, gpos)

    async def run(self, poll_interval=0.1):
        while self._running:
            await self.run_once()
            await asyncio.sleep(poll_interval)
```

**Key differences from Marten:**

| Concern | Marten Async Daemon | Our ProjectionDaemon |
|---------|---------------------|----------------------|
| Parallelism | Thread-pool across projections | Sequential fan-out (asyncio single-threaded) |
| Checkpoints | PostgreSQL `mt_event_progression` | `projection_checkpoints` table (or in-memory) |
| Fault tolerance | Configurable dead-letter queue | Log + skip after `max_retry` attempts |
| Inline mode | Synchronous in same transaction | Not supported (separate polling loop) |
| Rebuild | `RebuildAsync()` | `rebuild_from_scratch(name)`: truncate + replay |

**Parallelism trade-off:** Marten spawns one shard per projection type, running them truly in parallel on a thread pool. Our daemon processes projections sequentially per event. For the current load (< 1000 events/s), sequential fan-out is sufficient and avoids thread-safety concerns in projection state. For higher throughput, each projection could be moved to its own `asyncio.Task` with a shared queue.
