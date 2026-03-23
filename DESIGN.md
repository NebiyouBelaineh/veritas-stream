# DESIGN.md: VeritasStream Architectural Decisions

Six sections documenting the design decisions, tradeoffs, and implementation
choices made in building The Ledger event sourcing infrastructure.

---

## Section 1: Data Boundary Decisions

The system touches seven distinct data categories. For each I document where it
lives, who holds write authority, and why moving it would introduce a coupling
problem.

### applicant_registry.companies

Lives in the Applicant Registry (read-only to The Ledger). The originating CRM
system is the single writer of company identity records. If The Ledger were to
write here, two systems would own the same entity: a company name change in the
CRM would not propagate to The Ledger's copy, creating a forked record with no
clear canonical version. The registry client (`ledger/registry/client.py`)
enforces this boundary: it exposes only query methods with no mutation surface.

### applicant_registry.financial_history

Lives in the Applicant Registry (read-only). Historical financials originate
from the applicant's accounting system and are a durable external record. Writing
them into the event store would mean The Ledger makes an authoritative claim about
a fact it did not produce. More importantly, if the applicant later submits a
restatement, The Ledger would need to decide whether to update its copy or append
a correction event (neither is correct, because the authoritative correction
lives in the source accounting system).

### applicant_registry.compliance_flags

Lives in the Applicant Registry (read-only). Compliance flags are written by
external screening vendors (OFAC, PEP list providers). The Ledger reads them as
inputs to agent decisions; it does not produce them. If The Ledger wrote
compliance flags, it would simultaneously be a reader and a writer of the same
record. Downstream agents reading those flags would be reading The Ledger's own
output rather than an independent source, creating circular reasoning in the
compliance chain that a regulator cannot unwind.

### applicant_registry.loan_relationships

Lives in the Applicant Registry (read-only). Cross-institution loan history is a
regulatory data product supplied by credit bureaus. The Ledger has no write
authority here by definition: it processes loan applications, it does not
maintain the bureau's loan relationship database.

### events table

Lives in the event store (append-only). The loan processing facts produced by
The Ledger (who decided what, when, with which confidence, via which model
version) exist only as a result of The Ledger's agents running. No other system
produces these facts. The `events` table is therefore the natural home, and
append-only is the correct access pattern: once a credit decision is recorded, the
only valid correction is a `CreditAnalysisCorrected` compensating event, not an
UPDATE.

### Projection tables (application_summary, compliance_audit_view, agent_performance_ledger)

Lives in derived read model tables. These are deterministic projections of the
event stream: given the same events in the same order, the same projection state
is always produced. They can be truncated and rebuilt from `global_position=0`
at any time via `ProjectionDaemon.rebuild_from_scratch()`. Treating them as
authoritative would be a mistake: if a handler bug produced wrong values, the
only recovery is to fix the handler and rebuild, not to patch the projection rows
directly.

### outbox table

Lives in the same transaction as the events it tracks. The outbox row and its
corresponding event row are written atomically in `EventStore.append()`. This
guarantees at-least-once delivery even if the process crashes between writing the
event and publishing it externally: on restart, the outbox relay re-reads
unpublished rows. If the outbox were written in a separate transaction, a crash
between the two transactions would silently drop the event.

---

## Section 2: Aggregate Boundary Justification

The implementation uses four aggregate stream types. I considered and rejected
additional stream types for credit and fraud records; the reasoning is in the
retrospective.

### loan-{application_id} (LoanApplicationAggregate)

**Why this boundary:** The loan stream owns the application state machine. Every
state transition (SUBMITTED to DOCUMENTS_PENDING to CREDIT_ANALYSIS_COMPLETE and
so on) must be totally ordered: no two transitions can be valid simultaneously.
A single stream with OCC on `stream_position` enforces this ordering without
requiring any distributed lock.

**Rejected alternative:** Merging the loan stream with the compliance stream into
a single per-application stream. This would force the compliance handler and the
credit analysis handler to contend on the same `stream_position` when processing
concurrent checks. Under the current boundary, `handle_compliance_check` appends
to `compliance-{id}` while `handle_credit_analysis_completed` appends to
`loan-{id}`: no contention between the two.

**OCC implication:** Only one handler advances the loan state machine at a time.
This is correct by design: the state machine itself enforces sequential
progression through pipeline stages, so only one transition is valid at any
point.

### compliance-{application_id} (ComplianceRecordAggregate)

**Why this boundary:** Multiple compliance rules (AML-001, KYC-001) can be
evaluated concurrently by independent agent calls. Each `handle_compliance_check`
call appends to `compliance-{id}` at its own `stream_position`. If AML and KYC
checks run in parallel, AML appends at version 0 and KYC appends at version 1
with no contention between them.

**Rejected alternative:** Writing compliance rule events directly to `loan-{id}`.
This would force AML and KYC rule evaluations to serialize on the loan stream
version. If both start from version 3, exactly one succeeds and the other must
reload and retry: turning what is naturally a parallel operation into a
serialized one with unnecessary retry overhead.

**OCC implication:** ComplianceAgent and CreditAnalysisAgent write to
`compliance-{id}` and `loan-{id}` respectively. They never share a `stream_position`
and therefore never collide.

### agent-{agent_type}-{session_id} (AgentSessionAggregate)

**Why this boundary:** Each agent session needs an independent append sequence
for the Gas Town crash recovery pattern. The session stream is one-writer by
construction: only the agent that holds `session_id` ever appends to
`agent-{type}-{session_id}`. This means session stream appends never collide.

**Rejected alternative:** Appending session events to `loan-{id}`. This would
make crash recovery require scanning the full loan stream to find agent events,
breaking `reconstruct_agent_context()` which relies on loading a single small
stream for the crashed session. It would also cause session events to contend
with state machine transitions on the loan stream.

**OCC implication:** No cross-session collision is possible by construction.
Each `session_id` is unique, so each session stream is independent of every
other.

### audit-{entity_type}-{entity_id} (AuditLedgerAggregate)

**Why this boundary:** Integrity checks write `AuditIntegrityCheckRun` events to
a separate audit stream rather than to the entity stream being audited. This
ensures that running `run_integrity_check()` on `loan-APEX-001` does not contend
with the loan state machine advancing on `loan-APEX-001`. If audit events were
appended to the loan stream, a concurrent integrity check would bump the loan
stream version, forcing any concurrent loan event to reload and retry
unnecessarily.

**Rejected alternative:** Storing integrity check results in a separate
`audit_runs` table. Rejected because a table row is mutable; a malicious actor
who can UPDATE the `audit_runs` table can fabricate a clean audit record. Storing
the result as an immutable event in the append-only `events` table eliminates
this attack surface.

**OCC implication:** Audit events and loan state machine events write to
different streams. An integrity check running concurrently with a loan approval
produces zero OCC collisions.

---

## Section 3: Concurrency Analysis

### The production OCC mechanism

`EventStore.append()` enforces OCC via a `SELECT ... FOR UPDATE` on the
`event_streams` row, followed by a manual version check inside the transaction:

```python
row = await conn.fetchrow(
    "SELECT current_version, archived_at FROM event_streams "
    "WHERE stream_id = $1 FOR UPDATE", stream_id
)
current = row["current_version"] if row else -1
if current != expected_version:
    raise OptimisticConcurrencyError(stream_id, expected_version, current)
```

The `FOR UPDATE` row lock ensures that two concurrent transactions holding the
same `expected_version=3` cannot both pass the version check: the first acquires
the lock, checks version (3 == 3, passes), appends, updates `current_version` to
4, and commits. The second then acquires the lock, checks version (4 != 3), and
raises `OptimisticConcurrencyError(expected=3, actual=4)`.

The `InMemoryEventStore` (used in tests) achieves the same guarantee via an
`asyncio.Lock` per stream, making the check-then-append atomic within a single
Python process.

### Expected OCC error rate under peak load

At 100 concurrent applications with 4 agents each, the expected OCC collision
rate on `loan-{id}` is near zero. The reason: the state machine enforces stage
ordering. `handle_credit_analysis_completed` requires the loan to be in the
DOCUMENTS_PENDING state; `handle_fraud_screening_completed` requires
CREDIT_ANALYSIS_COMPLETE. These stages cannot execute concurrently on the same
application because each handler calls `loan_agg.assert_valid_transition()`; the
second handler to run finds the loan in the wrong state and raises `DomainError`,
not `OptimisticConcurrencyError`.

The only genuine OCC risk is two instances of the same agent type being launched
for the same application. This is prevented upstream by the Gas Town contract:
`handle_start_agent_session` checks `await store.stream_version(stream_id) != -1`
and raises `DomainError: Agent session already exists` before any decision event
is attempted.

Under 100 concurrent applications, each application's streams are independent.
The shared resource under true concurrency (400 concurrent writers) is the
PostgreSQL connection pool (min_size=2, max_size=10 per `EventStore.__init__`).
Connection pool exhaustion is the more likely bottleneck than OCC collisions.

### What the OCC loser receives

`OptimisticConcurrencyError(stream_id, expected=3, actual=4)` with the
stream ID, expected version, and actual version as structured fields. The MCP
tool layer wraps this as:

```json
{
  "success": false,
  "error_type": "OptimisticConcurrencyError",
  "stream_id": "loan-APEX-001",
  "expected_version": 3,
  "actual_version": 4,
  "suggested_action": "reload_stream_and_retry"
}
```

### Retry strategy

Exponential backoff with jitter, maximum 3 retries. On each retry the handler
reloads the aggregate (`LoanApplicationAggregate.load()`) to re-read the current
stream state, re-validates the business rules against the updated state, then
re-attempts the append. If the new events make the handler's work redundant (for
example, another agent already appended `CreditAnalysisCompleted`), the handler
must detect this via `loan_agg.assert_credit_analysis_not_locked()` and return
without appending a duplicate.

After 3 failed retries, the agent session appends `AgentSessionFailed` with
`recoverable=True` and the `last_successful_node` for Gas Town recovery.

---

## Section 4: Upcasting Inference Decisions

Two event types require upcasting from v1 to v2: `CreditAnalysisCompleted` and
`DecisionGenerated`. The `UpcasterRegistry` in `ledger/upcasters.py` applies
these transforms in `load_stream()` and `load_all()`, never on append.

### Immutability contract

The upcaster always operates on a copy of the event dict; it never modifies the
stored row. The stored event is verified to be unchanged by
`test_upcasting_does_not_modify_stored_event`, which snapshots the raw payload
before upcasting and asserts byte-for-byte identity after calling
`upcast()`.

### CreditAnalysisCompleted v1 to v2

Three fields added in v2:

**`regulatory_basis` (set to `[]`):** Pre-2026 events predated the EU AI Act
citation requirement. An empty list correctly models "no citations were recorded"
without fabricating citations that were never made. Downstream code that renders
regulatory basis handles an empty list as "no citations required at the time of
this analysis."

**`confidence_score` (set to `None`):** The v1 model did not output a confidence
score. I chose null over fabrication for this reason: `confidence_score` is used
downstream in `apply_policy_constraints` to cap confidence at 0.50 when an active
HIGH fraud flag is present. A fabricated confidence of 0.75 would bypass a
constraint that should have applied to historical decisions. Null is safe;
fabrication would silently corrupt historical compliance analysis.

**`model_version` (inferred from `recorded_at`):** Deployment records show which
model was live at each date. Since `recorded_at` is set by the store at append
time and cannot be spoofed by the event payload, it is a reliable proxy. The
inference has approximately 5% error rate for events recorded within 48 hours of
a deployment boundary. The consequence of an incorrect `model_version` is
cosmetic: it would show the wrong label in the audit trail but would not change
the credit decision or affect compliance calculations. Inference is safe here.

The inference timeline used:

```python
_MODEL_VERSION_TIMELINE = [
    (datetime(2026, 3, 1), "v2.4"),
    (datetime(2026, 1, 1), "v2.3"),
    (datetime(2025, 7, 1), "v2.2"),
    (datetime(2025, 1, 1), "v2.1"),
]
```

### DecisionGenerated v1 to v2

**`model_versions` (set to `{}`):** The v1 schema did not record which model
versions contributed to a decision. An empty dict honestly signals "versions were
not recorded" without fabricating a mapping. A fabricated `{"credit": "v2.1"}`
would misrepresent which model versions were actually used in historical decisions
reviewed by regulators.

### When null beats fabrication

The rule I applied: if the missing field participates in any calculation that
affects a regulatory outcome (compliance verdict, risk tier, confidence cap),
null is the correct default. A null value forces consuming code to handle the
"unknown" case explicitly, which is safer than silently using a fabricated value
that might not trigger the intended safety check.

If the missing field is display-only (an audit trail label, a version string for
a dashboard), inference is acceptable provided the error rate is below 5% and
the inference method is deterministic and documented.

---

## Section 5: EventStoreDB Comparison

Mapping the PostgreSQL implementation to EventStoreDB concepts, and identifying
where the implementation works harder than a native event store would require.

| PostgreSQL implementation | EventStoreDB equivalent | Gap |
|---|---|---|
| `UNIQUE(stream_id, stream_position)` + `SELECT ... FOR UPDATE` | Native stream append with `WrongExpectedVersion` | PostgreSQL requires a row lock; EventStoreDB's OCC is built into the append protocol |
| `global_position GENERATED ALWAYS AS IDENTITY` | `$all` stream global position | Functional parity; both provide a monotonic global sequence |
| `load_all(from_position)` polling in `ProjectionDaemon` | Catch-up Subscription on `$all` | EventStoreDB delivers events server-push; our daemon polls every 100ms |
| `UpcasterRegistry` applied in `load_stream()` | No built-in upcasting; client-side transform required | Parity: both require application code |
| `outbox` table written in same transaction | Persistent Subscriptions provide at-least-once by design | EventStoreDB eliminates the outbox table entirely |
| `archive_stream()` sets `archived_at` | `$maxAge` / `$maxCount` stream metadata | Different model: EventStoreDB supports age-based truncation as a native stream policy |
| `run_integrity_check()` writing `AuditIntegrityCheckRun` | No native equivalent | Both systems require application-level hash chain implementation |
| `projection_checkpoints` table | EventStoreDB tracks subscription position server-side | Our daemon must explicitly save checkpoints; a crash between projection update and checkpoint save risks reprocessing |

### The most significant gap: polling versus push

The `ProjectionDaemon` wakes every 100ms and re-scans
`global_position > min_checkpoint`. EventStoreDB's persistent subscriptions
deliver events server-push as they are committed, with backpressure and
at-least-once guarantees built into the subscription protocol.

The polling model has two costs. First, it introduces up to 100ms of artificial
projection lag before a handler runs. Under the p99 less than 500ms SLO for
`ApplicationSummary`, 100ms polling latency consumes 20% of the budget before
any handler work begins.

Second, the checkpoint update in `_save_checkpoint()` is not atomic with the
projection state update in PostgreSQL production use. If `projection.handle()`
writes to a database table and then the process crashes before
`_save_checkpoint()` completes, the daemon reprocesses the event on restart.
This makes all projection handlers idempotent by necessity. EventStoreDB
persistent subscriptions track the subscription position server-side, eliminating
this race entirely.

### What EventStoreDB provides that this implementation works hardest to replicate

The combination of server-push delivery, server-side checkpoint tracking, and
built-in at-least-once guarantees removes the need for the outbox table, the
polling loop, the checkpoint atomicity concern, and most of the `ProjectionDaemon`
infrastructure. All of that work exists in this codebase specifically because
PostgreSQL is a general-purpose database rather than a purpose-built event store.

---

## Section 6: Retrospective

### Week 4: Collapsing credit and fraud records into the loan stream

In the current implementation, `CreditAnalysisCompleted` and
`FraudScreeningCompleted` are appended to `loan-{id}` rather than to
`credit-{id}` and `fraud-{id}` as separate aggregate streams. The architecture
document (`architecture.md`) describes six stream types including `credit-` and
`fraud-`; the implementation converged on four.

The practical consequence: the CreditAnalysisAgent and FraudDetectionAgent both
append to `loan-{id}`. Under the current sequential pipeline (credit completes
before fraud starts), this causes zero OCC collisions. But the architecture.md
description implies these agents could eventually run concurrently; under
concurrent execution, both agents would race to append to `loan-{id}`, forcing
one to retry.

With another day I would extract `CreditAnalysisCompleted` to a `credit-{id}`
stream and `FraudScreeningCompleted` to a `fraud-{id}` stream, matching the
architecture document. The loan stream would receive only the state transition
trigger events (`CreditAnalysisRequested`, `FraudScreeningRequested`), not the
analysis results themselves. This eliminates the future OCC collision risk and
more clearly separates the concerns of the analysis agents from the loan state
machine.

### Week 5: Checkpoint atomicity in the ProjectionDaemon

The `ProjectionDaemon._save_checkpoint()` call at line 119 of `daemon.py` runs
after `projection.handle(event)` completes but as a separate operation, not
inside the same database transaction as the projection's state update. For the
`InMemoryEventStore` used in tests, this is harmless: both operations are
in-memory and effectively atomic within a single asyncio task. For a PostgreSQL
projection that writes to a database table, a crash between the handler commit
and the checkpoint save means the event is reprocessed on restart.

The current design sidesteps this by requiring all projection handlers to be
idempotent (INSERT ... ON CONFLICT DO UPDATE). That works, but it shifts the
correctness burden from the infrastructure to every projection handler. With
another day I would wrap the handler call and the checkpoint save in a single
PostgreSQL transaction, documented as the atomicity guarantee in
`_save_checkpoint()`. This would allow non-idempotent handlers and make the
infrastructure's correctness guarantee explicit rather than implicit.
