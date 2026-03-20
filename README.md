# VeritasStream — The Ledger

**Event-sourcing infrastructure for Apex Financial Services' AI-driven loan processing platform.**

The Ledger provides append-only event streams, aggregate replay, cryptographic audit chains, and an MCP server interface so AI agents can write commands and read projections through a single, LLM-consumable API.

---

## System Architecture

```mermaid
flowchart TD
    Agent([AI Agent / LLM]) -->|MCP Tool call| MCP[MCP Server\nledger/mcp/]
    Agent -->|MCP Resource read| MCP
    MCP -->|Commands| Handlers[Command Handlers\nledger/commands/handlers.py]
    MCP -->|Query| Projections[Projections\nledger/projections/]
    Handlers -->|append| ES[(EventStore\nPostgreSQL / InMemory)]
    ES -->|load_all| Daemon[ProjectionDaemon]
    Daemon --> AppSummary[ApplicationSummary]
    Daemon --> ComplianceAudit[ComplianceAuditView]
    Daemon --> AgentPerf[AgentPerformanceLedger]
    Projections --> AppSummary
    Projections --> ComplianceAudit
    Projections --> AgentPerf
    Handlers -->|load_stream| Aggregates[Domain Aggregates\nledger/domain/aggregates/]
    AuditChain[Audit Hash Chain\nledger/integrity/] -->|SHA-256 chain| ES
    GasTown[Gas Town\nledger/integrity/] -->|reconstruct context| ES
    Upcasting[Upcasters\nledger/upcasting/] -->|v1→v2 on read| ES
```

---

## Quick Start

### 1. Install dependencies (requires Python 3.12)

```bash
uv sync
```

### 2. Start the database

```bash
docker compose up -d
```

This starts PostgreSQL on port **5433** (host) mapped to 5432 (container).

### 3. Run database migrations

```bash
DATABASE_URL=postgresql://postgres:apex@localhost:5433/apex_ledger \
  uv run psql -U postgres -h localhost -p 5433 -d apex_ledger -f schema.sql
```

### 4. Run all tests

```bash
# In-memory tests (no database required)
uv run pytest tests/ --ignore=tests/test_schema.py --ignore=tests/test_event_store.py -v

# Schema + EventStore tests (requires PostgreSQL on port 5433)
DATABASE_URL=postgresql://postgres:apex@localhost:5433/apex_ledger \
  uv run pytest tests/test_schema.py tests/test_event_store.py -v
```

### 5. Run the MCP server

```python
from ledger.event_store import EventStore
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.mcp.server import create_mcp_server

store = EventStore("postgresql://postgres:apex@localhost:5433/apex_ledger")
summary = ApplicationSummaryProjection()
compliance = ComplianceAuditViewProjection()
perf = AgentPerformanceLedgerProjection()
daemon = ProjectionDaemon(store, [summary, compliance, perf])

mcp = create_mcp_server(store, daemon, {
    "summary": summary,
    "compliance": compliance,
    "agent_performance": perf,
})
mcp.run()
```

---

## Event Flow — Full Loan Lifecycle

```mermaid
sequenceDiagram
    participant Client
    participant MCP as MCP Server
    participant Handlers as Command Handlers
    participant ES as EventStore
    participant Daemon as ProjectionDaemon

    Client->>MCP: submit_application
    MCP->>Handlers: handle_submit_application
    Handlers->>ES: append("loan-{id}", ApplicationSubmitted)

    Client->>MCP: start_agent_session (credit)
    MCP->>Handlers: handle_start_agent_session
    Handlers->>ES: append("agent-credit-{sess}", AgentSessionStarted + AgentInputValidated)

    Client->>MCP: record_credit_analysis
    MCP->>Handlers: handle_credit_analysis_completed
    Handlers->>ES: append("loan-{id}", CreditAnalysisCompleted)

    Client->>MCP: record_fraud_screening
    Client->>MCP: record_compliance_check (×N rules)
    Client->>MCP: generate_decision
    MCP->>Handlers: handle_generate_decision
    Handlers->>ES: append("loan-{id}", DecisionGenerated)

    Client->>MCP: record_human_review
    Handlers->>ES: append("loan-{id}", HumanReviewCompleted)

    ES-->>Daemon: load_all (poll)
    Daemon-->>Client: ledger://applications/{id} → FinalApproved
```

---

## Aggregate State Machine

```mermaid
stateDiagram-v2
    [*] --> SUBMITTED: ApplicationSubmitted
    SUBMITTED --> DOCUMENTS_PENDING: DocumentsRequested
    DOCUMENTS_PENDING --> DOCUMENTS_UPLOADED: DocumentsUploaded
    DOCUMENTS_UPLOADED --> DOCUMENTS_PROCESSED: DocumentsProcessed
    DOCUMENTS_PROCESSED --> UNDER_REVIEW: UnderReview
    UNDER_REVIEW --> PENDING_DECISION: DecisionGenerated (APPROVE/DECLINE)
    UNDER_REVIEW --> PENDING_HUMAN_REVIEW: DecisionGenerated (REFER)
    PENDING_HUMAN_REVIEW --> APPROVED: HumanReviewCompleted (APPROVED)
    PENDING_HUMAN_REVIEW --> DECLINED: HumanReviewCompleted (DECLINED)
    PENDING_DECISION --> APPROVED: ApplicationApproved
    PENDING_DECISION --> DECLINED: ApplicationDeclined
    APPROVED --> [*]
    DECLINED --> [*]
```

---

## Test Suite

```bash
# Branch 1 — Schema (requires PostgreSQL)
pytest tests/test_schema.py -v

# Branch 2 — Domain aggregates + business rules
pytest tests/test_aggregates.py -v

# Branch 3 — Command handlers + lifecycle
pytest tests/test_command_handlers.py -v

# Branch 4 — Projections + daemon
pytest tests/test_projections.py -v

# Branch 5 — Upcasting + integrity
pytest tests/test_upcasting.py tests/test_integrity.py -v

# Branch 6 — MCP integration
pytest tests/test_mcp_integration.py -v

# All in-memory tests at once
pytest tests/ --ignore=tests/test_schema.py --ignore=tests/test_event_store.py -v
```

**Coverage by branch:**

| Branch | Test file | Requirements proven |
|--------|-----------|---------------------|
| 1 | `test_schema.py` | Append-only constraint, outbox FK, identity-based global ordering |
| 2 | `test_aggregates.py` | All 6 named business rules (state machine, Gas Town, model lock, confidence floor, compliance dependency, causal chain) |
| 3 | `test_command_handlers.py` | Double-Decision Test, Gas Town enforcement, full lifecycle, OCC losers get typed error |
| 4 | `test_projections.py` | ApplicationSummary SLO < 500 ms, ComplianceAuditView SLO < 2 s, temporal query, rebuild determinism, daemon fault tolerance |
| 5 | `test_upcasting.py` | Mandatory TRP immutability, null-over-fabrication for unknown fields |
| 5 | `test_integrity.py` | Tamper detection, Gas Town crash recovery, NEEDS_RECONCILIATION on partial state |
| 6 | `test_mcp_integration.py` | Full lifecycle via MCP only, structured error types, projection-backed resources, health watchdog |

---

## MCP Tools and Resources

### Tools (commands — write side)

| Tool | Description |
|------|-------------|
| `submit_application` | Submit a new loan application |
| `start_agent_session` | Open an agent session (Gas Town contract) |
| `record_credit_analysis` | Record credit analysis result |
| `record_fraud_screening` | Record fraud screening result |
| `record_compliance_check` | Record a compliance rule evaluation |
| `generate_decision` | Generate final lending decision |
| `record_human_review` | Record human reviewer outcome |
| `run_integrity_check` | Run cryptographic audit hash-chain check |

### Resources (projections — read side)

| Resource URI | Description |
|--------------|-------------|
| `ledger://applications/{id}` | ApplicationSummary read model |
| `ledger://applications/{id}/compliance` | Current compliance state |
| `ledger://applications/{id}/compliance/{as_of}` | Temporal compliance query |
| `ledger://applications/{id}/audit-trail` | Full event history (stream replay) |
| `ledger://agents/{agent_id}/performance` | Agent performance statistics |
| `ledger://agents/{agent_id}/sessions/{session_id}` | Gas Town session reconstruction |
| `ledger://ledger/health` | Projection daemon lag report |

---

## Project Structure

```
veritas-stream/
├── ledger/
│   ├── event_store.py              # EventStore (PostgreSQL) + InMemoryEventStore
│   ├── commands/
│   │   └── handlers.py             # 7 command handlers (load → validate → append)
│   ├── domain/
│   │   └── aggregates/
│   │       ├── loan_application.py # LoanApplicationAggregate (13 event types, 6 rules)
│   │       ├── agent_session.py    # AgentSessionAggregate (Gas Town)
│   │       ├── compliance_record.py
│   │       └── audit_ledger.py
│   ├── projections/
│   │   ├── daemon.py               # ProjectionDaemon (checkpoint-aware fan-out)
│   │   ├── application_summary.py  # ApplicationSummaryProjection
│   │   ├── compliance_audit.py     # ComplianceAuditViewProjection (temporal queries)
│   │   └── agent_performance.py    # AgentPerformanceLedgerProjection
│   ├── upcasting/
│   │   ├── registry.py             # UpcasterRegistry (immutability contract)
│   │   └── upcasters.py            # CreditAnalysisCompleted v1→v2, DecisionGenerated v1→v2
│   ├── integrity/
│   │   ├── audit_chain.py          # SHA-256 hash chain tamper detection
│   │   └── gas_town.py             # reconstruct_agent_context (crash recovery)
│   └── mcp/
│       ├── server.py               # create_mcp_server() factory
│       ├── tools.py                # 8 MCP tools
│       └── resources.py            # 7 MCP resources
├── tests/
│   ├── test_schema.py
│   ├── test_aggregates.py
│   ├── test_command_handlers.py
│   ├── test_projections.py
│   ├── test_upcasting.py
│   ├── test_integrity.py
│   └── test_mcp_integration.py
├── schema.sql                      # PostgreSQL DDL (events, event_streams, projections, outbox)
├── docker-compose.yml              # PostgreSQL 16 on port 5433
└── pyproject.toml
```
