"""
scripts/demos/demo_gas_town.py
==============================
Demonstrates the Gas Town persistent ledger pattern: an AI agent's session
stream IS its memory. After a crash, a new process replays the stream and
resumes from exactly where the crashed agent left off.

What you will see:
  1. A credit analysis agent starts a session and executes 3 nodes.
  2. The process is "killed" (the in-memory agent object never existed here).
  3. reconstruct_agent_context() rebuilds the full session state from the
     stream alone — no in-memory object required.
  4. A new AgentSessionRecovered event is appended, pointing to the crashed
     session, and the agent resumes from the next node.

Run:
    uv run python scripts/demos/demo_gas_town.py
"""
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from ledger.event_store import EventStore
from ledger.integrity.gas_town import reconstruct_agent_context, NEEDS_RECONCILIATION

DB_URL = os.environ["DATABASE_URL"]
SEP = "-" * 60

AGENT_ID   = "credit_analysis"
SESSION_ID = "demo-gas-town-sess-001"
APP_ID     = "DEMO-GT-001"


def _ev(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


async def main():
    store = EventStore(DB_URL)
    await store.connect()

    stream_id = f"agent-{AGENT_ID}-{SESSION_ID}"

    # -- clean up any prior run
    async with store._pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM outbox WHERE event_id IN "
            "(SELECT event_id FROM events WHERE stream_id=$1)", stream_id
        )
        await conn.execute("DELETE FROM events        WHERE stream_id=$1", stream_id)
        await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", stream_id)

    now = datetime.now(timezone.utc)

    # ── STEP 1: Start agent session and execute 3 nodes ───────────────────────
    print(SEP)
    print("STEP 1 — Agent starts a credit analysis session, executes 3 nodes")
    print(SEP)

    events = [
        _ev("AgentSessionStarted",
            session_id=SESSION_ID,
            agent_type="credit_analysis",
            agent_id=AGENT_ID,
            application_id=APP_ID,
            model_version="v2.3",
            langgraph_graph_version="1.0",
            context_source="event_store",
            context_token_count=500,
            started_at=now.isoformat()),
        _ev("AgentInputValidated",
            session_id=SESSION_ID,
            agent_type="credit_analysis",
            application_id=APP_ID,
            inputs_validated=["application_id", "applicant_id"],
            validation_duration_ms=12,
            validated_at=now.isoformat()),
        _ev("AgentNodeExecuted",
            session_id=SESSION_ID,
            agent_type="credit_analysis",
            node_name="validate_inputs",
            node_sequence=1,
            input_keys=["application_id"],
            output_keys=["applicant_id"],
            llm_called=False,
            duration_ms=15,
            executed_at=now.isoformat()),
        _ev("AgentNodeExecuted",
            session_id=SESSION_ID,
            agent_type="credit_analysis",
            node_name="load_external_data",
            node_sequence=2,
            input_keys=["applicant_id"],
            output_keys=["company_profile", "financial_history"],
            llm_called=False,
            duration_ms=230,
            executed_at=now.isoformat()),
        _ev("AgentNodeExecuted",
            session_id=SESSION_ID,
            agent_type="credit_analysis",
            node_name="analyze_credit_risk",
            node_sequence=3,
            input_keys=["company_profile", "financial_history"],
            output_keys=["credit_analysis"],
            llm_called=True,
            llm_tokens_input=1400,
            llm_tokens_output=320,
            llm_cost_usd=0.0022,
            duration_ms=3800,
            executed_at=now.isoformat()),
    ]

    await store.append(stream_id, events, expected_version=-1)

    for ev in events:
        print(f"  appended: {ev['event_type']}")
    print(f"\n  Stream {stream_id!r} now has {len(events)} events.")

    # ── STEP 2: Crash ──────────────────────────────────────────────────────────
    print()
    print(SEP)
    print("STEP 2 — Process killed mid-execution")
    print(SEP)
    print()
    print("  *** PROCESS KILLED — in-memory agent state is gone.")
    print("      The stream survives in PostgreSQL. ***")
    print()
    print("  Next node that would have run: write_output")
    print("  In-memory state lost: company_profile, financial_history, credit_analysis")

    # ── STEP 3: Reconstruct from the stream ───────────────────────────────────
    print()
    print(SEP)
    print("STEP 3 — New process calls reconstruct_agent_context()")
    print(SEP)

    ctx = await reconstruct_agent_context(
        store,
        agent_id=AGENT_ID,
        session_id=SESSION_ID,
        agent_type="credit_analysis",
    )

    print(f"  session_id      : {ctx.session_id}")
    print(f"  application_id  : {ctx.application_id}")
    print(f"  model_version   : {ctx.model_version}")
    print(f"  nodes_completed : {ctx.nodes_completed}")
    print(f"  health_status   : {ctx.health_status}")
    print(f"  last_position   : {ctx.last_position}")
    print(f"  context_text    : {ctx.context_text[:200]}...")

    assert ctx.health_status == NEEDS_RECONCILIATION, (
        f"Expected NEEDS_RECONCILIATION, got {ctx.health_status!r}"
    )
    assert len(ctx.nodes_completed) == 3, (
        f"Expected 3 completed nodes, got {ctx.nodes_completed}"
    )
    assert "analyze_credit_risk" in ctx.nodes_completed

    # ── STEP 4: Append recovery event and resume ──────────────────────────────
    print()
    print(SEP)
    print("STEP 4 — Agent appends AgentSessionRecovered and resumes from write_output")
    print(SEP)

    recovery_event = _ev(
        "AgentSessionRecovered",
        session_id=SESSION_ID,
        agent_type="credit_analysis",
        application_id=APP_ID,
        recovered_from_session_id=SESSION_ID,
        recovery_point="write_output",
        recovered_at=datetime.now(timezone.utc).isoformat(),
    )

    ver = await store.stream_version(stream_id)
    await store.append(stream_id, [recovery_event], expected_version=ver)
    print(f"  appended: AgentSessionRecovered")
    print(f"  recovery_point: write_output  (the node AFTER the last completed node)")
    print(f"  recovered_from_session_id: {SESSION_ID}")
    print(f"  Zero nodes repeated — agent resumes exactly where it crashed.")

    print()
    print("PASSED — agent state recovered from stream; no repeated work.")

    await store.close()


if __name__ == "__main__":
    asyncio.run(main())
