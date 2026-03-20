"""
ledger/integrity/gas_town.py
==============================
Gas Town pattern — reconstruct an agent's operational context from its event
stream so it can resume work after a crash or restart.

Named after the "Gas Town" district in Mad Max: Fury Road — a place that keeps
running even when its operators are gone, as long as the records survive.

Algorithm:
  1. Load the agent session stream.
  2. Classify events: completed, pending, error.
  3. Summarise into a prose context string (token-budget aware).
  4. Return AgentContext with health_status:
       HEALTHY             — session completed normally
       NEEDS_RECONCILIATION — session started but did not complete
         (interrupted mid-run, or last event is an in-progress node)
       FAILED              — session failed with unrecoverable error

The returned AgentContext carries enough information for the agent to decide
whether to resume, retry, or escalate.
"""
from __future__ import annotations

from dataclasses import dataclass, field


# ── Health status constants ───────────────────────────────────────────────────

HEALTHY = "HEALTHY"
NEEDS_RECONCILIATION = "NEEDS_RECONCILIATION"
FAILED = "FAILED"


@dataclass
class AgentContext:
    session_id: str
    agent_type: str | None
    agent_id: str | None
    application_id: str | None
    model_version: str | None
    context_text: str                       # prose summary for LLM prompt
    last_position: int                      # last stream_position seen
    nodes_completed: list[str] = field(default_factory=list)
    pending_work: list[str] = field(default_factory=list)
    health_status: str = NEEDS_RECONCILIATION
    session_started: bool = False
    session_completed: bool = False
    session_failed: bool = False
    tools_called: list[str] = field(default_factory=list)


async def reconstruct_agent_context(
    store,
    agent_id: str,
    session_id: str,
    agent_type: str = "unknown",
    max_context_tokens: int = 2000,
) -> AgentContext:
    """
    Rebuild the operational context for an agent session by replaying its
    event stream.

    Args:
        store:             EventStore or InMemoryEventStore.
        agent_id:          The agent's identifier.
        session_id:        The session to reconstruct.
        agent_type:        Agent type (used to build the stream id if unknown).
        max_context_tokens: Approximate word budget for the prose summary.

    Returns:
        AgentContext with enough information to resume or escalate.
    """
    stream_id = f"agent-{agent_id}-{session_id}"
    events = await store.load_stream(stream_id)

    ctx = AgentContext(
        session_id=session_id,
        agent_type=agent_type,
        agent_id=agent_id,
        application_id=None,
        model_version=None,
        context_text="",
        last_position=-1,
    )

    if not events:
        ctx.context_text = f"No events found for session {session_id}."
        return ctx

    ctx.last_position = events[-1]["stream_position"]

    # ── Classify events ───────────────────────────────────────────────────────
    summary_lines: list[str] = []
    in_progress_nodes: list[str] = []

    for event in events:
        et = event["event_type"]
        p = event.get("payload", {})

        if et == "AgentSessionStarted":
            ctx.session_started = True
            ctx.agent_type = p.get("agent_type", agent_type)
            ctx.agent_id = p.get("agent_id", agent_id)
            ctx.application_id = p.get("application_id")
            ctx.model_version = p.get("model_version")
            summary_lines.append(
                f"Session started for application {ctx.application_id} "
                f"using model {ctx.model_version}."
            )

        elif et in ("AgentInputValidated", "AgentContextLoaded"):
            summary_lines.append("Agent context loaded and inputs validated.")

        elif et == "AgentInputValidationFailed":
            ctx.pending_work.append(
                f"Input validation failed: {p.get('validation_errors', [])}"
            )
            summary_lines.append(
                f"Input validation failed — missing: {p.get('missing_inputs', [])}."
            )

        elif et == "AgentNodeExecuted":
            node = p.get("node_name", "unknown")
            ctx.nodes_completed.append(node)
            # Track nodes that look like decision starts without completion
            if "decision" in node.lower() or "generate" in node.lower():
                in_progress_nodes.append(node)
            summary_lines.append(f"Node '{node}' executed (seq {p.get('node_sequence', '?')}).")

        elif et == "AgentToolCalled":
            tool = p.get("tool_name", "unknown")
            ctx.tools_called.append(tool)
            summary_lines.append(f"Tool '{tool}' called.")

        elif et == "AgentOutputWritten":
            written = p.get("events_written", [])
            summary_lines.append(
                f"Output written to domain stream: {len(written)} event(s). "
                f"Summary: {p.get('output_summary', '')}."
            )
            # Output written means decision-like nodes are now complete
            in_progress_nodes.clear()

        elif et == "AgentSessionCompleted":
            ctx.session_completed = True
            in_progress_nodes.clear()
            summary_lines.append(
                f"Session completed. Nodes executed: {p.get('total_nodes_executed', '?')}, "
                f"LLM calls: {p.get('total_llm_calls', '?')}, "
                f"cost: ${p.get('total_cost_usd', '?')}."
            )

        elif et == "AgentSessionFailed":
            ctx.session_failed = True
            recoverable = p.get("recoverable", True)
            summary_lines.append(
                f"Session failed at node '{p.get('last_successful_node', 'unknown')}': "
                f"{p.get('error_message', '')} "
                f"({'recoverable' if recoverable else 'unrecoverable'})."
            )
            if recoverable:
                ctx.pending_work.append(
                    f"Resume from '{p.get('last_successful_node', 'unknown')}'"
                )

        elif et == "AgentSessionRecovered":
            ctx.session_failed = False
            summary_lines.append(
                f"Session recovered from {p.get('recovered_from_session_id', '?')} "
                f"at '{p.get('recovery_point', '?')}'."
            )

    # ── Determine health status ───────────────────────────────────────────────
    if ctx.session_completed:
        ctx.health_status = HEALTHY
    elif ctx.session_failed and not ctx.session_failed:
        ctx.health_status = FAILED
    elif ctx.session_started and not ctx.session_completed:
        ctx.health_status = NEEDS_RECONCILIATION
        if in_progress_nodes:
            ctx.pending_work.append(
                f"Decision in progress at node(s): {in_progress_nodes} — "
                "no corresponding completion event found."
            )
        elif ctx.nodes_completed:
            ctx.pending_work.append(
                f"Session interrupted after node '{ctx.nodes_completed[-1]}' — "
                "no AgentSessionCompleted received."
            )
        else:
            ctx.pending_work.append("Session started but no nodes executed.")
    else:
        ctx.health_status = NEEDS_RECONCILIATION

    # ── Build prose context (token-budget aware) ──────────────────────────────
    # Simple budget: truncate summary lines to approx max_context_tokens words
    words_used = 0
    kept_lines: list[str] = []
    for line in summary_lines:
        words = len(line.split())
        if words_used + words > max_context_tokens:
            kept_lines.append("[...earlier context truncated for token budget...]")
            break
        kept_lines.append(line)
        words_used += words

    ctx.context_text = "\n".join(kept_lines)
    return ctx
