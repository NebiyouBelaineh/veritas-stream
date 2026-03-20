"""
ledger/domain/aggregates/agent_session.py
==========================================
AgentSessionAggregate — tracks one agent execution session.

Business rules enforced here:
  2. Gas Town: assert_context_loaded() before any decision event
  3. Model version locking: assert_model_version_current() before CreditAnalysisCompleted
"""
from __future__ import annotations
from dataclasses import dataclass, field

from ledger.domain.aggregates.loan_application import DomainError


@dataclass
class AgentSessionAggregate:
    agent_id: str
    session_id: str
    agent_type: str | None = None
    application_id: str | None = None
    context_loaded: bool = False
    model_version: str | None = None
    completed_nodes: list = field(default_factory=list)
    is_completed: bool = False
    is_failed: bool = False
    version: int = -1

    @classmethod
    async def load(
        cls, store, agent_id: str, session_id: str
    ) -> "AgentSessionAggregate":
        """Load and replay the agent session stream."""
        agg = cls(agent_id=agent_id, session_id=session_id)
        # Agent session stream: "agent-{agent_type}-{session_id}"
        # We load by session_id since agent_type may not be known at load time;
        # command handlers pass agent_type explicitly when constructing the stream id.
        stream_events = await store.load_stream(f"agent-{agent_id}-{session_id}")
        for event in stream_events:
            agg.apply(event)
        return agg

    @classmethod
    async def load_by_stream(
        cls, store, stream_id: str, agent_id: str, session_id: str
    ) -> "AgentSessionAggregate":
        """Load from an explicit stream id (e.g. agent-credit_analysis-{session_id})."""
        agg = cls(agent_id=agent_id, session_id=session_id)
        stream_events = await store.load_stream(stream_id)
        for event in stream_events:
            agg.apply(event)
        return agg

    def apply(self, event: dict) -> None:
        """Apply one stored event."""
        et = event.get("event_type")
        p = event.get("payload", {})
        self.version += 1

        if et == "AgentSessionStarted":
            self.agent_type = p.get("agent_type")
            self.application_id = p.get("application_id")
            self.model_version = p.get("model_version")

        elif et == "AgentInputValidated":
            # Context is considered loaded once inputs are validated
            self.context_loaded = True

        elif et == "AgentContextLoaded":
            # Explicit context-loaded event (Gas Town pattern)
            self.context_loaded = True

        elif et == "AgentNodeExecuted":
            node_name = p.get("node_name")
            if node_name:
                self.completed_nodes.append(node_name)

        elif et == "AgentSessionCompleted":
            self.is_completed = True

        elif et == "AgentSessionFailed":
            self.is_failed = True

        elif et == "AgentSessionRecovered":
            self.is_failed = False

    # ── Business rule assertions ──────────────────────────────────────────────

    def assert_context_loaded(self) -> None:
        """Rule 2 (Gas Town): An agent must have loaded context before any decision."""
        if not self.context_loaded:
            raise DomainError(
                f"AgentSession {self.session_id} has not loaded context. "
                "An AgentContextLoaded (or AgentInputValidated) event must precede "
                "any decision event — this is the Gas Town pattern."
            )

    def assert_model_version_current(self, expected_version: str) -> None:
        """Rule 3: Model version must match what was declared at session start."""
        if self.model_version != expected_version:
            raise DomainError(
                f"Model version mismatch for session {self.session_id}: "
                f"session declared {self.model_version!r}, "
                f"CreditAnalysisCompleted carries {expected_version!r}. "
                "No further analysis events may be appended once the version diverges."
            )
