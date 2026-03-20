"""
ledger/projections/agent_performance.py
=========================================
AgentPerformanceLedgerProjection — tracks per-(agent_id, model_version)
statistics across all sessions.

A separate row exists for each (agent_id, model_version) pair. This lets
operators ask "did model v2.3 behave differently than v2.2?"
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime

from ledger.projections import Projection


@dataclass
class AgentPerformanceRow:
    agent_id: str
    model_version: str
    analyses_completed: int = 0
    decisions_generated: int = 0
    _confidence_sum: float = field(default=0.0, repr=False)
    _confidence_count: int = field(default=0, repr=False)
    _duration_sum_ms: int = field(default=0, repr=False)
    _duration_count: int = field(default=0, repr=False)
    approve_count: int = 0
    decline_count: int = 0
    refer_count: int = 0
    human_override_count: int = 0
    review_count: int = 0      # sessions that went to human review
    first_seen_at: str | None = None
    last_seen_at: str | None = None

    @property
    def avg_confidence_score(self) -> float | None:
        if self._confidence_count == 0:
            return None
        return self._confidence_sum / self._confidence_count

    @property
    def avg_duration_ms(self) -> float | None:
        if self._duration_count == 0:
            return None
        return self._duration_sum_ms / self._duration_count

    @property
    def approve_rate(self) -> float | None:
        total = self.analyses_completed
        return self.approve_count / total if total else None

    @property
    def decline_rate(self) -> float | None:
        total = self.analyses_completed
        return self.decline_count / total if total else None

    @property
    def refer_rate(self) -> float | None:
        total = self.analyses_completed
        return self.refer_count / total if total else None

    @property
    def human_override_rate(self) -> float | None:
        if self.review_count == 0:
            return None
        return self.human_override_count / self.review_count


class AgentPerformanceLedgerProjection(Projection):
    """
    Read model for aggregate agent performance statistics, partitioned by
    (agent_id, model_version) so version regressions are detectable.
    """

    name = "agent_performance_ledger"

    def __init__(self) -> None:
        # (agent_id, model_version) → row
        self._rows: dict[tuple[str, str], AgentPerformanceRow] = {}
        # session_id → (agent_id, model_version) for join
        self._sessions: dict[str, tuple[str, str]] = {}

    # ── Projection interface ──────────────────────────────────────────────────

    async def handle(self, event: dict) -> None:
        et = event["event_type"]
        p = event.get("payload", {})
        recorded_at = str(event.get("recorded_at", ""))

        if et == "AgentSessionStarted":
            agent_id = p.get("agent_id", "")
            model_version = p.get("model_version", "")
            session_id = p.get("session_id", "")
            if not agent_id or not model_version or not session_id:
                return

            key = (agent_id, model_version)
            self._sessions[session_id] = key

            row = self._rows.get(key)
            if row is None:
                row = AgentPerformanceRow(
                    agent_id=agent_id,
                    model_version=model_version,
                    first_seen_at=recorded_at,
                )
                self._rows[key] = row
            row.last_seen_at = recorded_at

        elif et == "CreditAnalysisCompleted":
            session_id = p.get("session_id", "")
            key = self._sessions.get(session_id)
            if key is None:
                return
            row = self._rows.get(key)
            if row is None:
                return

            row.analyses_completed += 1
            decision = p.get("decision") or {}
            conf = decision.get("confidence")
            if conf is not None:
                row._confidence_sum += float(conf)
                row._confidence_count += 1
            row.last_seen_at = recorded_at

        elif et == "AgentSessionCompleted":
            session_id = p.get("session_id", "")
            key = self._sessions.get(session_id)
            if key is None:
                return
            row = self._rows.get(key)
            if row is None:
                return

            duration = p.get("total_duration_ms")
            if duration is not None:
                row._duration_sum_ms += int(duration)
                row._duration_count += 1
            row.last_seen_at = recorded_at

        elif et == "DecisionGenerated":
            # Track decisions per orchestrator session
            orch_session_id = p.get("orchestrator_session_id", "")
            key = self._sessions.get(orch_session_id)
            if key is None:
                return
            row = self._rows.get(key)
            if row is None:
                return

            row.decisions_generated += 1
            recommendation = p.get("recommendation", "")
            if recommendation == "APPROVE":
                row.approve_count += 1
            elif recommendation == "DECLINE":
                row.decline_count += 1
            elif recommendation in ("REFER", "MANUAL_REVIEW"):
                row.refer_count += 1
                row.review_count += 1

        elif et == "HumanReviewCompleted":
            # Count overrides; attribute to the agent that produced the original decision
            # We don't know the session here directly, so we scan sessions for this app
            if p.get("override", False):
                app_id = p.get("application_id", "")
                for sid, key in self._sessions.items():
                    # All sessions for this application that are credit analysis agents
                    row = self._rows.get(key)
                    if row is None:
                        continue
                    # Only attribute to the agent that contributed (simplified: attribute once)
                    row.human_override_count += 1
                    break  # attribute to first matching agent

    async def truncate(self) -> None:
        self._rows.clear()
        self._sessions.clear()

    # ── Query interface ───────────────────────────────────────────────────────

    def get(self, agent_id: str, model_version: str) -> AgentPerformanceRow | None:
        return self._rows.get((agent_id, model_version))

    def all(self) -> list[AgentPerformanceRow]:
        return list(self._rows.values())
