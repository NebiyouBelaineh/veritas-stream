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

    async def handle(self, event: dict, conn=None) -> None:
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

        if conn is not None:
            # Upsert any rows that may have been updated by this event.
            # Check which key was touched (AgentSessionStarted, CreditAnalysisCompleted,
            # AgentSessionCompleted, DecisionGenerated each update one row).
            key_touched = None
            if et == "AgentSessionStarted":
                agent_id = p.get("agent_id", "")
                model_version = p.get("model_version", "")
                if agent_id and model_version:
                    key_touched = (agent_id, model_version)
            elif et in ("CreditAnalysisCompleted", "AgentSessionCompleted", "DecisionGenerated"):
                session_id = p.get("session_id") or p.get("orchestrator_session_id", "")
                key_touched = self._sessions.get(session_id)
            elif et == "HumanReviewCompleted" and p.get("override", False):
                # Attribute to first matched session key (same logic as above)
                for sid, k in self._sessions.items():
                    key_touched = k
                    break

            if key_touched:
                row = self._rows.get(key_touched)
                if row:
                    await self._upsert(row, conn)

    async def _upsert(self, row: "AgentPerformanceRow", conn) -> None:
        """Idempotent upsert to agent_performance_ledger."""
        import decimal

        def _d(v):
            if v is None:
                return None
            try:
                return decimal.Decimal(str(round(v, 6)))
            except Exception:
                return None

        from datetime import datetime

        def _ts(v):
            if v is None:
                return None
            if isinstance(v, datetime):
                return v
            try:
                return datetime.fromisoformat(str(v))
            except (ValueError, TypeError):
                return None

        await conn.execute(
            """INSERT INTO agent_performance_ledger (
                   agent_id, model_version,
                   analyses_completed, decisions_generated,
                   avg_confidence_score, avg_duration_ms,
                   approve_rate, decline_rate, refer_rate, human_override_rate,
                   first_seen_at, last_seen_at
               ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
               ON CONFLICT (agent_id, model_version) DO UPDATE SET
                   analyses_completed   = EXCLUDED.analyses_completed,
                   decisions_generated  = EXCLUDED.decisions_generated,
                   avg_confidence_score = EXCLUDED.avg_confidence_score,
                   avg_duration_ms      = EXCLUDED.avg_duration_ms,
                   approve_rate         = EXCLUDED.approve_rate,
                   decline_rate         = EXCLUDED.decline_rate,
                   refer_rate           = EXCLUDED.refer_rate,
                   human_override_rate  = EXCLUDED.human_override_rate,
                   last_seen_at         = EXCLUDED.last_seen_at""",
            row.agent_id, row.model_version,
            row.analyses_completed, row.decisions_generated,
            _d(row.avg_confidence_score), _d(row.avg_duration_ms),
            _d(row.approve_rate), _d(row.decline_rate),
            _d(row.refer_rate), _d(row.human_override_rate),
            _ts(row.first_seen_at), _ts(row.last_seen_at),
        )

    async def truncate(self) -> None:
        self._rows.clear()
        self._sessions.clear()

    # ── Query interface ───────────────────────────────────────────────────────

    def get(self, agent_id: str, model_version: str) -> AgentPerformanceRow | None:
        return self._rows.get((agent_id, model_version))

    def all(self) -> list[AgentPerformanceRow]:
        return list(self._rows.values())
