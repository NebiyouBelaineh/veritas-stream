"""
ledger/projections/compliance_audit.py
========================================
ComplianceAuditViewProjection — full compliance record per application with
temporal query support.

`get_compliance_at(application_id, as_of)` replays stored compliance events
up to `as_of` and returns the compliance state as it existed at that moment.
This satisfies regulatory time-travel requirements: an auditor can ask
"what was the compliance status of application X at time T?"

Temporal ordering uses the payload's `evaluated_at` field (the time the
compliance agent evaluated the rule), not `recorded_at` (the time it was
persisted). `evaluated_at` is controlled by the command emitting the event
and is stable across replays.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone

from ledger.projections import Projection


@dataclass
class ComplianceEventRecord:
    event_type: str
    rule_id: str | None
    rule_name: str | None
    session_id: str | None
    passed: bool | None          # True=passed, False=failed, None=note/completed
    is_hard_block: bool
    overall_verdict: str | None  # set when event_type=ComplianceCheckCompleted
    evaluated_at: datetime


@dataclass
class ComplianceSnapshot:
    application_id: str
    rules_passed: set = field(default_factory=set)
    rules_failed: set = field(default_factory=set)
    hard_blocked: bool = False
    verdict: str | None = None


class ComplianceAuditViewProjection(Projection):
    """
    Append-only log of compliance events per application.
    Supports temporal queries via event replay up to a given timestamp.
    """

    name = "compliance_audit_view"

    def __init__(self) -> None:
        # application_id → list of ComplianceEventRecord (in evaluated_at order)
        self._log: dict[str, list[ComplianceEventRecord]] = {}

    # ── Projection interface ──────────────────────────────────────────────────

    async def handle(self, event: dict) -> None:
        et = event["event_type"]
        if et not in (
            "ComplianceCheckInitiated",
            "ComplianceRulePassed",
            "ComplianceRuleFailed",
            "ComplianceRuleNoted",
            "ComplianceCheckCompleted",
        ):
            return

        p = event.get("payload", {})
        app_id = p.get("application_id")
        if not app_id:
            return

        evaluated_at_raw = p.get("evaluated_at") or p.get("initiated_at") or p.get("completed_at")
        if evaluated_at_raw is None:
            evaluated_at_raw = str(event.get("recorded_at", datetime.now(timezone.utc).isoformat()))

        if isinstance(evaluated_at_raw, str):
            try:
                evaluated_at = datetime.fromisoformat(evaluated_at_raw)
            except ValueError:
                evaluated_at = datetime.now(timezone.utc)
        elif isinstance(evaluated_at_raw, datetime):
            evaluated_at = evaluated_at_raw
        else:
            evaluated_at = datetime.now(timezone.utc)

        # Normalise to UTC-aware
        if evaluated_at.tzinfo is None:
            evaluated_at = evaluated_at.replace(tzinfo=timezone.utc)

        record = ComplianceEventRecord(
            event_type=et,
            rule_id=p.get("rule_id"),
            rule_name=p.get("rule_name"),
            session_id=p.get("session_id"),
            passed=(True if et == "ComplianceRulePassed" else
                    False if et == "ComplianceRuleFailed" else None),
            is_hard_block=p.get("is_hard_block", False),
            overall_verdict=p.get("overall_verdict") if et == "ComplianceCheckCompleted" else None,
            evaluated_at=evaluated_at,
        )

        self._log.setdefault(app_id, []).append(record)
        # Keep log sorted by evaluation time for efficient temporal queries
        self._log[app_id].sort(key=lambda r: r.evaluated_at)

    async def truncate(self) -> None:
        self._log.clear()

    # ── Query interface ───────────────────────────────────────────────────────

    def get_compliance_at(
        self, application_id: str, as_of: datetime
    ) -> ComplianceSnapshot:
        """
        Replay compliance events for `application_id` up to `as_of` and
        return the state as it existed at that moment.

        Returns an empty snapshot if no events existed before `as_of`.
        """
        snapshot = ComplianceSnapshot(application_id=application_id)
        events = self._log.get(application_id, [])

        # Normalise as_of to UTC-aware for comparison
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)

        for record in events:
            if record.evaluated_at > as_of:
                break  # log is sorted; no need to continue

            if record.event_type == "ComplianceRulePassed" and record.rule_id:
                snapshot.rules_passed.add(record.rule_id)
                snapshot.rules_failed.discard(record.rule_id)

            elif record.event_type == "ComplianceRuleFailed" and record.rule_id:
                snapshot.rules_failed.add(record.rule_id)
                snapshot.rules_passed.discard(record.rule_id)
                if record.is_hard_block:
                    snapshot.hard_blocked = True

            elif record.event_type == "ComplianceCheckCompleted":
                snapshot.verdict = record.overall_verdict

        return snapshot

    def get_current(self, application_id: str) -> ComplianceSnapshot:
        """Return the latest compliance state (no time filter)."""
        far_future = datetime(9999, 12, 31, tzinfo=timezone.utc)
        return self.get_compliance_at(application_id, far_future)
