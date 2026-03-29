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

# Increment this when the projection's apply() logic changes in a way that
# would produce different output from the same input events. Snapshots with
# a lower version are stale and must be ignored during temporal queries.
CURRENT_SNAPSHOT_VERSION = 1

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

    async def handle(self, event: dict, conn=None) -> None:
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

        if conn is not None:
            await self._persist(app_id, record, event, conn)

    async def _persist(self, app_id: str, record: "ComplianceEventRecord", event: dict, conn) -> None:
        """Write compliance event to DB. Idempotent per (application_id, rule_id)."""
        import json as _json

        recorded_at_raw = event.get("recorded_at")
        if isinstance(recorded_at_raw, str):
            try:
                recorded_at = datetime.fromisoformat(recorded_at_raw)
            except (ValueError, TypeError):
                recorded_at = datetime.now(timezone.utc)
        elif isinstance(recorded_at_raw, datetime):
            recorded_at = recorded_at_raw
        else:
            recorded_at = datetime.now(timezone.utc)

        if record.event_type in ("ComplianceRulePassed", "ComplianceRuleFailed") and record.rule_id:
            verdict = "PASSED" if record.passed else "FAILED"
            p = event.get("payload", {})
            await conn.execute(
                """INSERT INTO compliance_audit_view (
                       application_id, rule_id, rule_version, verdict,
                       failure_reason, regulation_set, evaluated_at,
                       evidence_hash, recorded_at
                   ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                   ON CONFLICT (application_id, rule_id) DO UPDATE SET
                       verdict        = EXCLUDED.verdict,
                       failure_reason = EXCLUDED.failure_reason,
                       evaluated_at   = EXCLUDED.evaluated_at,
                       recorded_at    = EXCLUDED.recorded_at""",
                app_id,
                record.rule_id,
                p.get("rule_version"),
                verdict,
                p.get("failure_reason"),
                p.get("regulation_set"),
                record.evaluated_at,
                p.get("evidence_hash"),
                recorded_at,
            )

        elif record.event_type == "ComplianceCheckCompleted":
            # Save a full snapshot of current state for time-travel queries.
            snapshot = self.get_current(app_id)
            gpos = event.get("global_position", 0)
            state_json = {
                "rules_passed": list(snapshot.rules_passed),
                "rules_failed": list(snapshot.rules_failed),
                "hard_blocked": snapshot.hard_blocked,
                "verdict": snapshot.verdict,
            }
            await conn.execute(
                """INSERT INTO compliance_audit_snapshots
                       (application_id, snapshot_at, global_position, state_json,
                        snapshot_version)
                   VALUES ($1,$2,$3,$4,$5)
                   ON CONFLICT (application_id, snapshot_at) DO NOTHING""",
                app_id, record.evaluated_at, gpos, _json.dumps(state_json),
                CURRENT_SNAPSHOT_VERSION,
            )

    async def warm_load(self, pool) -> None:
        """
        Rebuild self._log from the DB so compliance history is available
        immediately after server restart without replaying all past events.

        Reads per-rule verdicts from compliance_audit_view, then pins the
        overall verdict using the latest snapshot per application.
        """
        async with pool.acquire() as conn:
            rule_rows = await conn.fetch(
                """SELECT application_id, rule_id, verdict, evaluated_at
                   FROM compliance_audit_view
                   ORDER BY application_id, evaluated_at"""
            )
            for r in rule_rows:
                app_id = r["application_id"]
                et = "ComplianceRulePassed" if r["verdict"] == "PASSED" else "ComplianceRuleFailed"
                evaluated_at = r["evaluated_at"]
                if evaluated_at.tzinfo is None:
                    evaluated_at = evaluated_at.replace(tzinfo=timezone.utc)
                record = ComplianceEventRecord(
                    event_type=et,
                    rule_id=r["rule_id"],
                    rule_name=None,
                    session_id=None,
                    passed=(r["verdict"] == "PASSED"),
                    is_hard_block=False,
                    overall_verdict=None,
                    evaluated_at=evaluated_at,
                )
                self._log.setdefault(app_id, []).append(record)

            snapshot_rows = await conn.fetch(
                """SELECT DISTINCT ON (application_id)
                       application_id, snapshot_at, state_json
                   FROM compliance_audit_snapshots
                   WHERE snapshot_version = $1
                   ORDER BY application_id, snapshot_at DESC""",
                CURRENT_SNAPSHOT_VERSION,
            )
            for r in snapshot_rows:
                import json as _json
                app_id = r["application_id"]
                state = r["state_json"]
                if isinstance(state, str):
                    state = _json.loads(state)
                snapshot_at = r["snapshot_at"]
                if snapshot_at.tzinfo is None:
                    snapshot_at = snapshot_at.replace(tzinfo=timezone.utc)
                record = ComplianceEventRecord(
                    event_type="ComplianceCheckCompleted",
                    rule_id=None,
                    rule_name=None,
                    session_id=None,
                    passed=None,
                    is_hard_block=False,
                    overall_verdict=state.get("verdict"),
                    evaluated_at=snapshot_at,
                )
                self._log.setdefault(app_id, []).append(record)

        for app_id in self._log:
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
