"""
ledger/domain/aggregates/compliance_record.py
==============================================
ComplianceRecordAggregate — tracks rule evaluations for one application.

Business rules enforced here:
  5. assert_all_checks_complete(required) — all required rule IDs must have
     a PASS or FAIL result before a verdict can be issued.
"""
from __future__ import annotations
from dataclasses import dataclass, field

from ledger.domain.aggregates.loan_application import DomainError


@dataclass
class ComplianceRecordAggregate:
    application_id: str
    rules_passed: set = field(default_factory=set)   # rule_ids
    rules_failed: set = field(default_factory=set)   # rule_ids
    hard_blocked: bool = False
    verdict: str | None = None                       # "CLEAR" | "BLOCKED" | "CONDITIONAL"
    version: int = -1

    @classmethod
    async def load(cls, store, application_id: str) -> "ComplianceRecordAggregate":
        """Load and replay the compliance stream."""
        agg = cls(application_id=application_id)
        stream_events = await store.load_stream(f"compliance-{application_id}")
        for event in stream_events:
            agg.apply(event)
        return agg

    def apply(self, event: dict) -> None:
        """Apply one stored event."""
        et = event.get("event_type")
        p = event.get("payload", {})
        self.version = event.get("stream_position", self.version + 1)

        if et == "ComplianceCheckInitiated":
            pass  # No state change needed — rules_passed/failed start empty

        elif et == "ComplianceRulePassed":
            rule_id = p.get("rule_id")
            if rule_id:
                self.rules_passed.add(rule_id)
                self.rules_failed.discard(rule_id)  # supersede any prior failure

        elif et == "ComplianceRuleFailed":
            rule_id = p.get("rule_id")
            if rule_id:
                self.rules_failed.add(rule_id)
                self.rules_passed.discard(rule_id)
            if p.get("is_hard_block", False):
                self.hard_blocked = True

        elif et == "ComplianceRuleNoted":
            pass  # Notes do not affect pass/fail sets

        elif et == "ComplianceCheckCompleted":
            self.verdict = p.get("overall_verdict")

    # ── Business rule assertions ──────────────────────────────────────────────

    def assert_all_checks_complete(self, required_rule_ids: set) -> None:
        """Rule 5: All required rules must have been evaluated (passed or failed)."""
        evaluated = self.rules_passed | self.rules_failed
        missing = required_rule_ids - evaluated
        if missing:
            raise DomainError(
                f"Cannot issue compliance verdict: rules not yet evaluated: {missing}. "
                "All required compliance checks must complete before a verdict."
            )
