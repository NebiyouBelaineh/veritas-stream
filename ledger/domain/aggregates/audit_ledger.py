"""
ledger/domain/aggregates/audit_ledger.py
=========================================
AuditLedgerAggregate — tracks cryptographic integrity check runs for one entity.

Stream: "audit-{entity_type}-{entity_id}"
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass
class AuditLedgerAggregate:
    entity_id: str
    entity_type: str | None = None
    last_integrity_hash: str | None = None
    events_verified: int = 0
    last_tamper_detected: bool = False
    last_chain_valid: bool = True
    version: int = -1

    @classmethod
    async def load(
        cls, store, entity_type: str, entity_id: str
    ) -> "AuditLedgerAggregate":
        """Load and replay the audit stream for an entity."""
        agg = cls(entity_id=entity_id, entity_type=entity_type)
        stream_events = await store.load_stream(f"audit-{entity_type}-{entity_id}")
        for event in stream_events:
            agg.apply(event)
        return agg

    def apply(self, event: dict) -> None:
        """Apply one stored event."""
        et = event.get("event_type")
        p = event.get("payload", {})
        self.version += 1

        if et == "AuditIntegrityCheckRun":
            self.entity_type = p.get("entity_type", self.entity_type)
            self.last_integrity_hash = p.get("integrity_hash")
            self.events_verified = p.get("events_verified_count", 0)
            self.last_tamper_detected = p.get("tamper_detected", False)
            self.last_chain_valid = p.get("chain_valid", True)
