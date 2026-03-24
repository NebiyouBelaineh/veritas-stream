"""
ledger/upcasters.py — UpcasterRegistry
=======================================
Upcasters transform old event versions to the current version ON READ.
They NEVER write to the events table. Immutability is non-negotiable.

RULE: if event_version == current version, return unchanged.
      if event_version < current version, apply the chain of upcasters.

CreditAnalysisCompleted v1 -> v2 adds three fields:
  regulatory_basis   -> [] (empty list; pre-2026 events predate EU AI Act citations)
  confidence_score   -> None (not fabricated; a guessed value would incorrectly
                        trigger the confidence < 0.60 REFER constraint on historical
                        decisions where the score was genuinely unknown)
  model_version      -> inferred from recorded_at vs deployment timeline (~5% error
                        rate near deployment boundaries; consequence is cosmetic display
                        only, not used in decisioning)

DecisionGenerated v1 -> v2 adds:
  model_versions     -> {} (empty dict; pre-v2 events did not record contributing models)
"""
from __future__ import annotations

from datetime import datetime, timezone


# Deployment timeline for model_version inference.
# Each tuple is (go_live_datetime, version_string).
# Events recorded on or after go_live_datetime used that version.
# ~5% error rate for events within 48 hours of a boundary.
_MODEL_VERSION_TIMELINE = [
    (datetime(2026, 3, 1, tzinfo=timezone.utc), "v2.4"),
    (datetime(2026, 1, 1, tzinfo=timezone.utc), "v2.3"),
    (datetime(2025, 7, 1, tzinfo=timezone.utc), "v2.2"),
    (datetime(2025, 1, 1, tzinfo=timezone.utc), "v2.1"),
]


def _infer_model_version(recorded_at_raw) -> str:
    """
    Infer model_version from recorded_at timestamp.

    recorded_at is set by the store at append time and cannot be spoofed
    by the event payload, making it a reliable proxy for which model was
    live at that moment.

    Returns "v2.1" as the floor for any event predating the timeline or
    when recorded_at is missing or unparseable.
    """
    if recorded_at_raw is None:
        return "v2.1"
    try:
        ts = datetime.fromisoformat(str(recorded_at_raw))
    except (ValueError, TypeError):
        return "v2.1"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    for boundary, version in _MODEL_VERSION_TIMELINE:
        if ts >= boundary:
            return version
    return "v2.1"


class UpcasterRegistry:
    """Apply on load_stream() — never on append()."""

    def upcast(self, event: dict) -> dict:
        et = event.get("event_type")
        ver = event.get("event_version", 1)

        if et == "CreditAnalysisCompleted" and ver < 2:
            event = dict(event)
            event["event_version"] = 2
            p = dict(event.get("payload", {}))
            p.setdefault("regulatory_basis", [])
            # confidence_score: None — not fabricated. A guessed value would
            # incorrectly trigger the confidence < 0.60 -> REFER constraint.
            p.setdefault("confidence_score", None)
            # model_version: inferred from recorded_at; ~5% error near boundaries;
            # consequence is cosmetic (display label only, not used in decisioning).
            p.setdefault(
                "model_version",
                _infer_model_version(event.get("recorded_at")),
            )
            event["payload"] = p

        if et == "DecisionGenerated" and ver < 2:
            event = dict(event)
            event["event_version"] = 2
            p = dict(event.get("payload", {}))
            p.setdefault("model_versions", {})
            event["payload"] = p

        return event
