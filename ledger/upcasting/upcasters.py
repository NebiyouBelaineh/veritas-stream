"""
ledger/upcasting/upcasters.py
===============================
Registered upcasters for all versioned events.

Inference strategy (documented requirement):
  model_version  — inferred from recorded_at using a version-date table;
                   fallback: "legacy-pre-2026"
  confidence_score — set to None; we do NOT fabricate unknown values.
                   Presence of the field (even as None) signals v2 format.
  model_versions — empty dict {}; presence signals v2 DecisionGenerated.

Import `create_registry()` to get a fully wired UpcasterRegistry.
"""
from __future__ import annotations

from datetime import datetime, timezone

from ledger.upcasting.registry import UpcasterRegistry


# ── Model-version inference ────────────────────────────────────────────────────
#
# Maps deployment dates to the model version that was live at that time.
# Entries are (cutoff_datetime, model_version); evaluated newest-first.
# If recorded_at pre-dates all entries, returns the fallback string.

_MODEL_VERSION_TIMELINE: list[tuple[datetime, str]] = [
    (datetime(2026, 3, 1, tzinfo=timezone.utc), "v2.4"),
    (datetime(2026, 1, 1, tzinfo=timezone.utc), "v2.3"),
    (datetime(2025, 7, 1, tzinfo=timezone.utc), "v2.2"),
    (datetime(2025, 1, 1, tzinfo=timezone.utc), "v2.1"),
]
_LEGACY_MODEL_VERSION = "legacy-pre-2026"


def _infer_model_version(recorded_at: str | datetime | None) -> str:
    """Return the model version that was live when `recorded_at` occurred."""
    if recorded_at is None:
        return _LEGACY_MODEL_VERSION

    if isinstance(recorded_at, str):
        try:
            recorded_at = datetime.fromisoformat(recorded_at)
        except ValueError:
            return _LEGACY_MODEL_VERSION

    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)

    for cutoff, version in _MODEL_VERSION_TIMELINE:
        if recorded_at >= cutoff:
            return version

    return _LEGACY_MODEL_VERSION


# ── Registry factory ──────────────────────────────────────────────────────────

def create_registry() -> UpcasterRegistry:
    """Return a fully wired UpcasterRegistry with all production upcasters."""
    registry = UpcasterRegistry()

    # ── CreditAnalysisCompleted v1 → v2 ──────────────────────────────────────
    # v2 adds:
    #   regulatory_basis: list[str]  — empty list if unknown
    #   confidence_score: float|None — None (do not fabricate unknowns)
    #   model_version: str           — inferred from recorded_at
    @registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
    def _credit_v1_v2(payload: dict) -> dict:
        payload = dict(payload)
        payload.setdefault("regulatory_basis", [])
        payload.setdefault("confidence_score", None)  # never fabricate
        if not payload.get("model_version"):
            payload["model_version"] = _infer_model_version(
                payload.get("recorded_at")
            )
        return payload

    # ── DecisionGenerated v1 → v2 ────────────────────────────────────────────
    # v2 adds:
    #   model_versions: dict[str, str]  — empty dict signals v2 envelope
    @registry.upcaster("DecisionGenerated", from_version=1, to_version=2)
    def _decision_v1_v2(payload: dict) -> dict:
        payload = dict(payload)
        payload.setdefault("model_versions", {})
        return payload

    return registry


# ── Module-level singleton (convenience) ─────────────────────────────────────
#
# Import this in EventStore/InMemoryEventStore constructors:
#   from ledger.upcasting.upcasters import default_registry
#   store = EventStore(db_url, upcaster_registry=default_registry)

default_registry: UpcasterRegistry = create_registry()
