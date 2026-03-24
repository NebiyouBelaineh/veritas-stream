"""
what_if/projector.py
====================
Counterfactual projection: replay a real event stream but substitute a
different event at a branch point, then compare projection outcomes.

NEVER writes to the store. All projection instances are created fresh and
discarded after the function returns. The store is accessed read-only.

Usage example:

    result = await run_what_if(
        store=store,
        application_id="APEX-0007",
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[
            {"event_type": "CreditAnalysisCompleted", "event_version": 1,
             "payload": {..., "decision": {"risk_tier": "HIGH"}}},
        ],
        projections=[summary_proj, compliance_proj],
    )
    print(result.real_outcome)            # projection state under real events
    print(result.counterfactual_outcome)  # projection state under counterfactual
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any


@dataclass
class WhatIfResult:
    """
    Outcome of a counterfactual projection run.

    real_outcome and counterfactual_outcome are dicts keyed by projection name.
    Each value is the serialised state of that projection after replay.
    """
    application_id: str
    branch_at_event_type: str
    branch_position: int              # stream_position of the branched event (-1 if not found)
    real_outcome: dict[str, Any]
    counterfactual_outcome: dict[str, Any]
    events_before_branch: int
    counterfactual_events_injected: int
    real_events_suppressed: int       # causally dependent real events skipped in counterfactual path
    divergence_summary: list[str] = field(default_factory=list)


def _causation_id(event: dict) -> str | None:
    return event.get("metadata", {}).get("causation_id")


def _event_id(event: dict) -> str | None:
    # asyncpg returns event_id as a UUID object; causation_id stored in JSONB
    # metadata is a plain string. Normalise to str so set membership works.
    eid = event.get("event_id")
    return str(eid) if eid is not None else None


def _is_causally_dependent(event: dict, suppressed_ids: set[str]) -> bool:
    """
    Return True if the event's causation_id traces back to a suppressed event.
    Only one level of causation is checked here; this is sufficient for the
    single-branch scenarios the what-if projector supports.
    """
    cid = _causation_id(event)
    return cid in suppressed_ids if cid else False


def _projection_snapshot(projections: list) -> dict[str, Any]:
    """
    Serialise the current in-memory state of each projection.
    Uses each projection's internal _rows or _log dict if present,
    falling back to a repr for projections without a standard state attr.
    """
    snapshot = {}
    for proj in projections:
        name = getattr(proj, "name", type(proj).__name__)
        if hasattr(proj, "_rows"):
            # ApplicationSummaryProjection
            snapshot[name] = {
                k: vars(v) for k, v in proj._rows.items()
            }
        elif hasattr(proj, "_log"):
            # ComplianceAuditViewProjection
            snapshot[name] = {
                k: [vars(r) for r in records]
                for k, records in proj._log.items()
            }
        else:
            snapshot[name] = repr(proj)
    return snapshot


def _build_divergence_summary(
    real: dict[str, Any],
    counterfactual: dict[str, Any],
) -> list[str]:
    """Produce a human-readable list of fields that differ between outcomes."""
    diffs = []
    for proj_name in set(real) | set(counterfactual):
        real_proj = real.get(proj_name, {})
        cf_proj = counterfactual.get(proj_name, {})
        for app_id in set(real_proj) | set(cf_proj):
            real_row = real_proj.get(app_id, {})
            cf_row = cf_proj.get(app_id, {})
            for key in set(real_row) | set(cf_row):
                rv = real_row.get(key)
                cv = cf_row.get(key)
                if rv != cv:
                    diffs.append(
                        f"{proj_name}.{app_id}.{key}: real={rv!r} vs counterfactual={cv!r}"
                    )
    return diffs


async def run_what_if(
    store,
    application_id: str,
    branch_at_event_type: str,
    counterfactual_events: list[dict],
    projections: list,
) -> WhatIfResult:
    """
    Replay the event stream for `application_id`, branching at the first
    occurrence of `branch_at_event_type`.

    Real path: all events including the real branch event and subsequent events.
    Counterfactual path: events before the branch, then `counterfactual_events`,
    then real events that are causally independent of the branched event.

    Precondition: `projections` are fresh instances (not shared with live state).
    Guarantee: the store is never written to. All state is discarded after return.

    Args:
        store: any object with an async load_stream(stream_id) method.
        application_id: the application whose stream is branched.
        branch_at_event_type: the event type at which to diverge.
        counterfactual_events: events to inject instead of the real branch event.
        projections: projection instances to evaluate under both scenarios.

    Returns:
        WhatIfResult with real_outcome and counterfactual_outcome dicts.
    """
    stream_id = f"loan-{application_id}"
    all_events: list[dict] = await store.load_stream(stream_id)

    # Find the branch point
    branch_index = -1
    branch_position = -1
    for i, ev in enumerate(all_events):
        if ev.get("event_type") == branch_at_event_type:
            branch_index = i
            branch_position = ev.get("stream_position", i)
            break

    events_before = all_events[:branch_index] if branch_index >= 0 else all_events
    events_after = all_events[branch_index + 1:] if branch_index >= 0 else []

    # Collect IDs of events being suppressed so we can skip their causal dependents
    suppressed_ids: set[str] = set()
    if branch_index >= 0:
        eid = _event_id(all_events[branch_index])
        if eid:
            suppressed_ids.add(eid)

    # Filter post-branch real events: skip those causally dependent on the branch
    independent_real_events = []
    for ev in events_after:
        if _is_causally_dependent(ev, suppressed_ids):
            # Also suppress this event's ID so transitive dependents are caught
            eid = _event_id(ev)
            if eid:
                suppressed_ids.add(eid)
        else:
            independent_real_events.append(ev)

    real_events_suppressed = len(events_after) - len(independent_real_events)

    # Build two sets of fresh projection copies: one per scenario
    real_projections = [copy.deepcopy(p) for p in projections]
    cf_projections = [copy.deepcopy(p) for p in projections]

    # Real path: events_before + real branch event + all post-branch real events
    real_sequence = events_before[:]
    if branch_index >= 0:
        real_sequence.append(all_events[branch_index])
    real_sequence.extend(events_after)

    for ev in real_sequence:
        for proj in real_projections:
            await proj.handle(ev)

    # Counterfactual path: events_before + injected events + independent real events
    cf_sequence = events_before[:]
    cf_sequence.extend(counterfactual_events)
    cf_sequence.extend(independent_real_events)

    for ev in cf_sequence:
        for proj in cf_projections:
            await proj.handle(ev)

    real_outcome = _projection_snapshot(real_projections)
    cf_outcome = _projection_snapshot(cf_projections)

    return WhatIfResult(
        application_id=application_id,
        branch_at_event_type=branch_at_event_type,
        branch_position=branch_position,
        real_outcome=real_outcome,
        counterfactual_outcome=cf_outcome,
        events_before_branch=len(events_before),
        counterfactual_events_injected=len(counterfactual_events),
        real_events_suppressed=real_events_suppressed,
        divergence_summary=_build_divergence_summary(real_outcome, cf_outcome),
    )
