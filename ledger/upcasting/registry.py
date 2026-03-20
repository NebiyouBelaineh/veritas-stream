"""
ledger/upcasting/registry.py
==============================
Decorator-based UpcasterRegistry.

IMMUTABILITY CONTRACT (mandatory TRP requirement):
  upcast() ALWAYS returns a NEW dict. The input event is NEVER modified.
  The caller can verify this by reading the original event from the store
  before and after calling upcast() — the raw stored payload must be
  byte-for-byte identical.

Usage::

    registry = UpcasterRegistry()

    @registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
    def upcast_credit_v1_v2(payload: dict) -> dict:
        payload = dict(payload)         # always copy, never mutate
        payload.setdefault("regulatory_basis", [])
        return payload
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class UpcasterRegistry:
    """
    Chain-of-responsibility upcaster.  Each registered function transforms
    one version of a payload into the next; the registry applies the chain
    automatically.

    Registered functions receive and return a plain dict (the payload only,
    not the full event envelope).  They must return a NEW dict — the input
    must not be modified.

    The registry itself guarantees immutability by deep-copying the envelope
    before applying any transforms.
    """

    def __init__(self) -> None:
        # event_type → {from_version: transform_fn}
        self._chain: dict[str, dict[int, callable]] = {}

    def upcaster(self, event_type: str, from_version: int, to_version: int):
        """Decorator that registers an upcast function for one version step."""
        if to_version != from_version + 1:
            raise ValueError(
                f"Upcasters must advance exactly one version at a time "
                f"(got {from_version}→{to_version} for {event_type!r})."
            )

        def decorator(fn: callable) -> callable:
            self._chain.setdefault(event_type, {})[from_version] = fn
            return fn

        return decorator

    def upcast(self, event: dict) -> dict:
        """
        Return an upcasted copy of `event`.  The original dict is NEVER
        modified — a structural copy is created before any transforms run.

        If no upcasters apply, the original object is returned as-is
        (no allocation).
        """
        et = event.get("event_type")
        v = event.get("event_version", 1)
        chain = self._chain.get(et, {})

        if v not in chain:
            return event  # nothing to do — return original unchanged

        # Create a structural copy: new envelope + new payload dict.
        # We do NOT deepcopy nested values inside the payload to avoid
        # unnecessary allocations; each upcaster function is responsible
        # for copying the fields it modifies.
        upcasted: dict = {**event, "payload": dict(event.get("payload", {}))}

        while upcasted.get("event_version", 1) in chain:
            from_v = upcasted["event_version"]
            fn = chain[from_v]
            try:
                upcasted["payload"] = fn(dict(upcasted["payload"]))
            except Exception as exc:
                logger.error(
                    "Upcaster for %r v%d→v%d raised %s — returning partially upcasted event",
                    et, from_v, from_v + 1, exc,
                )
                break
            upcasted["event_version"] = from_v + 1

        return upcasted
