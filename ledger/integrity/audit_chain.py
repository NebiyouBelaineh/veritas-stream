"""
ledger/integrity/audit_chain.py
=================================
Cryptographic audit hash chain for event stream integrity verification.

Algorithm:
  1. Load all events from the target stream.
  2. For each event, compute sha256(canonical_json(payload)).
  3. Compute a chain hash:
       chain_hash = sha256(prev_chain_hash || event_hash_0 || ... || event_hash_N)
     where prev_chain_hash is the integrity_hash stored in the most recent
     AuditIntegrityCheckRun for this entity (or empty string if none).
  4. Compare computed chain_hash with the stored one to detect tampering:
       - If no prior check exists: chain_valid=True, tamper_detected=False.
       - If prior check exists and hashes match: chain_valid=True, tamper_detected=False.
       - If prior check exists and hashes differ: chain_valid=False, tamper_detected=True.
  5. Append AuditIntegrityCheckRun to the audit stream.

Tamper detection works because the computed hash of the *current* stream
contents is compared against the hash recorded during the *previous* check.
Any modification to an event payload will change the hash and break the chain.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4


def _sha256(data: str) -> str:
    return hashlib.sha256(data.encode()).hexdigest()


def _canonical(payload: dict) -> str:
    """Deterministic JSON for hashing — sorted keys, no extra whitespace."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


async def run_integrity_check(
    store,
    stream_id: str,
    entity_type: str | None = None,
    entity_id: str | None = None,
) -> dict:
    """
    Verify the integrity of `stream_id` and record the result.

    Derives `entity_type` / `entity_id` from `stream_id` if not provided
    (splits on the first '-': "loan-APEX-001" → entity_type="loan",
    entity_id="APEX-001").

    Returns a dict with keys:
      stream_id, entity_type, entity_id, events_verified,
      integrity_hash, previous_hash, chain_valid, tamper_detected
    """
    # Derive entity identifiers
    if entity_type is None or entity_id is None:
        parts = stream_id.split("-", 1)
        entity_type = parts[0] if entity_type is None else entity_type
        entity_id = parts[1] if (entity_id is None and len(parts) > 1) else (entity_id or stream_id)

    audit_stream = f"audit-{entity_type}-{entity_id}"

    # ── Step 1: Load events to verify ────────────────────────────────────────
    events = await store.load_stream(stream_id)

    # ── Step 2: Hash each event payload ──────────────────────────────────────
    event_hashes = [_sha256(_canonical(e.get("payload", {}))) for e in events]

    # ── Step 3: Load previous audit result (if any) ──────────────────────────
    audit_events = await store.load_stream(audit_stream)
    previous_check = None
    for ae in reversed(audit_events):
        if ae.get("event_type") == "AuditIntegrityCheckRun":
            previous_check = ae
            break

    previous_hash: str | None = None
    if previous_check:
        previous_hash = previous_check.get("payload", {}).get("integrity_hash")

    # ── Step 4: Compute chain hash ────────────────────────────────────────────
    chain_parts = []
    if previous_hash:
        chain_parts.append(previous_hash)
    chain_parts.extend(event_hashes)
    integrity_hash = _sha256("".join(chain_parts)) if chain_parts else _sha256("")

    # ── Step 5: Detect tampering ──────────────────────────────────────────────
    if previous_check is None:
        chain_valid = True
        tamper_detected = False
    else:
        # Re-compute the hash as it was computed during the previous check.
        # For that we need the hash of events as they were at previous check time.
        # We detect tampering by comparing what the previous check stored
        # against a fresh re-hash of only those events (up to previous_check's
        # event count).  Simplification: compare stored hash with re-computed
        # hash of the same stream content.
        prev_payload = previous_check.get("payload", {})
        prev_stored_hash = prev_payload.get("integrity_hash", "")
        prev_event_count = prev_payload.get("events_verified_count", 0)

        # Re-hash the first N events (those that existed at previous check time)
        rehash_parts = []
        prev_prev_hash = prev_payload.get("previous_hash")
        if prev_prev_hash:
            rehash_parts.append(prev_prev_hash)
        rehash_parts.extend(event_hashes[:prev_event_count])
        rehash = _sha256("".join(rehash_parts)) if rehash_parts else _sha256("")

        tamper_detected = rehash != prev_stored_hash
        chain_valid = not tamper_detected

    # ── Step 6: Append AuditIntegrityCheckRun ────────────────────────────────
    audit_version = await store.stream_version(audit_stream)
    audit_event = {
        "event_type": "AuditIntegrityCheckRun",
        "event_version": 1,
        "payload": {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "check_timestamp": datetime.now(timezone.utc).isoformat(),
            "events_verified_count": len(events),
            "integrity_hash": integrity_hash,
            "previous_hash": previous_hash,
            "chain_valid": chain_valid,
            "tamper_detected": tamper_detected,
        },
    }
    await store.append(audit_stream, [audit_event], expected_version=audit_version)

    return {
        "stream_id": stream_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "events_verified": len(events),
        "integrity_hash": integrity_hash,
        "previous_hash": previous_hash,
        "chain_valid": chain_valid,
        "tamper_detected": tamper_detected,
    }
