"""
scripts/demos/demo_occ.py
==========================
Demonstrates Optimistic Concurrency Control (OCC).

OCC is how the event store prevents two agents from making conflicting writes
to the same stream simultaneously -- without locks spanning multiple requests.

Every append() call includes expected_version.  The DB checks the current
stream version atomically.  If another writer has appended since you last read,
your write is rejected with OptimisticConcurrencyError.  You must reload and retry.

What you will see:
  1. Two concurrent appends at the same expected_version: one wins, one loses.
  2. The losing writer reloads and retries -- second attempt succeeds.
  3. Rapid sequential appends: each one increments version cleanly.
  4. Appending to an archived stream: raises StreamArchivedError.

Run:
    uv run python scripts/demos/demo_occ.py
"""
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from ledger.event_store import EventStore, OptimisticConcurrencyError, StreamArchivedError

DB_URL = os.environ["DATABASE_URL"]
SEP = "-" * 60


def _ev(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


async def main():
    store = EventStore(DB_URL)
    await store.connect()

    for stream_id in ["occ-stream-A", "occ-stream-B", "occ-stream-C"]:
        async with store._pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM outbox WHERE event_id IN "
                "(SELECT event_id FROM events WHERE stream_id=$1)", stream_id
            )
            await conn.execute("DELETE FROM events        WHERE stream_id=$1", stream_id)
            await conn.execute("DELETE FROM event_streams WHERE stream_id=$1", stream_id)

    # ── Scenario 1: concurrent writers on an existing stream ─────────────────
    # Real-world case: two agents both read the stream at version N, then both
    # try to append. Only one transaction can hold the FOR UPDATE lock; the
    # other sees the version has advanced and gets OCC.
    print(SEP)
    print("SCENARIO 1 — two concurrent appends at the same expected_version")
    print("            Both agents read version 1, both try to append at 1.")
    print("            One wins; the other gets OptimisticConcurrencyError.")
    print(SEP)

    stream_a = "occ-stream-A"

    # Seed stream so both writers have a version to compete at
    await store.append(stream_a, [_ev("ApplicationSubmitted")], expected_version=-1)
    race_ver = await store.stream_version(stream_a)   # both agents will read this

    results = {}

    async def writer(name: str, event_type: str):
        try:
            positions = await store.append(
                stream_a, [_ev(event_type, writer=name)], expected_version=race_ver
            )
            results[name] = ("ok", positions[0])
        except OptimisticConcurrencyError as e:
            results[name] = ("occ", str(e))

    await asyncio.gather(writer("Agent-Alpha", "CreditAnalysisRequested"),
                         writer("Agent-Beta",  "CreditAnalysisRequested"))

    for name, (outcome, detail) in results.items():
        icon = "OK " if outcome == "ok" else "OCC"
        print(f"  [{icon}] {name}: {detail}")

    winners = [n for n, (o, _) in results.items() if o == "ok"]
    losers  = [n for n, (o, _) in results.items() if o == "occ"]
    assert len(winners) == 1, f"Expected exactly 1 winner, got {winners}"
    assert len(losers)  == 1, f"Expected exactly 1 loser, got {losers}"

    ver = await store.stream_version(stream_a)
    print(f"\n  Stream version after race: {ver}  (exactly 1 event written)")

    # ── Scenario 2: loser reloads and retries ─────────────────────────────────
    print()
    print(SEP)
    print("SCENARIO 2 — the losing writer reloads and retries")
    print(SEP)

    async def append_with_retry(stream_id: str, event_type: str, label: str, max_retries: int = 3):
        for attempt in range(1, max_retries + 1):
            current_ver = await store.stream_version(stream_id)
            try:
                positions = await store.append(
                    stream_id,
                    [_ev(event_type, label=label, attempt=attempt)],
                    expected_version=current_ver,
                )
                print(f"  Attempt {attempt}: SUCCESS at version {positions[0]}")
                return positions[0]
            except OptimisticConcurrencyError as e:
                print(f"  Attempt {attempt}: OCC ({e}) -- reloading and retrying")
                await asyncio.sleep(0.01)
        raise RuntimeError("Max retries exceeded")

    # Writer B lost in scenario 1; simulate it retrying with the correct version
    loser_name = losers[0]
    print(f"  {loser_name} retrying after OCC:")
    final_pos = await append_with_retry(stream_a, "FraudScreeningRequested", loser_name)
    ver = await store.stream_version(stream_a)
    print(f"  Stream version after retry: {ver}")
    assert ver == final_pos

    # ── Scenario 3: rapid sequential appends ──────────────────────────────────
    print()
    print(SEP)
    print("SCENARIO 3 — 5 sequential appends, each incrementing version by 1")
    print(SEP)

    stream_b = "occ-stream-B"
    event_types = [
        "ApplicationSubmitted",
        "CreditAnalysisRequested",
        "FraudScreeningRequested",
        "ComplianceCheckRequested",
        "DecisionRequested",
    ]

    for et in event_types:
        ver = await store.stream_version(stream_b)
        positions = await store.append(stream_b, [_ev(et)], expected_version=ver)
        new_ver = await store.stream_version(stream_b)
        print(f"  {et:<35}  v{ver} → v{new_ver}")

    final_ver = await store.stream_version(stream_b)
    assert final_ver == len(event_types), f"Expected v{len(event_types)}, got v{final_ver}"

    # ── Scenario 4: wrong expected_version ────────────────────────────────────
    print()
    print(SEP)
    print("SCENARIO 4 — append with stale expected_version (simulates missed update)")
    print(SEP)

    stale_ver = 0  # pretend we read the stream when it was at v0
    actual_ver = await store.stream_version(stream_b)  # it's now at v4
    print(f"  Stale version we have: {stale_ver}")
    print(f"  Actual stream version: {actual_ver}")

    try:
        await store.append(stream_b, [_ev("StaleWrite")], expected_version=stale_ver)
        print("  ERROR: write should have been rejected!")
    except OptimisticConcurrencyError as e:
        print(f"  OCC raised as expected: {e}")
        print(f"  suggested_action: reload stream at v{e.actual} and retry")

    # ── Scenario 5: archived stream ───────────────────────────────────────────
    print()
    print(SEP)
    print("SCENARIO 5 — append to an archived stream raises StreamArchivedError")
    print(SEP)

    stream_c = "occ-stream-C"
    await store.append(stream_c, [_ev("ApplicationSubmitted")], expected_version=-1)
    await store.archive_stream(stream_c)
    print(f"  Stream '{stream_c}' archived.")

    try:
        await store.append(stream_c, [_ev("CreditAnalysisRequested")],
                           expected_version=await store.stream_version(stream_c))
        print("  ERROR: write to archived stream should have been rejected!")
    except StreamArchivedError as e:
        print(f"  StreamArchivedError raised as expected: {e}")

    print()
    print("PASSED — OCC prevents conflicting writes; retry pattern recovers gracefully.")

    await store.close()


if __name__ == "__main__":
    asyncio.run(main())
