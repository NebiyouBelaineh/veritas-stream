"""
ledger/projections/daemon.py
==============================
ProjectionDaemon — polls the EventStore and fans events out to registered
projections, maintaining per-projection checkpoints.

Key behaviours:
  - Polls load_all(from_position=lowest_checkpoint) every `poll_interval` seconds
  - Per-projection checkpoint persisted via store.save/load_checkpoint
  - On handler exception: log, skip event (up to max_retry retries), continue
  - get_lag(name) → ms since the last event processed by that projection
  - rebuild_from_scratch(name) → truncate state + replay all events from 0
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from ledger.projections import Projection

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    """
    Drives one or more Projection instances by pulling events from the store
    and dispatching them in global-position order.

    Usage::

        daemon = ProjectionDaemon(store, [summary_proj, performance_proj, audit_proj])
        await daemon.run_once()   # process all pending events
        # or: await daemon.run() # long-lived polling loop
    """

    def __init__(
        self,
        store,
        projections: list[Projection],
        max_retry: int = 3,
        pool=None,
    ) -> None:
        self.store = store
        self.projections: dict[str, Projection] = {p.name: p for p in projections}
        self.max_retry = max_retry
        self._pool = pool                                # asyncpg Pool for atomic writes
        self._checkpoints: dict[str, int] = {}          # fallback if store has no checkpoints
        self._last_processed_at: dict[str, datetime] = {}
        self._running = False

    # ── Atomic dispatch ───────────────────────────────────────────────────────

    async def _dispatch_atomic(
        self, name: str, projection: Projection, event: dict, gpos: int
    ) -> None:
        """
        Dispatch one event to one projection and save its checkpoint atomically.

        When a pool is available: projection write + checkpoint update happen in
        a single PostgreSQL transaction (satisfies the checkpoint transaction rule).
        When no pool: falls back to in-memory-only updates.
        """
        if self._pool is not None:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await projection.handle(event, conn=conn)
                    await conn.execute(
                        """INSERT INTO projection_checkpoints
                               (projection_name, last_position, updated_at)
                           VALUES ($1, $2, NOW())
                           ON CONFLICT (projection_name) DO UPDATE
                           SET last_position = EXCLUDED.last_position,
                               updated_at    = NOW()""",
                        f"proj_{name}",
                        gpos + 1,   # stored as last_processed+1 so default 0 = "nothing yet"
                    )
            self._checkpoints[name] = gpos   # keep in-memory cache in sync
        else:
            await projection.handle(event)
            await self._save_checkpoint(name, gpos)

    # ── Checkpoint helpers ────────────────────────────────────────────────────

    async def _load_checkpoint(self, name: str) -> int:
        """Returns last processed global_position (-1 if none)."""
        key = f"proj_{name}"
        if hasattr(self.store, "load_checkpoint"):
            # InMemoryEventStore defaults to 0; subtract 1 to get "last processed"
            stored = await self.store.load_checkpoint(key)
            # We store (last_processed + 1) so 0 means "nothing processed yet"
            return stored - 1
        return self._checkpoints.get(name, -1)

    async def _save_checkpoint(self, name: str, last_processed: int) -> None:
        key = f"proj_{name}"
        # Store (last_processed + 1) so default=0 maps to "nothing processed"
        if hasattr(self.store, "save_checkpoint"):
            await self.store.save_checkpoint(key, last_processed + 1)
        self._checkpoints[name] = last_processed

    # ── Core run loop ─────────────────────────────────────────────────────────

    async def run_once(self) -> int:
        """
        Reads all events past each projection's checkpoint and dispatches them.
        Returns the total number of (projection, event) dispatches performed.
        """
        # Find the lowest checkpoint so we load from there (avoid re-reading history)
        min_checkpoint = -1
        for name in self.projections:
            cp = await self._load_checkpoint(name)
            if cp < min_checkpoint or min_checkpoint == -2:  # sentinel
                min_checkpoint = cp
        if min_checkpoint == -2:
            min_checkpoint = -1

        from_position = max(0, min_checkpoint + 1)
        total_dispatched = 0

        async for event in self.store.load_all(from_position=from_position):
            gpos = event["global_position"]

            for name, projection in self.projections.items():
                checkpoint = await self._load_checkpoint(name)
                if gpos <= checkpoint:
                    continue  # already processed by this projection

                # Dispatch with retry
                last_exc: Exception | None = None
                for attempt in range(self.max_retry + 1):
                    try:
                        await self._dispatch_atomic(name, projection, event, gpos)
                        last_exc = None
                        break
                    except Exception as exc:
                        last_exc = exc
                        if attempt < self.max_retry:
                            logger.debug(
                                "Retrying %s for event %s (attempt %d): %s",
                                name, gpos, attempt + 1, exc,
                            )

                if last_exc is not None:
                    logger.warning(
                        "Skipping event at global_position=%d for projection %r "
                        "after %d attempts: %s",
                        gpos, name, self.max_retry + 1, last_exc,
                    )
                    # Advance checkpoint even on failure so bad events never block the daemon.
                    await self._save_checkpoint(name, gpos)
                    self._checkpoints[name] = gpos

                recorded_at = event.get("recorded_at")
                if isinstance(recorded_at, str):
                    try:
                        recorded_at = datetime.fromisoformat(recorded_at)
                    except ValueError:
                        recorded_at = datetime.now(timezone.utc)
                elif not isinstance(recorded_at, datetime):
                    recorded_at = datetime.now(timezone.utc)
                self._last_processed_at[name] = recorded_at
                total_dispatched += 1

        return total_dispatched

    async def run(self, poll_interval: float = 0.1) -> None:
        """Long-lived polling loop. Call stop() to exit gracefully."""
        self._running = True
        while self._running:
            await self.run_once()
            await asyncio.sleep(poll_interval)

    def stop(self) -> None:
        """Signal the polling loop to exit after the current iteration."""
        self._running = False

    # ── Operational helpers ───────────────────────────────────────────────────

    async def get_lag(self, projection_name: str) -> float:
        """
        Returns milliseconds since the last event was processed by this projection.
        Returns 0.0 if no events have been processed yet.
        """
        last_at = self._last_processed_at.get(projection_name)
        if last_at is None:
            return 0.0
        if not last_at.tzinfo:
            last_at = last_at.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - last_at).total_seconds() * 1000

    async def rebuild_from_scratch(self, projection_name: str) -> None:
        """
        Truncates a projection's state and replays all events from global_position 0.
        Used to recover from data corruption or schema changes.
        """
        projection = self.projections[projection_name]
        await projection.truncate()
        # Reset checkpoint to -1 (nothing processed)
        await self._save_checkpoint(projection_name, -1)
        self._last_processed_at.pop(projection_name, None)

        last_pos = -1
        async for event in self.store.load_all(from_position=0):
            try:
                await projection.handle(event)
            except Exception as exc:
                logger.warning(
                    "Skipping bad event at global_position=%d during rebuild of %r: %s",
                    event["global_position"], projection_name, exc,
                )
            last_pos = event["global_position"]

        if last_pos >= 0:
            await self._save_checkpoint(projection_name, last_pos)
            self._last_processed_at[projection_name] = datetime.now(timezone.utc)
