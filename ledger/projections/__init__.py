"""
ledger/projections/__init__.py
================================
Base Projection ABC. All projections implement this interface so the
ProjectionDaemon can handle them uniformly.
"""
from __future__ import annotations
from abc import ABC, abstractmethod


class Projection(ABC):
    """
    Read-model projection. Receives events from the ProjectionDaemon and
    updates its in-memory (or DB-backed) state.

    Projections are idempotent: replaying the same event twice must produce
    the same state as replaying it once.
    """

    #: Unique name — used as the checkpoint key.
    name: str

    @abstractmethod
    async def handle(self, event: dict) -> None:
        """Process one event. Must not raise on unknown event types."""

    @abstractmethod
    async def truncate(self) -> None:
        """
        Clear all projection state.
        Called by ProjectionDaemon.rebuild_from_scratch() before replay.
        """
