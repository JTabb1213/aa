"""
Abstract base class for exchange websocket connectors.

Every exchange connector must:
  1. Connect to the exchange's websocket API
  2. Parse incoming messages into RawTick events
  3. Push RawTick events onto the shared ingestion queue

To add a new exchange:
  1. Create a new file in exchanges/ (e.g. binance.py)
  2. Subclass BaseExchange
  3. Implement _connect_and_stream()
  4. Register it in main.py's connector list
"""

import asyncio
import logging
from abc import ABC, abstractmethod

from core.models import RawTick

logger = logging.getLogger(__name__)


class BaseExchange(ABC):
    """
    Abstract base for all exchange websocket connectors.

    Handles:
      - Reconnection with exponential backoff
      - Pushing parsed RawTick events to the ingestion queue
      - Health tracking (last message timestamp)

    Subclasses only need to implement _connect_and_stream() — the
    reconnection logic, queue management, and health tracking are
    all handled here in the base class.
    """

    NAME: str = "unknown"   # Override in every subclass

    def __init__(self, queue: asyncio.Queue):
        self._queue = queue
        self.last_message_time: float = 0
        self._running = False

    # ------------------------------------------------------------------
    # Subclass must implement this
    # ------------------------------------------------------------------

    @abstractmethod
    async def _connect_and_stream(self) -> None:
        """
        Connect to the exchange websocket, subscribe to channels,
        and stream messages forever.

        Must call self._emit(tick) for each parsed RawTick.

        This method should NOT handle reconnection — the run() wrapper
        does that automatically with exponential backoff.
        """
        ...

    # ------------------------------------------------------------------
    # Provided by base class
    # ------------------------------------------------------------------

    async def _emit(self, tick: RawTick) -> None:
        """Push a parsed tick onto the ingestion queue."""
        await self._queue.put(tick)
        self.last_message_time = tick.received_at

    async def run(self) -> None:
        """
        Run the connector forever with automatic reconnection.
        Uses exponential backoff: 1s → 2s → 4s → 8s → ... capped at 60s.
        Resets to 1s after a successful connection.
        """
        self._running = True
        backoff = 1

        while self._running:
            try:
                logger.info(f"[{self.NAME}] Connecting...")
                await self._connect_and_stream()
            except Exception as e:
                logger.error(
                    f"[{self.NAME}] Connection error: {e}. "
                    f"Reconnecting in {backoff}s..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            else:
                # Clean disconnect (shouldn't normally happen) — reset backoff
                backoff = 1

    def stop(self) -> None:
        """Signal the connector to stop."""
        self._running = False
