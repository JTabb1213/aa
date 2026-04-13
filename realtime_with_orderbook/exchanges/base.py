"""
Abstract base class for orderbook exchange connectors.

Every connector must:
  1. Connect to the exchange's websocket API
  2. Subscribe to orderbook channels for configured pairs
  3. Parse incoming messages into RawOrderBook events
  4. Push RawOrderBook events onto the shared ingestion queue

Handles reconnection with exponential backoff automatically.
"""

import asyncio
import logging
from abc import ABC, abstractmethod

from core.models import RawOrderBook

logger = logging.getLogger(__name__)


class BaseExchange(ABC):
    """
    Abstract base for all orderbook exchange connectors.

    Handles:
      - Reconnection with exponential backoff (1s → 2s → ... → 60s cap)
      - Pushing parsed RawOrderBook events to the ingestion queue
      - Health tracking (last message timestamp)

    Subclasses only need to implement _connect_and_stream().
    """

    NAME: str = "unknown"

    def __init__(self, queue: asyncio.Queue):
        self._queue = queue
        self.last_message_time: float = 0
        self._running = False

    @abstractmethod
    async def _connect_and_stream(self) -> None:
        """
        Connect to the exchange websocket, subscribe to orderbook
        channels, and stream messages forever.

        Must call self._emit(book) for each parsed RawOrderBook.
        Should NOT handle reconnection — run() does that.
        """
        ...

    async def _emit(self, book: RawOrderBook) -> None:
        """Push a parsed orderbook onto the ingestion queue."""
        await self._queue.put(book)
        self.last_message_time = book.received_at

    async def run(self) -> None:
        """Run the connector forever with automatic reconnection."""
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
                logger.warning(
                    f"[{self.NAME}] Disconnected cleanly. "
                    f"Reconnecting in {backoff}s..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def stop(self) -> None:
        """Signal the connector to stop."""
        self._running = False
