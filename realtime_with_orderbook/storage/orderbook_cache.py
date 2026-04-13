"""
In-memory orderbook cache.

Stores the latest NormalizedOrderBook per coin per exchange.
The arbitrage engine reads from this cache to compare orderbooks
across exchanges and detect profitable spreads.
"""

import logging
import time
from typing import Dict, Optional, Set

from core.models import NormalizedOrderBook

logger = logging.getLogger(__name__)

# How long to keep exchange data before considering it stale (seconds)
STALENESS_TTL = 15.0


class OrderBookCache:
    """
    In-memory store of the latest orderbook per coin per exchange.

    The arbitrage engine can call get_books(coin_id) to get all
    exchange orderbooks for a coin and compare them.
    """

    def __init__(self, staleness_ttl: float = STALENESS_TTL):
        # coin_id -> {exchange -> NormalizedOrderBook}
        self._data: Dict[str, Dict[str, NormalizedOrderBook]] = {}
        self._staleness_ttl = staleness_ttl
        self._write_count: int = 0
        self._start_time: float = time.time()

    async def write(self, book: NormalizedOrderBook) -> None:
        """Store the latest orderbook for this coin+exchange."""
        coin_id = book.coin_id
        if coin_id not in self._data:
            self._data[coin_id] = {}
        self._data[coin_id][book.exchange] = book
        self._write_count += 1

    def get_books(self, coin_id: str) -> Optional[Dict[str, NormalizedOrderBook]]:
        """
        Return all exchange orderbooks for a coin.
        Prunes stale entries first.
        Returns None if no valid data.
        """
        self._prune_stale(coin_id)
        books = self._data.get(coin_id, {})
        return books if books else None

    def get_all_coins(self) -> Set[str]:
        """Return all coin IDs currently being tracked."""
        return set(self._data.keys())

    def _prune_stale(self, coin_id: str) -> None:
        """Remove exchange data older than staleness TTL."""
        if coin_id not in self._data:
            return
        now = time.time()
        stale = [
            ex for ex, book in self._data[coin_id].items()
            if now - book.timestamp > self._staleness_ttl
        ]
        for ex in stale:
            del self._data[coin_id][ex]

    @property
    def stats(self) -> dict:
        total_entries = sum(len(exs) for exs in self._data.values())
        return {
            "write_count": self._write_count,
            "coins_tracked": len(self._data),
            "exchange_entries": total_entries,
            "uptime_seconds": round(time.time() - self._start_time),
        }
