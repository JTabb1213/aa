"""
In-memory price cache — replaces Redis for the arbitrage engine.

Stores the latest normalized tick per coin per exchange using
the PriceAggregator. The arbitrage engine reads from this cache
directly to compare prices across exchanges and detect spreads.

Usage:
    cache = PriceCache()

    # Feed ticks as they arrive (called by the pipeline)
    await cache.write(tick)

    # Query cross-exchange data for a coin
    agg = cache.get_aggregates("bitcoin")
    # {
    #     "highest": {"exchange": "kraken", "price": 67895.50, ...},
    #     "lowest": {"exchange": "coinbase", "price": 67884.74, ...},
    #     "exchange_count": 2,
    #     "exchanges": {...},
    # }
"""

import logging
import time
from typing import Optional, Set

from core.models import NormalizedTick
from compute.aggregator import PriceAggregator

logger = logging.getLogger(__name__)


class PriceCache:
    """
    In-memory price store backed by PriceAggregator.

    Provides the same async write() interface as the old RedisWriter so
    the pipeline requires no structural changes. The arbitrage engine
    can call get_aggregates() on any coin to instantly compare prices
    across all live exchanges.

    Thread safety: single-threaded asyncio — no locking needed.
    """

    def __init__(self):
        self._aggregator = PriceAggregator()
        self._write_count: int = 0
        self._start_time: float = time.time()

    # ------------------------------------------------------------------
    # Pipeline interface (matches old RedisWriter.write signature)
    # ------------------------------------------------------------------

    async def write(self, tick: NormalizedTick) -> None:
        """Store the latest tick for this coin+exchange in memory."""
        self._aggregator.update(tick)
        self._write_count += 1

    # ------------------------------------------------------------------
    # Arbitrage query interface
    # ------------------------------------------------------------------

    def get_aggregates(self, coin_id: str) -> Optional[dict]:
        """
        Return cross-exchange aggregate data for a single coin.

        Returns a dict with keys: avg_price, highest, lowest,
        exchange_count, exchanges, timestamp.
        Returns None if the coin is unknown or all data is stale.
        """
        return self._aggregator.get_aggregates(coin_id)

    def get_all_coins(self) -> Set[str]:
        """Return the set of all coin IDs currently being tracked."""
        return self._aggregator.get_all_coins()

    def get_all_aggregates(self, max_coins: int = 100) -> dict:
        """
        Return a snapshot of the current aggregates for the tracked coins.

        Limits the output to `max_coins` entries to avoid printing too much.
        """
        result = {}
        coins = sorted(self.get_all_coins())[:max_coins]
        for coin_id in coins:
            aggregate = self.get_aggregates(coin_id)
            if aggregate:
                result[coin_id] = self._serialize_aggregate(aggregate)
        return result

    @staticmethod
    def _serialize_aggregate(aggregate: dict) -> dict:
        return {
            "coin_id": aggregate["coin_id"],
            "highest": aggregate["highest"].to_dict(),
            "lowest": aggregate["lowest"].to_dict(),
            "exchange_count": aggregate["exchange_count"],
            "exchanges": {
                exchange: snapshot.to_dict()
                for exchange, snapshot in aggregate["exchanges"].items()
            },
            "timestamp": aggregate["timestamp"],
        }

    @property
    def aggregator(self) -> PriceAggregator:
        """Direct access to the aggregator (for advanced queries)."""
        return self._aggregator

    # ------------------------------------------------------------------
    # Stats (used by health endpoint + pipeline logging)
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        uptime = time.time() - self._start_time
        agg_stats = self._aggregator.stats
        return {
            "write_count": self._write_count,
            "coins_tracked": agg_stats["coins_tracked"],
            "exchange_entries": agg_stats["total_exchange_entries"],
            "uptime_seconds": round(uptime),
        }
