"""
Price Aggregator — computes avg, best, worst across exchanges.

Maintains a rolling window of exchange prices per coin and
computes aggregates on demand. Stateless computations that
can be tested independently of Redis.

Usage:
    aggregator = PriceAggregator()
    
    # Feed ticks as they arrive
    aggregator.update(tick)
    
    # Get aggregates for a specific coin
    result = aggregator.get_aggregates("bitcoin")
    # {
    #     "avg_price": 67890.12,
    #     "highest": {"exchange": "kraken", "price": 67895.50, ...},
    #     "lowest": {"exchange": "coinbase", "price": 67884.74, ...},
    #     "exchange_count": 2,
    # }
"""

import time
from dataclasses import dataclass
from typing import Dict, Optional, Set

from core.models import NormalizedTick


# How long to keep exchange data before considering it stale (seconds)
EXCHANGE_STALENESS_TTL = 30.0


@dataclass
class ExchangeSnapshot:
    """Snapshot of a single exchange's price for a coin."""
    exchange: str
    price: float
    bid: float
    ask: float
    last: float
    vwap: Optional[float]
    volume_24h: Optional[float]
    spread_pct: float
    timestamp: float
    
    @classmethod
    def from_tick(cls, tick: NormalizedTick) -> "ExchangeSnapshot":
        return cls(
            exchange=tick.exchange,
            price=tick.price,
            bid=tick.bid,
            ask=tick.ask,
            last=tick.last,
            vwap=tick.vwap,
            volume_24h=tick.volume_24h,
            spread_pct=tick.spread_pct,
            timestamp=tick.timestamp,
        )
    
    def to_dict(self) -> dict:
        return {
            "exchange": self.exchange,
            "price": self.price,
            "bid": self.bid,
            "ask": self.ask,
            "last": self.last,
            "vwap": self.vwap,
            "volume_24h": self.volume_24h,
            "spread_pct": self.spread_pct,
            "timestamp": self.timestamp,
        }


class PriceAggregator:
    """
    Tracks prices from multiple exchanges per coin and computes aggregates.
    
    Maintains an in-memory cache of the latest price from each exchange
    for each coin. Prunes stale data automatically.
    """
    
    def __init__(self, staleness_ttl: float = EXCHANGE_STALENESS_TTL):
        # coin_id -> {exchange -> ExchangeSnapshot}
        self._data: Dict[str, Dict[str, ExchangeSnapshot]] = {}
        self._staleness_ttl = staleness_ttl
    
    def update(self, tick: NormalizedTick) -> None:
        """
        Update the price for a coin from an exchange.
        Call this for every normalized tick.
        """
        coin_id = tick.coin_id
        if coin_id not in self._data:
            self._data[coin_id] = {}
        
        self._data[coin_id][tick.exchange] = ExchangeSnapshot.from_tick(tick)
    
    def update_batch(self, ticks: list) -> Set[str]:
        """
        Update prices from a batch of ticks.
        Returns the set of coin_ids that were updated.
        """
        updated_coins = set()
        for tick in ticks:
            self.update(tick)
            updated_coins.add(tick.coin_id)
        return updated_coins
    
    def _prune_stale(self, coin_id: str) -> None:
        """Remove exchange data older than staleness TTL."""
        if coin_id not in self._data:
            return
        
        now = time.time()
        stale = [
            ex for ex, snap in self._data[coin_id].items()
            if now - snap.timestamp > self._staleness_ttl
        ]
        for ex in stale:
            del self._data[coin_id][ex]
    
    def get_aggregates(self, coin_id: str) -> Optional[dict]:
        """
        Compute aggregate stats for a coin across all tracked exchanges.
        
        Returns:
            {
                "coin_id": str,
                "highest": ExchangeSnapshot (highest price),
                "lowest": ExchangeSnapshot (lowest price),
                "exchange_count": int,
                "exchanges": {exchange: ExchangeSnapshot, ...},
                "timestamp": float,
            }
            
        Returns None if no valid exchange data exists.
        """
        self._prune_stale(coin_id)
        
        exchanges = self._data.get(coin_id, {})
        if not exchanges:
            return None
        
        # Filter out invalid prices
        valid = {
            ex: snap for ex, snap in exchanges.items()
            if snap.price > 0
        }
        
        if not valid:
            return None
        
        # Find highest and lowest priced exchanges
        highest_ex = max(valid, key=lambda ex: valid[ex].price)
        lowest_ex = min(valid, key=lambda ex: valid[ex].price)

        return {
            "coin_id": coin_id,
            "highest": valid[highest_ex],
            "lowest": valid[lowest_ex],
            "exchange_count": len(valid),
            "exchanges": valid,
            "timestamp": time.time(),
        }
    
    def get_all_coins(self) -> Set[str]:
        """Return set of all coin_ids currently being tracked."""
        return set(self._data.keys())
    
    @property
    def stats(self) -> dict:
        """Stats for health check / debugging."""
        total_exchanges = sum(len(exs) for exs in self._data.values())
        return {
            "coins_tracked": len(self._data),
            "total_exchange_entries": total_exchanges,
        }
