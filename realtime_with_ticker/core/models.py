"""
Data models shared across the realtime pipeline.

RawTick:        Exchange-specific event straight from the websocket.
NormalizedTick: Unified format after alias resolution + field mapping.
"""

from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class RawTick:
    """
    Raw tick event emitted by an exchange connector.

    Contains exchange-specific field names and values — no normalization
    has been applied yet.  The normalizer is responsible for converting
    this into a NormalizedTick.
    """

    exchange: str       # e.g. "kraken"
    pair: str           # exchange-native format, e.g. "XBT/USD"
    data: dict          # exchange-specific payload (bid, ask, etc.)
    received_at: float = field(default_factory=time.time)


@dataclass
class NormalizedTick:
    """
    Unified tick format after normalization.

    This is the canonical representation that gets written to Redis.
    All exchange-specific quirks have been resolved:
      - coin_id is the CoinGecko canonical ID (e.g. "bitcoin", not "XBT")
      - quote is lowercase (e.g. "usd")
      - price is the mid price (bid + ask) / 2
    """

    coin_id: str            # canonical ID, e.g. "bitcoin"
    quote: str              # quote currency, e.g. "usd"
    exchange: str           # source exchange, e.g. "kraken"
    price: float            # mid price = (bid + ask) / 2
    bid: float
    ask: float
    last: float             # last traded price
    vwap: Optional[float] = None
    volume_24h: Optional[float] = None
    spread_pct: float = 0.0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        """Serialize for Redis storage."""
        return {
            "coin_id": self.coin_id,
            "quote": self.quote,
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
