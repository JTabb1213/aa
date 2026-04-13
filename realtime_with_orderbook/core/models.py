"""
Data models for the orderbook-based arbitrage engine.

RawOrderBook:       Exchange-specific orderbook snapshot from websocket.
NormalizedOrderBook: Unified format after alias resolution + normalization.
PriceLevel:         A single (price, size) level in the orderbook.
"""

from dataclasses import dataclass, field
from typing import List, Optional
import time


@dataclass
class PriceLevel:
    """A single price level in the order book: (price, qty)."""
    price: float
    qty: float          # quantity in base asset units

    @property
    def value_usd(self) -> float:
        """Approximate USD value at this level (price × qty)."""
        return self.price * self.qty


@dataclass
class RawOrderBook:
    """
    Raw orderbook event emitted by an exchange connector.

    Contains exchange-specific data — no normalization applied yet.
    The normalizer converts this into a NormalizedOrderBook.
    """
    exchange: str               # e.g. "kraken"
    pair: str                   # exchange-native pair, e.g. "XBT/USDT"
    bids: List[PriceLevel]      # sorted descending by price (best bid first)
    asks: List[PriceLevel]      # sorted ascending by price (best ask first)
    received_at: float = field(default_factory=time.time)


@dataclass
class NormalizedOrderBook:
    """
    Unified orderbook format after normalization.

    All exchange-specific quirks have been resolved:
      - coin_id is the canonical ID (e.g. "bitcoin")
      - quote is lowercase (e.g. "usdt")
      - bids sorted descending (best first)
      - asks sorted ascending (best first)
    """
    coin_id: str                # canonical ID, e.g. "bitcoin"
    quote: str                  # quote currency, e.g. "usdt"
    exchange: str               # source exchange, e.g. "kraken"
    bids: List[PriceLevel]      # best bid first (descending price)
    asks: List[PriceLevel]      # best ask first (ascending price)
    timestamp: float = field(default_factory=time.time)

    @property
    def best_bid(self) -> Optional[PriceLevel]:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[PriceLevel]:
        return self.asks[0] if self.asks else None

    @property
    def mid_price(self) -> float:
        if self.best_bid and self.best_ask:
            return (self.best_bid.price + self.best_ask.price) / 2
        return 0.0

    @property
    def spread_pct(self) -> float:
        if self.best_bid and self.best_ask and self.mid_price > 0:
            return (self.best_ask.price - self.best_bid.price) / self.mid_price * 100
        return 0.0

    def bid_depth_usd(self, max_levels: int = 5) -> float:
        """Total USD value available on the bid side (up to max_levels)."""
        return sum(lvl.value_usd for lvl in self.bids[:max_levels])

    def ask_depth_usd(self, max_levels: int = 5) -> float:
        """Total USD value available on the ask side (up to max_levels)."""
        return sum(lvl.value_usd for lvl in self.asks[:max_levels])

    def effective_buy_price(self, usd_amount: float) -> Optional[float]:
        """
        Walk up the ask side to fill `usd_amount` and return the
        volume-weighted average price you'd actually pay.

        Returns None if the book is too thin to fill the order.
        """
        remaining = usd_amount
        total_qty = 0.0
        total_cost = 0.0

        for lvl in self.asks:
            level_value = lvl.value_usd
            if level_value >= remaining:
                qty = remaining / lvl.price
                total_qty += qty
                total_cost += remaining
                remaining = 0
                break
            else:
                total_qty += lvl.qty
                total_cost += level_value
                remaining -= level_value

        if remaining > 0:
            return None  # not enough liquidity
        return total_cost / total_qty if total_qty > 0 else None

    def effective_sell_price(self, usd_amount: float) -> Optional[float]:
        """
        Walk down the bid side to sell `usd_amount` and return the
        volume-weighted average price you'd actually receive.

        Returns None if the book is too thin to fill the order.
        """
        remaining = usd_amount
        total_qty = 0.0
        total_proceeds = 0.0

        for lvl in self.bids:
            level_value = lvl.value_usd
            if level_value >= remaining:
                qty = remaining / lvl.price
                total_qty += qty
                total_proceeds += remaining
                remaining = 0
                break
            else:
                total_qty += lvl.qty
                total_proceeds += level_value
                remaining -= level_value

        if remaining > 0:
            return None  # not enough liquidity
        return total_proceeds / total_qty if total_qty > 0 else None
