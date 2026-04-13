"""
Shared types for the execution layer.

All exchange clients accept and return these types.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(str, Enum):
    PENDING = "pending"
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass
class OrderRequest:
    """A normalized order request that any ExchangeClient can execute."""
    exchange: str
    coin_id: str
    side: OrderSide
    quote: str = "usdt"
    size_usd: float = 0.0
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class OrderResult:
    """The result of an order attempt."""
    exchange: str
    coin_id: str
    side: OrderSide
    status: OrderStatus
    order_id: Optional[str] = None
    filled_qty: float = 0.0
    filled_price: float = 0.0
    filled_usd: float = 0.0
    fee: float = 0.0
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    @property
    def is_success(self) -> bool:
        return self.status == OrderStatus.FILLED

    def __str__(self) -> str:
        if self.is_success:
            return (
                f"[order] {self.side.value.upper():<4} {self.coin_id} "
                f"on {self.exchange} — FILLED "
                f"qty={self.filled_qty:.8f} @ ${self.filled_price:.4f} "
                f"(${self.filled_usd:.2f}) fee=${self.fee:.4f}"
            )
        return (
            f"[order] {self.side.value.upper():<4} {self.coin_id} "
            f"on {self.exchange} — {self.status.value}"
            f"{f' ({self.error})' if self.error else ''}"
        )


@dataclass
class ArbitrageTradeResult:
    """Combined result of a two-leg arbitrage trade."""
    coin_id: str
    buy_result: OrderResult
    sell_result: OrderResult
    net_profit_usd: float = 0.0
    status: str = "pending"

    def compute_profit(self) -> None:
        if self.buy_result.is_success and self.sell_result.is_success:
            self.net_profit_usd = self.sell_result.filled_usd - self.buy_result.filled_usd
            self.status = "success"
        elif self.buy_result.is_success or self.sell_result.is_success:
            self.status = "partial"
        else:
            self.status = "failed"
