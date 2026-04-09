"""
Shared types for the execution layer.

All exchange clients accept and return these types, so the coordinator
and manager never need to know exchange-specific details.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(str, Enum):
    PENDING = "pending"         # submitted, not yet acknowledged
    OPEN = "open"               # accepted by exchange, waiting to fill
    FILLED = "filled"           # fully filled
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"       # exchange rejected the order
    FAILED = "failed"           # client-side error (network, auth, etc.)


# ---------------------------------------------------------------------------
# Order request (what you send)
# ---------------------------------------------------------------------------

@dataclass
class OrderRequest:
    """
    A normalized order request that any ExchangeClient can execute.

    Fields:
        exchange    — target exchange name (e.g. "kraken")
        coin_id     — canonical coin ID (e.g. "bitcoin")
        side        — buy or sell
        quote       — quote currency (e.g. "usd")
        size_usd    — how much USD to spend (for buys) or receive (for sells)
        order_type  — market or limit
        limit_price — required if order_type is limit
    """
    exchange: str
    coin_id: str
    side: OrderSide
    quote: str = "usd"
    size_usd: float = 0.0
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = None
    timestamp: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Order result (what you get back)
# ---------------------------------------------------------------------------

@dataclass
class OrderResult:
    """
    The result of an order attempt.

    Populated after the exchange responds (or after a client-side failure).
    """
    exchange: str
    coin_id: str
    side: OrderSide
    status: OrderStatus
    order_id: Optional[str] = None      # exchange-assigned order ID
    filled_qty: float = 0.0             # amount of crypto filled
    filled_price: float = 0.0           # average fill price
    filled_usd: float = 0.0            # total USD value of fills
    fee: float = 0.0                    # fee paid (in quote currency)
    error: Optional[str] = None         # error message if failed/rejected
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
            f"on {self.exchange} — {self.status.value.upper()} "
            f"error={self.error}"
        )


# ---------------------------------------------------------------------------
# Arbitrage trade result (both legs together)
# ---------------------------------------------------------------------------

@dataclass
class ArbitrageTradeResult:
    """
    The combined result of a two-sided arbitrage trade.

    Contains both the buy and sell legs plus computed profit.
    """
    coin_id: str
    buy_result: OrderResult
    sell_result: OrderResult
    gross_profit_usd: float = 0.0
    net_profit_usd: float = 0.0         # after fees
    status: str = "pending"             # "success", "partial", "failed"
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    def compute_profit(self) -> None:
        """Calculate profit from filled results."""
        if self.buy_result.is_success and self.sell_result.is_success:
            self.gross_profit_usd = self.sell_result.filled_usd - self.buy_result.filled_usd
            total_fees = self.buy_result.fee + self.sell_result.fee
            self.net_profit_usd = self.gross_profit_usd - total_fees
            self.status = "success"
        elif self.buy_result.is_success or self.sell_result.is_success:
            self.status = "partial"
            self.error = "Only one leg filled — manual intervention needed"
        else:
            self.status = "failed"
            self.error = "Both legs failed"

    def __str__(self) -> str:
        return (
            f"[trade] {self.coin_id} — {self.status.upper()} "
            f"net_profit=${self.net_profit_usd:.4f} "
            f"(buy={self.buy_result.status.value} "
            f"sell={self.sell_result.status.value})"
        )
