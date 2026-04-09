"""
Abstract base class for exchange trading clients.

Every exchange client must implement:
    - create_order()   — place a buy or sell order
    - get_order()      — check the status of an existing order
    - cancel_order()   — cancel an open order
    - get_balances()   — fetch available balances

Subclasses handle all exchange-specific API details:
    - Authentication (API key, secret, signing)
    - Symbol mapping (canonical coin_id → exchange symbol)
    - Request/response format translation
    - Rate limiting

To add a new exchange:
    1. Create execution/your_exchange.py
    2. Subclass ExchangeClient
    3. Implement the four abstract methods
    4. Register it in main.py
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional

from execution.types import OrderRequest, OrderResult, OrderStatus, OrderSide

logger = logging.getLogger(__name__)


class ExchangeClient(ABC):
    """
    Abstract base for all exchange trading clients.

    Provides:
        - A unified interface for order management
        - Dry-run mode (simulates orders without hitting the exchange)
        - Symbol resolution hook for subclasses

    Subclasses only need to implement the _execute_* and _fetch_* methods.
    The public methods handle dry-run checks and error wrapping.
    """

    NAME: str = "unknown"

    def __init__(self, dry_run: bool = True):
        """
        Args:
            dry_run: If True, orders are simulated locally instead of
                     being sent to the exchange. Always start with True.
        """
        self._dry_run = dry_run

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    # ------------------------------------------------------------------
    # Public API (do not override — override the _execute_* hooks below)
    # ------------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> OrderResult:
        """
        Place an order on this exchange.

        In dry-run mode, simulates a fill at the current price.
        In live mode, delegates to _execute_create_order().
        """
        logger.info(
            f"[{self.NAME}] {'DRY-RUN ' if self._dry_run else ''}"
            f"create_order: {request.side.value} {request.coin_id} "
            f"${request.size_usd:.2f} {request.order_type.value}"
        )

        if self._dry_run:
            return self._simulate_fill(request)

        try:
            return await self._execute_create_order(request)
        except Exception as e:
            logger.error(f"[{self.NAME}] create_order failed: {e}")
            return OrderResult(
                exchange=self.NAME,
                coin_id=request.coin_id,
                side=request.side,
                status=OrderStatus.FAILED,
                error=str(e),
            )

    async def get_order(self, order_id: str) -> OrderResult:
        """Check the status of an existing order."""
        if self._dry_run:
            return OrderResult(
                exchange=self.NAME,
                coin_id="unknown",
                side=OrderSide.BUY,
                status=OrderStatus.FILLED,
                order_id=order_id,
            )
        return await self._execute_get_order(order_id)

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True if successfully cancelled."""
        if self._dry_run:
            logger.info(f"[{self.NAME}] DRY-RUN cancel_order: {order_id}")
            return True
        return await self._execute_cancel_order(order_id)

    async def get_balances(self) -> Dict[str, float]:
        """
        Fetch available balances from the exchange.

        Returns a dict like: {"usd": 1500.00, "btc": 0.05, "eth": 2.0}
        Keys are lowercase asset names.
        """
        if self._dry_run:
            return self._get_simulated_balances()
        return await self._fetch_balances()

    # ------------------------------------------------------------------
    # Symbol mapping (override in subclass)
    # ------------------------------------------------------------------

    @abstractmethod
    def get_exchange_symbol(self, coin_id: str, quote: str = "usdt") -> Optional[str]:
        """
        Map a canonical coin_id + quote to the exchange-native symbol.

        Examples:
            "bitcoin", "usdt" → "XBTUSDT"    (Kraken)
            "bitcoin", "usdt" → "BTC-USDT"   (Coinbase)
            "bitcoin", "usdt" → "BTCUSDT"    (Binance)

        Returns None if the pair is not supported on this exchange.
        """
        ...

    # ------------------------------------------------------------------
    # Subclass must implement these (live execution)
    # ------------------------------------------------------------------

    @abstractmethod
    async def _execute_create_order(self, request: OrderRequest) -> OrderResult:
        """Send an order to the exchange. Must handle auth, signing, etc."""
        ...

    @abstractmethod
    async def _execute_get_order(self, order_id: str) -> OrderResult:
        """Query order status from the exchange."""
        ...

    @abstractmethod
    async def _execute_cancel_order(self, order_id: str) -> bool:
        """Cancel an order on the exchange. Return True on success."""
        ...

    @abstractmethod
    async def _fetch_balances(self) -> Dict[str, float]:
        """Fetch real balances from the exchange."""
        ...

    # ------------------------------------------------------------------
    # Dry-run simulation
    # ------------------------------------------------------------------

    def _simulate_fill(self, request: OrderRequest) -> OrderResult:
        """Simulate an instant fill for dry-run mode."""
        # In dry-run, assume fill at the requested size
        # (Real execution will use actual fill prices from the exchange)
        simulated_price = request.limit_price or 0.0
        simulated_qty = (
            request.size_usd / simulated_price if simulated_price > 0 else 0.0
        )
        return OrderResult(
            exchange=self.NAME,
            coin_id=request.coin_id,
            side=request.side,
            status=OrderStatus.FILLED,
            order_id=f"dry-run-{self.NAME}-{request.coin_id}",
            filled_qty=simulated_qty,
            filled_price=simulated_price,
            filled_usd=request.size_usd,
            fee=0.0,
        )

    def _get_simulated_balances(self) -> Dict[str, float]:
        """Return fake balances for dry-run mode."""
        return {
            "usd": 10_000.0,
            "btc": 1.0,
            "eth": 10.0,
            "sol": 100.0,
        }
