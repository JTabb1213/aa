"""
Execution manager — central registry of exchange trading clients.

Provides a single entry-point to:
    • look up the correct client for a given exchange
    • place orders through the unified ExchangeClient interface
    • query balances across all registered exchanges
"""

import logging
from typing import Dict, Optional

from execution.base import ExchangeClient
from execution.types import OrderRequest, OrderResult, OrderStatus

logger = logging.getLogger(__name__)


class ExecutionManager:
    """
    Holds one ExchangeClient per exchange and routes calls.

    Usage:
        mgr = ExecutionManager()
        mgr.register(KrakenClient(...))
        mgr.register(CoinbaseClient(...))
        mgr.register(BinanceClient(...))

        result = await mgr.place_order(OrderRequest(..., exchange="kraken"))
    """

    def __init__(self) -> None:
        self._clients: Dict[str, ExchangeClient] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, client: ExchangeClient) -> None:
        """Register an exchange client (keyed by its NAME)."""
        name = client.NAME
        self._clients[name] = client
        logger.info(f"[exec-mgr] registered {name} (dry_run={client._dry_run})")

    def get_client(self, exchange: str) -> Optional[ExchangeClient]:
        """Return the client for a given exchange name, or None."""
        return self._clients.get(exchange.lower())

    @property
    def exchanges(self) -> list:
        """List of registered exchange names."""
        return list(self._clients.keys())

    # ------------------------------------------------------------------
    # Order routing
    # ------------------------------------------------------------------

    async def place_order(self, request: OrderRequest) -> OrderResult:
        """Route an order to the correct exchange client."""
        client = self.get_client(request.exchange)
        if client is None:
            return OrderResult(
                exchange=request.exchange,
                coin_id=request.coin_id,
                side=request.side,
                status=OrderStatus.REJECTED,
                error=f"No client registered for '{request.exchange}'",
            )
        return await client.create_order(request)

    async def cancel_order(self, exchange: str, order_id: str) -> bool:
        """Cancel an order on the given exchange."""
        client = self.get_client(exchange)
        if client is None:
            logger.error(f"[exec-mgr] cancel: no client for '{exchange}'")
            return False
        return await client.cancel_order(order_id)

    async def get_order(self, exchange: str, order_id: str) -> Optional[OrderResult]:
        """Retrieve order status from the given exchange."""
        client = self.get_client(exchange)
        if client is None:
            return None
        return await client.get_order(order_id)

    # ------------------------------------------------------------------
    # Balances
    # ------------------------------------------------------------------

    async def get_all_balances(self) -> Dict[str, Dict[str, float]]:
        """
        Fetch balances from every registered exchange.

        Returns:
            { "kraken": {"usd": 1200.0, "btc": 0.015}, ... }
        """
        all_bal: Dict[str, Dict[str, float]] = {}
        for name, client in self._clients.items():
            try:
                all_bal[name] = await client.get_balances()
            except Exception as e:
                logger.error(f"[exec-mgr] balance fetch failed for {name}: {e}")
                all_bal[name] = {}
        return all_bal

    async def get_balance(self, exchange: str, asset: str = "usd") -> float:
        """Get single asset balance on one exchange."""
        client = self.get_client(exchange)
        if client is None:
            return 0.0
        balances = await client.get_balances()
        return balances.get(asset.lower(), 0.0)
