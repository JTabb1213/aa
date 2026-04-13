"""
Execution manager — routes orders to the correct exchange client.
"""

import logging
from typing import Dict, Optional

from execution.base import ExchangeClient
from execution.types import OrderRequest, OrderResult, OrderStatus

logger = logging.getLogger(__name__)


class ExecutionManager:
    """Central registry of exchange trading clients."""

    def __init__(self) -> None:
        self._clients: Dict[str, ExchangeClient] = {}

    def register(self, client: ExchangeClient) -> None:
        name = client.NAME
        self._clients[name] = client
        logger.info(f"[exec-mgr] registered {name} (dry_run={client._dry_run})")

    def get_client(self, exchange: str) -> Optional[ExchangeClient]:
        return self._clients.get(exchange.lower())

    @property
    def exchanges(self) -> list:
        return list(self._clients.keys())

    async def place_order(self, request: OrderRequest) -> OrderResult:
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
        client = self.get_client(exchange)
        if client is None:
            return False
        return await client.cancel_order(order_id)

    async def get_all_balances(self) -> Dict[str, Dict[str, float]]:
        result: Dict[str, Dict[str, float]] = {}
        for name, client in self._clients.items():
            try:
                result[name] = await client.get_balances()
            except Exception as e:
                logger.error(f"[exec-mgr] balance fetch failed for {name}: {e}")
                result[name] = {}
        return result
