"""
Abstract base class for exchange trading clients.

Provides dry-run simulation and a unified interface.
Subclasses implement exchange-specific API calls.
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional

from execution.types import OrderRequest, OrderResult, OrderStatus, OrderSide

logger = logging.getLogger(__name__)


class ExchangeClient(ABC):
    """Abstract base for all exchange trading clients."""

    NAME: str = "unknown"

    def __init__(self, dry_run: bool = True):
        self._dry_run = dry_run

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    async def create_order(self, request: OrderRequest) -> OrderResult:
        """Place an order. Simulates in dry-run mode."""
        logger.info(
            f"[{self.NAME}] {'DRY-RUN ' if self._dry_run else ''}"
            f"create_order: {request.side.value} {request.coin_id} "
            f"${request.size_usd:.2f}"
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
        if self._dry_run:
            return OrderResult(
                exchange=self.NAME, coin_id="unknown",
                side=OrderSide.BUY, status=OrderStatus.FILLED, order_id=order_id,
            )
        return await self._execute_get_order(order_id)

    async def cancel_order(self, order_id: str) -> bool:
        if self._dry_run:
            return True
        return await self._execute_cancel_order(order_id)

    async def get_balances(self) -> Dict[str, float]:
        if self._dry_run:
            return self._get_simulated_balances()
        return await self._fetch_balances()

    @staticmethod
    def _load_coin_map_from_json(exchange_name: str) -> Dict[str, str]:
        """Load coin_id → exchange symbol from coin_aliases.json."""
        try:
            import config
            with open(config.ALIAS_JSON_PATH) as fh:
                data = json.load(fh)
            return {
                coin_id: entry["exchange_symbols"][exchange_name]
                for coin_id, entry in data.get("assets", {}).items()
                if exchange_name in entry.get("exchange_symbols", {})
            }
        except Exception as exc:
            logger.warning(f"Could not load coin map for '{exchange_name}': {exc}")
            return {}

    @abstractmethod
    def get_exchange_symbol(self, coin_id: str, quote: str = "usdt") -> Optional[str]:
        ...

    @abstractmethod
    async def _execute_create_order(self, request: OrderRequest) -> OrderResult:
        ...

    @abstractmethod
    async def _execute_get_order(self, order_id: str) -> OrderResult:
        ...

    @abstractmethod
    async def _execute_cancel_order(self, order_id: str) -> bool:
        ...

    @abstractmethod
    async def _fetch_balances(self) -> Dict[str, float]:
        ...

    def _simulate_fill(self, request: OrderRequest) -> OrderResult:
        """Simulate an instant fill for test mode."""
        price = request.limit_price or 0.0
        qty = request.size_usd / price if price > 0 else 0.0
        return OrderResult(
            exchange=self.NAME,
            coin_id=request.coin_id,
            side=request.side,
            status=OrderStatus.FILLED,
            order_id=f"test-{self.NAME}-{request.coin_id}",
            filled_qty=qty,
            filled_price=price,
            filled_usd=request.size_usd,
            fee=0.0,
        )

    def _get_simulated_balances(self) -> Dict[str, float]:
        return {"usd": 10_000.0, "btc": 1.0, "eth": 10.0, "sol": 100.0}
