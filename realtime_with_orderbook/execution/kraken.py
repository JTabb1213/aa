"""Kraken trading client (stub — dry-run handled by base class)."""

from typing import Dict, Optional
from execution.base import ExchangeClient
from execution.types import OrderRequest, OrderResult, OrderStatus, OrderSide

QUOTE_MAP = {"usd": "USD", "usdt": "USDT"}


class KrakenClient(ExchangeClient):
    NAME = "kraken"

    def __init__(self, api_key: str = "", api_secret: str = "", dry_run: bool = True):
        super().__init__(dry_run=dry_run)
        self._api_key = api_key
        self._api_secret = api_secret
        self._coin_map = {
            "bitcoin": "XBT", "ethereum": "ETH", "solana": "SOL",
            **self._load_coin_map_from_json("kraken"),
        }

    def get_exchange_symbol(self, coin_id: str, quote: str = "usdt") -> Optional[str]:
        base = self._coin_map.get(coin_id)
        q = QUOTE_MAP.get(quote.lower())
        return f"{base}/{q}" if base and q else None

    async def _execute_create_order(self, request: OrderRequest) -> OrderResult:
        # TODO: Implement Kraken REST trading API
        raise NotImplementedError("Kraken live trading not yet implemented")

    async def _execute_get_order(self, order_id: str) -> OrderResult:
        raise NotImplementedError

    async def _execute_cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError

    async def _fetch_balances(self) -> Dict[str, float]:
        raise NotImplementedError
