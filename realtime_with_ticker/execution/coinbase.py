"""
Coinbase Advanced Trade trading client.

Implements the ExchangeClient interface for Coinbase's Advanced Trade API.

Coinbase Advanced Trade docs:
    https://docs.cloud.coinbase.com/advanced-trade-api/docs

Authentication:
    - CB-ACCESS-KEY header
    - CB-ACCESS-SIGN header (HMAC-SHA256 of timestamp + method + path + body)
    - CB-ACCESS-TIMESTAMP header
"""

import hashlib
import hmac
import json
import logging
import time
import uuid
from typing import Dict, Optional

import aiohttp

import config
from execution.base import ExchangeClient
from execution.types import (
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical coin_id → Coinbase product_id mapping
# ---------------------------------------------------------------------------
# Hardcoded fallback (used if coin_aliases.json is missing/unreadable)
_COINBASE_FALLBACK: dict = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "ripple": "XRP",
    "cardano": "ADA",
    "dogecoin": "DOGE",
    "avalanche-2": "AVAX",
    "litecoin": "LTC",
}

QUOTE_MAP = {
    "usd": "USD",
    "usdt": "USDT",
}


class CoinbaseClient(ExchangeClient):
    """
    Coinbase Advanced Trade REST client.

    Requires COINBASE_API_KEY and COINBASE_API_SECRET in .env.
    """

    NAME = "coinbase"
    BASE_URL = "https://api.coinbase.com"

    def __init__(self, api_key: str = "", api_secret: str = "", dry_run: bool = True):
        super().__init__(dry_run=dry_run)
        self._api_key = api_key
        self._api_secret = api_secret
        self._coin_map = {**_COINBASE_FALLBACK, **self._load_coin_map_from_json("coinbase")}

    # ------------------------------------------------------------------
    # Symbol mapping
    # ------------------------------------------------------------------

    def get_exchange_symbol(self, coin_id: str, quote: str = "usdt") -> Optional[str]:
        base = self._coin_map.get(coin_id)
        q = QUOTE_MAP.get(quote.lower())
        if base and q:
            return f"{base}-{q}"
        return None

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _sign(self, timestamp: str, method: str, path: str, body: str = "") -> dict:
        """Generate signed headers for a Coinbase Advanced Trade request."""
        message = f"{timestamp}{method.upper()}{path}{body}"
        sig = hmac.new(
            self._api_secret.encode(),
            message.encode(),
            hashlib.sha256,
        ).hexdigest()
        return {
            "CB-ACCESS-KEY": self._api_key,
            "CB-ACCESS-SIGN": sig,
            "CB-ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, body: dict = None) -> dict:
        """Make an authenticated request to Coinbase."""
        timestamp = str(int(time.time()))
        body_str = json.dumps(body) if body else ""
        headers = self._sign(timestamp, method, path, body_str)

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method,
                f"{self.BASE_URL}{path}",
                headers=headers,
                data=body_str if body else None,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                result = await resp.json()

        if "error" in result or "errors" in result:
            err = result.get("error") or result.get("errors", [{}])[0].get("message", "unknown")
            raise Exception(f"Coinbase API error: {err}")
        return result

    # ------------------------------------------------------------------
    # Live execution
    # ------------------------------------------------------------------

    async def _execute_create_order(self, request: OrderRequest) -> OrderResult:
        product_id = self.get_exchange_symbol(request.coin_id, request.quote)
        if not product_id:
            return OrderResult(
                exchange=self.NAME,
                coin_id=request.coin_id,
                side=request.side,
                status=OrderStatus.REJECTED,
                error=f"Unknown symbol for {request.coin_id}/{request.quote}",
            )

        client_order_id = str(uuid.uuid4())

        body = {
            "client_order_id": client_order_id,
            "product_id": product_id,
            "side": request.side.value.upper(),
        }

        if request.order_type == OrderType.MARKET:
            if request.side == OrderSide.BUY:
                # Market buy: specify quote_size (USD amount to spend)
                body["order_configuration"] = {
                    "market_market_ioc": {
                        "quote_size": str(round(request.size_usd, 2)),
                    }
                }
            else:
                # Market sell: specify base_size (crypto amount)
                if request.limit_price and request.limit_price > 0:
                    base_size = request.size_usd / request.limit_price
                    body["order_configuration"] = {
                        "market_market_ioc": {
                            "base_size": str(round(base_size, 8)),
                        }
                    }
                else:
                    return OrderResult(
                        exchange=self.NAME,
                        coin_id=request.coin_id,
                        side=request.side,
                        status=OrderStatus.REJECTED,
                        error="Market sell requires limit_price hint for volume calc",
                    )
        else:
            # Limit order
            if request.limit_price and request.limit_price > 0:
                base_size = request.size_usd / request.limit_price
                body["order_configuration"] = {
                    "limit_limit_gtc": {
                        "base_size": str(round(base_size, 8)),
                        "limit_price": str(request.limit_price),
                    }
                }

        result = await self._request("POST", "/api/v3/brokerage/orders", body)
        order_id = result.get("order_id") or result.get("success_response", {}).get("order_id")

        return OrderResult(
            exchange=self.NAME,
            coin_id=request.coin_id,
            side=request.side,
            status=OrderStatus.OPEN,
            order_id=order_id,
        )

    async def _execute_get_order(self, order_id: str) -> OrderResult:
        result = await self._request("GET", f"/api/v3/brokerage/orders/historical/{order_id}")
        order = result.get("order", {})
        status_str = order.get("status", "UNKNOWN")

        status_map = {
            "FILLED": OrderStatus.FILLED,
            "CANCELLED": OrderStatus.CANCELLED,
            "OPEN": OrderStatus.OPEN,
            "PENDING": OrderStatus.PENDING,
            "FAILED": OrderStatus.FAILED,
        }

        filled_size = float(order.get("filled_size", 0))
        filled_value = float(order.get("filled_value", 0))
        total_fees = float(order.get("total_fees", 0))
        avg_price = filled_value / filled_size if filled_size > 0 else 0

        return OrderResult(
            exchange=self.NAME,
            coin_id="",
            side=OrderSide.BUY,
            status=status_map.get(status_str, OrderStatus.FAILED),
            order_id=order_id,
            filled_qty=filled_size,
            filled_price=avg_price,
            filled_usd=filled_value,
            fee=total_fees,
        )

    async def _execute_cancel_order(self, order_id: str) -> bool:
        try:
            await self._request(
                "POST",
                "/api/v3/brokerage/orders/batch_cancel",
                {"order_ids": [order_id]},
            )
            return True
        except Exception as e:
            logger.error(f"[coinbase] cancel failed: {e}")
            return False

    async def _fetch_balances(self) -> Dict[str, float]:
        result = await self._request("GET", "/api/v3/brokerage/accounts")
        balances = {}
        for account in result.get("accounts", []):
            currency = account.get("currency", "").lower()
            available = float(account.get("available_balance", {}).get("value", 0))
            if available > 0:
                balances[currency] = available
        return balances
