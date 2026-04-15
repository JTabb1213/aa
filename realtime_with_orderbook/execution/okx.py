"""
OKX trading client.

Implements the ExchangeClient interface for OKX's v5 REST API.

OKX v5 docs:
    https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order

Authentication:
    - OK-ACCESS-KEY header
    - OK-ACCESS-SIGN header (Base64 HMAC-SHA256)
    - OK-ACCESS-TIMESTAMP header (ISO 8601)
    - OK-ACCESS-PASSPHRASE header

Base URL:
    Configured via OKX_BASE_URL in .env.
    US accounts MUST use https://us.okx.com

Order behaviour (fees):
    - Market BUY  (tgtCcy=quote_ccy): sz is in USDT.
      Fee is charged in the received base currency (ADA).
    - Market SELL (tgtCcy=base_ccy):  sz is in ADA.
      Fee is charged in USDT from the proceeds.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone
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
# Canonical coin_id → OKX symbol mapping
# ---------------------------------------------------------------------------
_OKX_FALLBACK: dict = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "ripple": "XRP",
    "cardano": "ADA",
    "dogecoin": "DOGE",
    "avalanche-2": "AVAX",
    "litecoin": "LTC",
}

QUOTE_MAP = {"usd": "USDT", "usdt": "USDT"}


class OKXClient(ExchangeClient):
    """
    OKX v5 REST trading client (spot).

    Requires OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE in .env.
    US accounts require OKX_BASE_URL=https://us.okx.com
    """

    NAME = "okx"

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        dry_run: bool = True,
    ):
        super().__init__(dry_run=dry_run)
        self._api_key = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        self._base_url = config.OKX_BASE_URL
        self._coin_map = {
            **_OKX_FALLBACK,
            **self._load_coin_map_from_json("okx"),
        }

    # ------------------------------------------------------------------
    # Symbol mapping
    # ------------------------------------------------------------------

    def get_exchange_symbol(
        self, coin_id: str, quote: str = "usdt"
    ) -> Optional[str]:
        base = self._coin_map.get(coin_id)
        q = QUOTE_MAP.get(quote.lower())
        if base and q:
            return f"{base}-{q}"
        return None

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _sign(
        self, timestamp: str, method: str, path: str, body: str = ""
    ) -> dict:
        pre_sign = f"{timestamp}{method.upper()}{path}{body}"
        sig = base64.b64encode(
            hmac.new(
                self._api_secret.encode(),
                pre_sign.encode(),
                hashlib.sha256,
            ).digest()
        ).decode()
        return {
            "OK-ACCESS-KEY": self._api_key,
            "OK-ACCESS-SIGN": sig,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
        }

    async def _request(
        self, method: str, path: str, body: dict = None
    ) -> dict:
        timestamp = (
            datetime.now(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            + "Z"
        )
        body_str = json.dumps(body) if body else ""
        headers = self._sign(timestamp, method, path, body_str)
        url = f"{self._base_url}{path}"

        async with aiohttp.ClientSession() as session:
            if method == "POST":
                async with session.post(
                    url,
                    headers=headers,
                    data=body_str,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    result = await resp.json()
            else:
                async with session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    result = await resp.json()

        if result.get("code", "0") != "0":
            raise Exception(
                f"OKX API error {result.get('code')}: {result.get('msg')}"
            )
        return result

    # ------------------------------------------------------------------
    # Live execution
    # ------------------------------------------------------------------

    async def _execute_create_order(self, request: OrderRequest) -> OrderResult:
        inst_id = self.get_exchange_symbol(request.coin_id, request.quote)
        if not inst_id:
            return OrderResult(
                exchange=self.NAME,
                coin_id=request.coin_id,
                side=request.side,
                status=OrderStatus.REJECTED,
                error=f"Unknown symbol for {request.coin_id}/{request.quote}",
            )

        body: dict = {
            "instId": inst_id,
            "tdMode": "cash",  # spot trading
            "side": "buy" if request.side == OrderSide.BUY else "sell",
            "ordType": (
                "market"
                if request.order_type == OrderType.MARKET
                else "limit"
            ),
        }

        if request.order_type == OrderType.MARKET:
            if request.side == OrderSide.BUY:
                # tgtCcy=quote_ccy → sz is in USDT
                body["tgtCcy"] = "quote_ccy"
                body["sz"] = str(round(request.size_usd, 2))
            else:
                if request.limit_price and request.limit_price > 0:
                    qty = request.size_usd / request.limit_price
                    body["tgtCcy"] = "base_ccy"
                    body["sz"] = str(round(qty, 8))
                else:
                    return OrderResult(
                        exchange=self.NAME,
                        coin_id=request.coin_id,
                        side=request.side,
                        status=OrderStatus.REJECTED,
                        error="Market sell needs limit_price hint",
                    )
        else:
            if request.limit_price and request.limit_price > 0:
                qty = request.size_usd / request.limit_price
                body["sz"] = str(round(qty, 8))
                body["px"] = str(request.limit_price)

        try:
            result = await self._request(
                "POST", "/api/v5/trade/order", body
            )
        except Exception as e:
            return OrderResult(
                exchange=self.NAME,
                coin_id=request.coin_id,
                side=request.side,
                status=OrderStatus.FAILED,
                error=str(e),
            )

        data_list = result.get("data", [{}])
        order_id = data_list[0].get("ordId", "") if data_list else ""

        if not order_id:
            return OrderResult(
                exchange=self.NAME,
                coin_id=request.coin_id,
                side=request.side,
                status=OrderStatus.FAILED,
                error="No ordId returned",
            )

        # Poll for fill (market orders fill nearly instantly)
        for attempt in range(20):
            await asyncio.sleep(0.3)
            try:
                fill = await self._request(
                    "GET",
                    f"/api/v5/trade/order?instId={inst_id}&ordId={order_id}",
                )
                order_data = fill.get("data", [{}])[0]
                state = order_data.get("state", "")

                if state == "filled":
                    filled_qty = float(order_data.get("accFillSz", 0))
                    avg_px = float(order_data.get("avgPx", 0) or 0)
                    fee_raw = abs(float(order_data.get("fee", 0) or 0))
                    fee_ccy = order_data.get("feeCcy", "")

                    # Convert fee to USDT equivalent if charged in
                    # base currency (happens on buys).
                    if fee_ccy.upper() not in ("USDT", "USD", ""):
                        fee_usd = fee_raw * avg_px
                    else:
                        fee_usd = fee_raw

                    return OrderResult(
                        exchange=self.NAME,
                        coin_id=request.coin_id,
                        side=request.side,
                        status=OrderStatus.FILLED,
                        order_id=order_id,
                        filled_qty=filled_qty,
                        filled_price=avg_px,
                        filled_usd=filled_qty * avg_px,
                        fee=fee_usd,
                    )
                elif state in ("canceled", "cancelled"):
                    return OrderResult(
                        exchange=self.NAME,
                        coin_id=request.coin_id,
                        side=request.side,
                        status=OrderStatus.CANCELLED,
                        order_id=order_id,
                        error=f"Order {state}",
                    )
            except Exception as e:
                logger.warning(f"[okx] poll attempt {attempt + 1}: {e}")

        return OrderResult(
            exchange=self.NAME,
            coin_id=request.coin_id,
            side=request.side,
            status=OrderStatus.OPEN,
            order_id=order_id,
            error="Fill not confirmed within polling timeout",
        )

    async def _execute_get_order(self, order_id: str) -> OrderResult:
        # Requires instId; create_order polls internally so this
        # is only needed for manual follow-ups.
        return OrderResult(
            exchange=self.NAME,
            coin_id="",
            side=OrderSide.BUY,
            status=OrderStatus.FAILED,
            error="get_order requires instId — use create_order",
        )

    async def _execute_cancel_order(self, order_id: str) -> bool:
        logger.warning("[okx] cancel_order: instId not available")
        return False

    async def _fetch_balances(self) -> Dict[str, float]:
        result = await self._request("GET", "/api/v5/account/balance")
        balances = {}
        for item in result.get("data", [{}])[0].get("details", []):
            asset = item.get("ccy", "").lower()
            available = float(item.get("availBal", 0))
            if available > 0:
                balances[asset] = available
        return balances
