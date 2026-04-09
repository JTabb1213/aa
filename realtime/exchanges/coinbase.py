"""
Coinbase websocket connector.

Connects to Coinbase's WebSocket feed API, subscribes to the ticker channel
for USD pairs, and emits RawTick events onto the ingestion queue.

Ticker channel docs:
    https://docs.cloud.coinbase.com/exchange/docs/websocket-overview

Message format (ticker):
    {
        "type": "ticker",
        "product_id": "BTC-USD",
        "price": "65432.10",
        "best_bid": "65432.00",
        "best_ask": "65433.00",
        "volume_24h": "12345.67",
        "time": "2026-03-29T12:00:00.000000Z"
    }
"""

import asyncio
import json
import logging
import time
from typing import List, Optional

import aiohttp
import websockets

import config
from core.models import RawTick
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)


# ==============================================================================
# TEST MODE — swap in a short list for development
# ==============================================================================
TEST_PRODUCTS_OVERRIDE = None
# TEST_PRODUCTS_OVERRIDE = [
#     "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD",
#     "DOGE-USD", "AVAX-USD", "DOT-USD", "LINK-USD", "LTC-USD",
# ]
# ==============================================================================


class CoinbaseConnector(BaseExchange):
    """
    Coinbase exchange websocket connector.

    Lifecycle:
        1. Fetch all tradeable products from Coinbase REST API
        2. Filter to configured quote currencies (default: USD only)
        3. Open websocket connection, subscribe to ticker channel
        4. Parse incoming ticker messages → RawTick → ingestion queue
    """

    NAME = "coinbase"

    def __init__(
        self,
        queue: asyncio.Queue,
        quote_currencies: Optional[List[str]] = None,
    ):
        super().__init__(queue)
        self._ws_url = config.COINBASE_WS_URL
        self._rest_url = config.COINBASE_REST_URL
        self._quote_currencies = quote_currencies or config.QUOTE_CURRENCIES
        self._products: List[str] = []

    # ------------------------------------------------------------------
    # Product discovery via REST API
    # ------------------------------------------------------------------

    def _load_products_from_json(self) -> List[str]:
        """
        Load trading products from coin_aliases.json (primary source).
        
        This is the source of truth for which products to subscribe to.
        The JSON file is updated periodically via cron/manual refresh.
        """
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                alias_data = json.load(fp)
            
            products = []
            for entry in alias_data.get("assets", {}).values():
                coinbase_sym = entry.get("exchange_symbols", {}).get("coinbase")
                if coinbase_sym:
                    # Coinbase uses "BTC-USD" format
                    products.append(f"{coinbase_sym}-USD")
            
            products = sorted(set(products))
            if products:
                logger.info(f"[coinbase] Loaded {len(products)} products from coin_aliases.json")
                return products
        except Exception as e:
            logger.warning(f"[coinbase] Failed to load products from JSON: {e}")
        
        return []

    async def _fetch_products_from_api(self) -> List[str]:
        """
        Fetch trading products from Coinbase's REST API (fallback).
        
        Used only if coin_aliases.json is missing or empty.
        """
        try:
            async with aiohttp.ClientSession() as session:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(self._rest_url, timeout=timeout) as resp:
                    data = await resp.json()

            products = []
            for product in data:
                product_id = product.get("id", "")
                quote = product.get("quote_currency", "")
                status = product.get("status", "")

                # Filter by quote currency and online status
                if quote.upper() in self._quote_currencies and status == "online":
                    products.append(product_id)

            products = sorted(set(products))
            logger.info(f"[coinbase] Fetched {len(products)} products from API (fallback)")
            return products

        except Exception as e:
            logger.error(f"[coinbase] API fallback failed: {e}")
            return []

    async def _fetch_products(self) -> List[str]:
        """
        Get trading products to subscribe to.
        
        Priority:
          1. coin_aliases.json (primary, fast, offline-capable)
          2. Coinbase REST API (fallback if JSON missing/empty)
          3. Hardcoded seed list (last resort)
        """
        # Primary: JSON file
        products = self._load_products_from_json()
        if products:
            return products
        
        # Fallback: API
        products = await self._fetch_products_from_api()
        if products:
            return products
        
        # Last resort: hardcoded seed
        logger.warning("[coinbase] Using hardcoded seed products (last resort)")
        return [
            "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD",
            "DOGE-USD", "AVAX-USD", "DOT-USD", "LINK-USD", "LTC-USD",
        ]

    # ------------------------------------------------------------------
    # WebSocket streaming
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        """
        Connect to Coinbase WebSocket, subscribe to ticker for all products,
        and emit RawTick events to the ingestion queue.
        """
        self._products = await self._fetch_products()

        # ==============================================================================
        # TEST MODE — override full product list with short dev list if set above
        # ==============================================================================
        if TEST_PRODUCTS_OVERRIDE is not None:
            self._products = TEST_PRODUCTS_OVERRIDE
            logger.info(
                f"[coinbase] ⚠️  TEST MODE ACTIVE: using {len(self._products)} products "
                f"instead of full list — set TEST_PRODUCTS_OVERRIDE = None to disable"
            )
        # ==============================================================================

        if not self._products:
            logger.warning("[coinbase] No products to subscribe to — sleeping 30s")
            await asyncio.sleep(30)
            return

        logger.info(f"[coinbase] Subscribing to {len(self._products)} products")

        # Coinbase subscribe message
        subscribe_msg = json.dumps({
            "type": "subscribe",
            "product_ids": self._products,
            "channels": ["ticker"],
        })

        async with websockets.connect(self._ws_url, ping_interval=30) as ws:
            await ws.send(subscribe_msg)
            logger.info(f"[coinbase] Subscribed to ticker channel")

            async for message in ws:
                data = json.loads(message)
                msg_type = data.get("type")

                # Skip non-ticker messages
                if msg_type != "ticker":
                    continue

                product_id = data.get("product_id", "")

                # Parse fields — Coinbase returns strings, convert to float
                try:
                    bid = float(data.get("best_bid", 0) or 0)
                    ask = float(data.get("best_ask", 0) or 0)
                    last = float(data.get("price", 0) or 0)
                    volume = float(data.get("volume_24h", 0) or 0)
                except (ValueError, TypeError):
                    continue

                tick = RawTick(
                    exchange="coinbase",
                    pair=product_id,  # "BTC-USD" format
                    data={
                        "bid": bid,
                        "ask": ask,
                        "last": last,
                        "vwap": None,  # Coinbase doesn't provide VWAP
                        "volume_24h": volume,
                    },
                    received_at=time.time(),
                )

                await self._emit(tick)
