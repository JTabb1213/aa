"""
Pionex websocket connector.

Connects to Pionex's public WebSocket API, subscribes to the TRADE stream
for USDT pairs, and emits RawTick events onto the ingestion queue.

Pionex's public stream only exposes TRADE and DEPTH topics — there is no
TICKER topic. We use TRADE to extract the latest traded price.

Pionex API docs:
    https://pionex-doc.gitbook.io/apidocs/websocket/general-info
    https://pionex-doc.gitbook.io/apidocs/websocket/public-stream/trade

IMPORTANT — heartbeat:
    The server sends {"op": "PING"} every 15s. The client MUST reply with
    {"op": "PONG", "timestamp": <ms>} or the server closes after 3 missed
    PINGs. The websockets library ping_interval must be set to None so it
    does not interfere with Pionex's custom protocol.

Message format (trade):
    {
        "topic": "TRADE",
        "symbol": "BTC_USDT",
        "data": [
            {
                "symbol": "BTC_USDT",
                "tradeId": "600848671",
                "price": "65432.10",
                "size": "0.0122",
                "side": "BUY",
                "timestamp": 1710000000000
            },
            ...
        ],
        "timestamp": 1710000000000
    }

Note: Pionex uses "_" separated pairs (BTC_USDT).
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
# TEST MODE
# ==============================================================================
TEST_SYMBOLS_OVERRIDE = None
# TEST_SYMBOLS_OVERRIDE = [
#     "BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "ADA_USDT",
#     "DOGE_USDT", "AVAX_USDT", "DOT_USDT", "LINK_USDT", "LTC_USDT",
# ]
# ==============================================================================


class PionexConnector(BaseExchange):
    """
    Pionex exchange websocket connector.

    Lifecycle:
        1. Load symbols from coin_aliases.json (or fallback to API)
        2. Subscribe to ticker stream for all symbols
        3. Parse incoming messages → RawTick → ingestion queue
    """

    NAME = "pionex"

    def __init__(
        self,
        queue: asyncio.Queue,
        quote_currencies: Optional[List[str]] = None,
    ):
        super().__init__(queue)
        self._ws_url = config.PIONEX_WS_URL
        self._rest_url = config.PIONEX_REST_URL
        self._quote_currencies = quote_currencies or ["USDT"]
        self._symbols: List[str] = []

    # ------------------------------------------------------------------
    # Symbol discovery
    # ------------------------------------------------------------------

    def _load_symbols_from_json(self) -> List[str]:
        """Load trading symbols from coin_aliases.json (primary source)."""
        logger.debug(f"[pionex] Attempting to load from: {config.ALIAS_JSON_PATH}")
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                alias_data = json.load(fp)

            symbols = []
            for entry in alias_data.get("assets", {}).values():
                pionex_sym = entry.get("exchange_symbols", {}).get("pionex")
                if pionex_sym:
                    symbols.append(f"{pionex_sym}_USDT")

            symbols = sorted(set(symbols))
            if symbols:
                logger.info(f"[pionex] Loaded {len(symbols)} symbols from coin_aliases.json")
                return symbols
            logger.warning("[pionex] JSON loaded but no symbols found for pionex")
        except Exception as e:
            logger.warning(f"[pionex] Failed to load symbols from JSON: {e}")
        return []

    async def _fetch_symbols_from_api(self) -> List[str]:
        """Fetch trading symbols from Pionex REST API (fallback)."""
        try:
            async with aiohttp.ClientSession() as session:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(self._rest_url, timeout=timeout) as resp:
                    data = await resp.json()

            symbols = []
            for item in data.get("data", {}).get("symbols", []):
                symbol = item.get("symbol", "")
                quote = item.get("quoteCurrency", "")
                enable = item.get("enable", False)

                if quote.upper() in self._quote_currencies and enable:
                    symbols.append(symbol)

            symbols = sorted(set(symbols))
            logger.info(f"[pionex] Fetched {len(symbols)} symbols from API (fallback)")
            return symbols
        except Exception as e:
            logger.error(f"[pionex] API fallback failed: {e}")
            return []

    async def _fetch_symbols(self) -> List[str]:
        """Get trading symbols. Priority: JSON → API → hardcoded."""
        symbols = self._load_symbols_from_json()
        if symbols:
            return symbols

        symbols = await self._fetch_symbols_from_api()
        if symbols:
            return symbols

        logger.warning("[pionex] Using hardcoded seed symbols (last resort)")
        return [
            "BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "ADA_USDT",
            "DOGE_USDT", "AVAX_USDT", "DOT_USDT", "LINK_USDT", "LTC_USDT",
        ]

    # ------------------------------------------------------------------
    # WebSocket streaming
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        """
        Connect to Pionex WebSocket, subscribe to ticker stream,
        and emit RawTick events to the ingestion queue.
        """
        self._symbols = await self._fetch_symbols()

        if TEST_SYMBOLS_OVERRIDE is not None:
            self._symbols = TEST_SYMBOLS_OVERRIDE
            logger.info(f"[pionex] ⚠️  TEST MODE: using {len(self._symbols)} symbols")

        if not self._symbols:
            logger.warning("[pionex] No symbols — sleeping 30s")
            await asyncio.sleep(30)
            return

        logger.info(f"[pionex] Subscribing to {len(self._symbols)} symbols")

        # ping_interval=None: we handle Pionex's custom PING/PONG manually
        async with websockets.connect(self._ws_url, ping_interval=None) as ws:
            logger.info(f"[pionex] WebSocket connected to {self._ws_url}")
            # Subscribe to each symbol's TRADE stream
            for sym in self._symbols:
                subscribe_msg = json.dumps({
                    "op": "SUBSCRIBE",
                    "topic": "TRADE",
                    "symbol": sym,
                })
                await ws.send(subscribe_msg)

            logger.info("[pionex] Connected and subscribed")

            async for message in ws:
                data = json.loads(message)

                op = data.get("op", "")

                # --- Heartbeat: reply to PING with PONG ---
                if op == "PING":
                    pong = json.dumps({"op": "PONG", "timestamp": int(time.time() * 1000)})
                    await ws.send(pong)
                    continue

                if op == "CLOSE":
                    logger.warning("[pionex] Server sent CLOSE")
                    raise ConnectionError("[pionex] Server closed connection (geo-restriction?)")

                topic = data.get("topic", "")
                if topic != "TRADE":
                    continue

                symbol = data.get("symbol", "")
                trades = data.get("data", [])
                if not trades:
                    continue

                # Use the most recent trade as the price snapshot
                latest = trades[0]
                try:
                    last = float(latest.get("price", 0) or 0)
                    volume = float(latest.get("size", 0) or 0)
                except (ValueError, TypeError):
                    continue

                tick = RawTick(
                    exchange="pionex",
                    pair=symbol,  # "BTC_USDT" format
                    data={
                        "bid": None,
                        "ask": None,
                        "last": last,
                        "vwap": None,
                        "volume_24h": volume,
                    },
                    received_at=time.time(),
                )

                await self._emit(tick)
