"""
OKX websocket connector.

Connects to OKX's WebSocket v5 API, subscribes to the tickers channel
for USDT spot pairs, and emits RawTick events onto the ingestion queue.

OKX WebSocket docs:
    https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-tickers-channel

Message format (tickers):
    {
        "arg": {"channel": "tickers", "instId": "BTC-USDT"},
        "data": [{
            "instId": "BTC-USDT",
            "last": "65432.1",
            "bidPx": "65432.0",
            "askPx": "65433.0",
            "vol24h": "12345.67",
            ...
        }]
    }

Note: OKX uses "-" separated pairs (BTC-USDT). The normalizer maps
USDT → canonical coin IDs.
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
#     "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "ADA-USDT",
#     "DOGE-USDT", "AVAX-USDT", "DOT-USDT", "LINK-USDT", "LTC-USDT",
# ]
# ==============================================================================


class OKXConnector(BaseExchange):
    """
    OKX exchange websocket connector (v5 spot).

    Lifecycle:
        1. Load symbols from coin_aliases.json (or fallback to API)
        2. Subscribe to tickers for all instruments
        3. Parse incoming messages → RawTick → ingestion queue
    """

    NAME = "okx"

    def __init__(
        self,
        queue: asyncio.Queue,
        quote_currencies: Optional[List[str]] = None,
    ):
        super().__init__(queue)
        self._ws_url = config.OKX_WS_URL
        self._rest_url = config.OKX_REST_URL
        self._quote_currencies = quote_currencies or ["USDT"]
        self._symbols: List[str] = []

    # ------------------------------------------------------------------
    # Symbol discovery
    # ------------------------------------------------------------------

    def _load_symbols_from_json(self) -> List[str]:
        """Load trading symbols from coin_aliases.json (primary source)."""
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                alias_data = json.load(fp)

            symbols = []
            for entry in alias_data.get("assets", {}).values():
                okx_sym = entry.get("exchange_symbols", {}).get("okx")
                if okx_sym:
                    symbols.append(f"{okx_sym}-USDT")

            symbols = sorted(set(symbols))
            if symbols:
                logger.info(f"[okx] Loaded {len(symbols)} symbols from coin_aliases.json")
                return symbols
        except Exception as e:
            logger.warning(f"[okx] Failed to load symbols from JSON: {e}")
        return []

    async def _fetch_symbols_from_api(self) -> List[str]:
        """Fetch instruments from OKX REST API (fallback)."""
        try:
            async with aiohttp.ClientSession() as session:
                timeout = aiohttp.ClientTimeout(total=10)
                params = {"instType": "SPOT"}
                async with session.get(self._rest_url, params=params, timeout=timeout) as resp:
                    data = await resp.json()

            symbols = []
            for item in data.get("data", []):
                inst_id = item.get("instId", "")
                state = item.get("state", "")
                # OKX uses "BTC-USDT" format
                quote = inst_id.split("-")[-1] if "-" in inst_id else ""

                if quote.upper() in self._quote_currencies and state == "live":
                    symbols.append(inst_id)

            symbols = sorted(set(symbols))
            logger.info(f"[okx] Fetched {len(symbols)} symbols from API (fallback)")
            return symbols
        except Exception as e:
            logger.error(f"[okx] API fallback failed: {e}")
            return []

    async def _fetch_symbols(self) -> List[str]:
        """Get trading symbols. Priority: JSON → API → hardcoded."""
        symbols = self._load_symbols_from_json()
        if symbols:
            return symbols

        symbols = await self._fetch_symbols_from_api()
        if symbols:
            return symbols

        logger.warning("[okx] Using hardcoded seed symbols (last resort)")
        return [
            "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "ADA-USDT",
            "DOGE-USDT", "AVAX-USDT", "DOT-USDT", "LINK-USDT", "LTC-USDT",
        ]

    # ------------------------------------------------------------------
    # WebSocket streaming
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        """
        Connect to OKX v5 WebSocket, subscribe to tickers,
        and emit RawTick events to the ingestion queue.
        """
        self._symbols = await self._fetch_symbols()

        if TEST_SYMBOLS_OVERRIDE is not None:
            self._symbols = TEST_SYMBOLS_OVERRIDE
            logger.info(f"[okx] ⚠️  TEST MODE: using {len(self._symbols)} symbols")

        if not self._symbols:
            logger.warning("[okx] No symbols — sleeping 30s")
            await asyncio.sleep(30)
            return

        logger.info(f"[okx] Subscribing to {len(self._symbols)} instruments")

        # OKX subscribe: one arg per instrument
        subscribe_msg = json.dumps({
            "op": "subscribe",
            "args": [
                {"channel": "tickers", "instId": sym} for sym in self._symbols
            ],
        })

        async with websockets.connect(self._ws_url, ping_interval=20) as ws:
            await ws.send(subscribe_msg)
            logger.info("[okx] Connected and subscribed")

            async for message in ws:
                data = json.loads(message)

                # Skip subscription confirmations / errors
                if "event" in data:
                    continue

                arg = data.get("arg", {})
                if arg.get("channel") != "tickers":
                    continue

                for item in data.get("data", []):
                    inst_id = item.get("instId", "")

                    try:
                        bid = float(item.get("bidPx", 0) or 0)
                        ask = float(item.get("askPx", 0) or 0)
                        last = float(item.get("last", 0) or 0)
                        volume = float(item.get("vol24h", 0) or 0)
                    except (ValueError, TypeError):
                        continue

                    tick = RawTick(
                        exchange="okx",
                        pair=inst_id,  # "BTC-USDT" format
                        data={
                            "bid": bid,
                            "ask": ask,
                            "last": last,
                            "vwap": None,
                            "volume_24h": volume,
                        },
                        received_at=time.time(),
                    )

                    await self._emit(tick)
