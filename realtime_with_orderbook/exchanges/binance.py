"""
Binance.US orderbook connector.

Uses combined stream URL with depth snapshots:
  wss://stream.binance.us:9443/stream?streams=btcusdt@depth5@100ms

Message format:
  {
    "stream": "btcusdt@depth5@100ms",
    "data": {
      "lastUpdateId": 123456,
      "bids": [["65000.00", "1.500"], ...],
      "asks": [["65001.00", "2.000"], ...],
    }
  }
"""

import asyncio
import json
import logging
from typing import List, Optional

import websockets

import config
from core.models import RawOrderBook, PriceLevel
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)


class BinanceOrderBookConnector(BaseExchange):
    NAME = "binance"

    def __init__(self, queue: asyncio.Queue, symbols: Optional[List[str]] = None):
        super().__init__(queue)
        self._ws_base = config.BINANCE_WS_URL
        self._symbols = symbols or self._load_symbols()
        self._depth = config.ORDERBOOK_DEPTH

    def _load_symbols(self) -> List[str]:
        """Load symbols from coin_aliases.json in Binance format (btcusdt)."""
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                data = json.load(fp)
            symbols = []
            for entry in data.get("assets", {}).values():
                sym = entry.get("exchange_symbols", {}).get("binance")
                if sym:
                    symbols.append(f"{sym.lower()}usdt")
            return sorted(set(symbols))
        except Exception as e:
            logger.warning(f"[binance] Failed to load symbols: {e}")
            return ["btcusdt", "ethusdt", "solusdt"]

    async def _connect_and_stream(self) -> None:
        streams = "/".join(
            f"{sym}@depth{self._depth}@100ms" for sym in self._symbols
        )
        ws_url = f"{self._ws_base}/stream?streams={streams}"

        async with websockets.connect(ws_url, ping_interval=30) as ws:
            logger.info(f"[binance] Connected with {len(self._symbols)} streams")

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                data = msg.get("data", {})
                stream = msg.get("stream", "")

                raw_bids = data.get("bids", [])
                raw_asks = data.get("asks", [])
                if not raw_bids and not raw_asks:
                    continue

                # Extract symbol from stream name: "btcusdt@depth5@100ms"
                symbol = stream.split("@")[0].upper() if stream else ""

                bids = [
                    PriceLevel(float(b[0]), float(b[1]))
                    for b in raw_bids
                ]
                asks = [
                    PriceLevel(float(a[0]), float(a[1]))
                    for a in raw_asks
                ]

                await self._emit(RawOrderBook(
                    exchange=self.NAME,
                    pair=symbol,       # e.g. "BTCUSDT"
                    bids=bids,
                    asks=asks,
                ))
