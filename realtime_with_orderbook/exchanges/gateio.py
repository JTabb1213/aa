"""
Gate.io orderbook connector (WebSocket v4).

Subscribes to spot.order_book channel with depth and update interval.

Message format:
  {
    "channel": "spot.order_book",
    "event": "update",
    "result": {
      "t": 1700000000000,
      "s": "BTC_USDT",
      "bids": [["65000.00", "1.5"], ...],
      "asks": [["65001.00", "2.0"], ...],
    }
  }
"""

import asyncio
import json
import logging
import time
from typing import List, Optional

import websockets

import config
from core.models import RawOrderBook, PriceLevel
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)


class GateioOrderBookConnector(BaseExchange):
    NAME = "gateio"

    def __init__(self, queue: asyncio.Queue, pairs: Optional[List[str]] = None):
        super().__init__(queue)
        self._ws_url = config.GATEIO_WS_URL
        self._pairs = pairs or self._load_pairs()
        self._depth = config.ORDERBOOK_DEPTH

    def _load_pairs(self) -> List[str]:
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                data = json.load(fp)
            pairs = []
            for entry in data.get("assets", {}).values():
                sym = entry.get("exchange_symbols", {}).get("gateio")
                if sym:
                    pairs.append(f"{sym}_USDT")
            return sorted(set(pairs))
        except Exception as e:
            logger.warning(f"[gateio] Failed to load pairs: {e}")
            return ["BTC_USDT", "ETH_USDT", "SOL_USDT"]

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(self._ws_url, ping_interval=None) as ws:
            # Gate.io requires one subscribe per pair with [symbol, level, interval]
            for pair in self._pairs:
                subscribe = {
                    "time": int(time.time()),
                    "channel": "spot.order_book",
                    "event": "subscribe",
                    "payload": [pair, str(self._depth), "100ms"],
                }
                await ws.send(json.dumps(subscribe))

            logger.info(f"[gateio] Subscribed to order_book for {len(self._pairs)} pairs")

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                channel = msg.get("channel")
                event = msg.get("event")

                if channel != "spot.order_book" or event != "update":
                    # Check for errors
                    if msg.get("result", {}).get("status") == "fail":
                        err = msg.get("error", {}).get("message", "unknown")
                        logger.warning(f"[gateio] Subscribe failed: {err}")
                    continue

                result = msg.get("result", {})
                symbol = result.get("s", "")
                raw_bids = result.get("bids", [])
                raw_asks = result.get("asks", [])

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
                    pair=symbol,  # e.g. "BTC_USDT"
                    bids=bids,
                    asks=asks,
                ))
