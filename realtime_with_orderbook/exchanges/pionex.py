"""
Pionex orderbook connector.

Subscribes to DEPTH topic with configurable limit.

Message format:
  {
    "topic": "DEPTH",
    "symbol": "BTC_USDT",
    "data": {
      "bids": [["65000.00", "1.5"], ...],
      "asks": [["65001.00", "2.0"], ...],
    },
    "timestamp": 1700000000000
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


class PionexOrderBookConnector(BaseExchange):
    NAME = "pionex"

    def __init__(self, queue: asyncio.Queue, pairs: Optional[List[str]] = None):
        super().__init__(queue)
        self._ws_url = config.PIONEX_WS_URL
        self._pairs = pairs or self._load_pairs()
        self._depth = config.ORDERBOOK_DEPTH

    def _load_pairs(self) -> List[str]:
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                data = json.load(fp)
            pairs = []
            for entry in data.get("assets", {}).values():
                sym = entry.get("exchange_symbols", {}).get("pionex")
                if sym:
                    pairs.append(f"{sym}_USDT")
            return sorted(set(pairs))
        except Exception as e:
            logger.warning(f"[pionex] Failed to load pairs: {e}")
            return ["BTC_USDT", "ETH_USDT", "SOL_USDT"]

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(self._ws_url, ping_interval=None) as ws:
            for pair in self._pairs:
                subscribe = {
                    "op": "SUBSCRIBE",
                    "topic": "DEPTH",
                    "symbol": pair,
                    "limit": self._depth,
                }
                await ws.send(json.dumps(subscribe))

            logger.info(f"[pionex] Subscribed to DEPTH for {len(self._pairs)} pairs")

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Handle Pionex PING/PONG
                if msg.get("op") == "PING":
                    pong = json.dumps({"op": "PONG", "timestamp": int(time.time() * 1000)})
                    await ws.send(pong)
                    continue

                # Skip subscription acknowledgment — no data, just a confirm
                if msg.get("type") == "SUBSCRIBED":
                    continue

                # Handle errors
                if msg.get("type") == "ERROR":
                    logger.warning(f"[pionex] Error: {msg.get('message', '')}")
                    continue

                topic = msg.get("topic")
                if topic != "DEPTH":
                    continue

                symbol = msg.get("symbol", "")
                data = msg.get("data", {})
                raw_bids = data.get("bids", [])
                raw_asks = data.get("asks", [])

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
