"""
OKX orderbook connector (v5 public).

Subscribes to the "books5" channel (top-5 levels, pushed frequently).

Message format:
  {
    "arg": {"channel": "books5", "instId": "BTC-USDT"},
    "data": [{
      "bids": [["65000.0", "1.5", "0", "3"], ...],
      "asks": [["65001.0", "2.0", "0", "5"], ...],
      "ts": "1700000000000"
    }]
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


class OKXOrderBookConnector(BaseExchange):
    NAME = "okx"

    def __init__(self, queue: asyncio.Queue, pairs: Optional[List[str]] = None):
        super().__init__(queue)
        self._ws_url = config.OKX_WS_URL
        self._pairs = pairs or self._load_pairs()

    def _load_pairs(self) -> List[str]:
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                data = json.load(fp)
            pairs = []
            for entry in data.get("assets", {}).values():
                sym = entry.get("exchange_symbols", {}).get("okx")
                if sym:
                    pairs.append(f"{sym}-USDT")
            return sorted(set(pairs))
        except Exception as e:
            logger.warning(f"[okx] Failed to load pairs: {e}")
            return ["BTC-USDT", "ETH-USDT", "SOL-USDT"]

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(self._ws_url, ping_interval=20) as ws:
            subscribe = {
                "op": "subscribe",
                "args": [
                    {"channel": "books5", "instId": pair}
                    for pair in self._pairs
                ],
            }
            await ws.send(json.dumps(subscribe))
            logger.info(f"[okx] Subscribed to books5 for {len(self._pairs)} pairs")

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Skip subscription confirmations
                if "event" in msg:
                    continue

                arg = msg.get("arg", {})
                if arg.get("channel") != "books5":
                    continue

                inst_id = arg.get("instId", "")
                for snapshot in msg.get("data", []):
                    raw_bids = snapshot.get("bids", [])
                    raw_asks = snapshot.get("asks", [])

                    # OKX format: [price, qty, deprecated, numOrders]
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
                        pair=inst_id,  # e.g. "BTC-USDT"
                        bids=bids,
                        asks=asks,
                    ))
