"""
Coinbase Advanced Trade orderbook connector.

Requires JWT authentication (ES256 with EC private key).
Subscribes to the "level2" channel.

Message format:
  {
    "channel": "l2_data",
    "events": [{
      "type": "snapshot" | "update",
      "product_id": "BTC-USDT",
      "updates": [
        {"side": "bid", "price_level": "65000.00", "new_quantity": "1.5"},
        {"side": "offer", "price_level": "65001.00", "new_quantity": "2.0"},
      ]
    }]
  }
"""

import asyncio
import base64
import json
import logging
import os
import secrets
import time
from pathlib import Path
from typing import List, Optional

import websockets
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

import config
from core.models import RawOrderBook, PriceLevel
from exchanges.base import BaseExchange

logger = logging.getLogger(__name__)

JWT_TTL_SECS = 120  # Coinbase requires ≤120 seconds


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _create_jwt(key_id: str, private_key: ec.EllipticCurvePrivateKey) -> str:
    """Generate a Coinbase Advanced Trade JWT (matches official SDK)."""
    header = {"alg": "ES256", "typ": "JWT", "kid": key_id, "nonce": secrets.token_hex()}
    now = int(time.time())
    payload = {
        "sub": key_id,
        "iss": "cdp",
        "nbf": now,
        "exp": now + JWT_TTL_SECS,
    }

    encoded_header = _base64url_encode(json.dumps(header, separators=(",", ":")).encode())
    encoded_payload = _base64url_encode(json.dumps(payload, separators=(",", ":")).encode())
    message = f"{encoded_header}.{encoded_payload}".encode()

    sig_der = private_key.sign(message, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(sig_der)
    raw_sig = r.to_bytes(32, "big") + s.to_bytes(32, "big")

    return f"{encoded_header}.{encoded_payload}.{_base64url_encode(raw_sig)}"


def _load_private_key(key_data: str) -> ec.EllipticCurvePrivateKey:
    """Load EC private key from PEM string (handles \\n escapes)."""
    pem = key_data.replace("\\n", "\n").replace("\\r", "\r").encode()
    return serialization.load_pem_private_key(pem, password=None, backend=default_backend())


class CoinbaseOrderBookConnector(BaseExchange):
    NAME = "coinbase"

    def __init__(self, queue: asyncio.Queue, pairs: Optional[List[str]] = None):
        super().__init__(queue)
        self._ws_url = config.COINBASE_WS_URL
        self._pairs = pairs or self._load_pairs()
        self._key_id = config.COINBASE_ADT_KEY_ID
        self._private_key_raw = config.COINBASE_ADT_PRIVATE_KEY
        self._private_key_path = config.COINBASE_ADT_PRIVATE_KEY_PATH
        # Maintain local book per product for incremental updates
        self._books: dict = {}  # product_id -> {"bids": {price: qty}, "asks": {price: qty}}

    def _has_credentials(self) -> bool:
        return bool(self._key_id and (self._private_key_raw or self._private_key_path))

    async def run(self) -> None:
        """Override: exit permanently if credentials are not configured."""
        if not self._has_credentials():
            logger.warning(
                "[coinbase] No credentials configured — connector disabled. "
                "Set COINBASE_ADT_KEY_ID and COINBASE_ADT_PRIVATE_KEY in .env to enable."
            )
            return  # exit permanently, no reconnect loop
        await super().run()

    def _load_pairs(self) -> List[str]:
        """Load pairs from coin_aliases.json in Coinbase format (BTC-USDT)."""
        try:
            with open(config.ALIAS_JSON_PATH) as fp:
                data = json.load(fp)
            pairs = []
            for entry in data.get("assets", {}).values():
                sym = entry.get("exchange_symbols", {}).get("coinbase")
                if sym:
                    pairs.append(f"{sym}-USDT")
            return sorted(set(pairs))
        except Exception as e:
            logger.warning(f"[coinbase] Failed to load pairs: {e}")
            return ["BTC-USDT", "ETH-USDT", "SOL-USDT"]

    def _get_jwt(self) -> str:
        """Generate a fresh JWT for Coinbase authentication."""
        if not self._key_id:
            raise ValueError("COINBASE_ADT_KEY_ID not set")

        if self._private_key_raw:
            pk = _load_private_key(self._private_key_raw)
        elif self._private_key_path:
            pem_data = Path(self._private_key_path).read_bytes()
            pk = serialization.load_pem_private_key(pem_data, password=None, backend=default_backend())
        else:
            raise ValueError("No Coinbase private key configured")

        return _create_jwt(self._key_id, pk)

    async def _connect_and_stream(self) -> None:
        jwt = self._get_jwt()

        async with websockets.connect(self._ws_url, ping_interval=30) as ws:
            subscribe = {
                "type": "subscribe",
                "product_ids": self._pairs,
                "channel": "level2",
                "jwt": jwt,
            }
            await ws.send(json.dumps(subscribe))
            logger.info(f"[coinbase] Subscribed to level2 for {len(self._pairs)} pairs")

            # JWT expires after 120s — schedule renewal
            jwt_refresh_task = asyncio.create_task(
                self._refresh_jwt_loop(ws)
            )

            try:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Check for errors
                    if msg.get("type") == "error":
                        error_msg = msg.get("message", "unknown error")
                        logger.error(f"[coinbase] Error: {error_msg}")
                        if "authentication" in error_msg.lower():
                            raise ConnectionError(f"Coinbase auth error: {error_msg}")
                        continue

                    channel = msg.get("channel")
                    if channel != "l2_data":
                        continue

                    for event in msg.get("events", []):
                        event_type = event.get("type")
                        product_id = event.get("product_id", "")
                        updates = event.get("updates", [])

                        if event_type == "snapshot":
                            self._books[product_id] = {"bids": {}, "asks": {}}
                            book = self._books[product_id]
                            for u in updates:
                                price = float(u.get("price_level", 0))
                                qty = float(u.get("new_quantity", 0))
                                side = u.get("side", "")
                                if side == "bid" and qty > 0:
                                    book["bids"][price] = qty
                                elif side == "offer" and qty > 0:
                                    book["asks"][price] = qty

                        elif event_type == "update":
                            if product_id not in self._books:
                                continue
                            book = self._books[product_id]
                            for u in updates:
                                price = float(u.get("price_level", 0))
                                qty = float(u.get("new_quantity", 0))
                                side = u.get("side", "")
                                target = book["bids"] if side == "bid" else book["asks"]
                                if qty == 0:
                                    target.pop(price, None)
                                else:
                                    target[price] = qty
                        else:
                            continue

                        # Emit current book state
                        book = self._books.get(product_id, {"bids": {}, "asks": {}})
                        depth = config.ORDERBOOK_DEPTH
                        bids = sorted(
                            [PriceLevel(p, q) for p, q in book["bids"].items()],
                            key=lambda x: x.price, reverse=True,
                        )[:depth]
                        asks = sorted(
                            [PriceLevel(p, q) for p, q in book["asks"].items()],
                            key=lambda x: x.price,
                        )[:depth]

                        await self._emit(RawOrderBook(
                            exchange=self.NAME,
                            pair=product_id,  # e.g. "BTC-USDT"
                            bids=bids,
                            asks=asks,
                        ))
            finally:
                jwt_refresh_task.cancel()
                try:
                    await jwt_refresh_task
                except asyncio.CancelledError:
                    pass

    async def _refresh_jwt_loop(self, ws) -> None:
        """Re-authenticate every 90 seconds (JWT valid for 120s)."""
        while True:
            await asyncio.sleep(90)
            try:
                jwt = self._get_jwt()
                resubscribe = {
                    "type": "subscribe",
                    "product_ids": self._pairs,
                    "channel": "level2",
                    "jwt": jwt,
                }
                await ws.send(json.dumps(resubscribe))
                logger.info("[coinbase] JWT refreshed")
            except Exception as e:
                logger.error(f"[coinbase] JWT refresh failed: {e}")
