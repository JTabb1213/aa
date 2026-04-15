#!/usr/bin/env python3
"""
Coinbase orderbook test.

Connects to the Coinbase Advanced Trade WebSocket feed and subscribes to the
level2 orderbook channel for a few USDT pairs.

This test loads authentication details from a local .env file in the same
directory and generates the required JWT automatically.

Usage:
    python test/orderbook_test/test_coinbase_orderbook.py
"""

import asyncio
import base64
import json
import os
import secrets
import sys
import time
from pathlib import Path

import websockets
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
)

COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
TEST_PAIRS = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
ENV_FILENAME = ".env"
KEY_ID_VAR = "COINBASE_ADT_KEY_ID"
PRIVATE_KEY_VAR = "COINBASE_ADT_PRIVATE_KEY"
PRIVATE_KEY_PATH_VAR = "COINBASE_ADT_PRIVATE_KEY_PATH"
JWT_TTL_SECS = 120  # Coinbase requires ≤120 seconds


def _load_dotenv(env_path: Path) -> None:
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _load_private_key_from_bytes(pem_data: bytes) -> ec.EllipticCurvePrivateKey:
    try:
        return serialization.load_pem_private_key(
            pem_data,
            password=None,
            backend=default_backend(),
        )
    except Exception as exc:
        print(f"ERROR: Failed to load private key: {exc}")
        sys.exit(1)


def _load_private_key(private_key_path: Path) -> ec.EllipticCurvePrivateKey:
    if not private_key_path.exists():
        print(f"ERROR: Private key file not found: {private_key_path}")
        sys.exit(1)

    pem_data = private_key_path.read_bytes()
    return _load_private_key_from_bytes(pem_data)


def _create_jwt(key_id: str, private_key: ec.EllipticCurvePrivateKey) -> str:
    header = {"alg": "ES256", "typ": "JWT", "kid": key_id, "nonce": secrets.token_hex()}
    now = int(time.time())
    payload = {
        "sub": key_id,
        "iss": "cdp",
        "nbf": now,
        "exp": now + JWT_TTL_SECS,
    }

    encoded_header = _base64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    encoded_payload = _base64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    message = f"{encoded_header}.{encoded_payload}".encode("utf-8")

    signature_der = private_key.sign(message, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(signature_der)
    raw_signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")

    encoded_signature = _base64url_encode(raw_signature)
    return f"{encoded_header}.{encoded_payload}.{encoded_signature}"


def _unescape_env_value(value: str) -> str:
    return value.replace("\\n", "\n").replace("\\r", "\r")


def _load_auth() -> str:
    env_path = Path(__file__).resolve().parent / ENV_FILENAME
    _load_dotenv(env_path)

    key_id = os.getenv(KEY_ID_VAR, "").strip()
    private_key_value = os.getenv(PRIVATE_KEY_VAR, "").strip()
    private_key_path = os.getenv(PRIVATE_KEY_PATH_VAR, "").strip()

    if not key_id or not (private_key_value or private_key_path):
        print("ERROR: Missing Coinbase Advanced Trade auth settings.")
        print(f"Please set {KEY_ID_VAR} and either {PRIVATE_KEY_VAR} or {PRIVATE_KEY_PATH_VAR} in {env_path}")
        sys.exit(1)

    if private_key_value:
        pem_text = _unescape_env_value(private_key_value)
        private_key = _load_private_key_from_bytes(pem_text.encode("utf-8"))
    else:
        private_key_file = Path(private_key_path)
        if not private_key_file.is_absolute():
            private_key_file = env_path.parent / private_key_file
        private_key = _load_private_key(private_key_file)

    return _create_jwt(key_id, private_key)


async def run_coinbase_orderbook_test():
    print("=" * 80)
    print("  Coinbase Advanced Trade Orderbook Test")
    print("=" * 80)
    print(f"URL: {COINBASE_WS_URL}")
    print(f"Pairs: {TEST_PAIRS}")
    print("=" * 80)
    print()

    jwt = _load_auth()
    
    # Debug: print JWT parts for inspection
    try:
        parts = jwt.split(".")
        if len(parts) == 3:
            payload_b64 = parts[1]
            padding = "=" * (4 - len(payload_b64) % 4)
            payload_json = base64.urlsafe_b64decode(payload_b64 + padding).decode("utf-8")
            print(f"[DEBUG] JWT Payload: {payload_json}")
    except Exception as e:
        print(f"[DEBUG] Could not decode JWT: {e}")
    
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": TEST_PAIRS,
        "channel": "level2",
        "jwt": jwt,
    }

    tick_count = 0
    try:
        async with websockets.connect(COINBASE_WS_URL, ping_interval=30) as ws:
            await ws.send(json.dumps(subscribe_msg))
            print("✓ Sent subscribe message")
            print("Waiting for orderbook messages... (Ctrl+C to stop)")
            print("-" * 80)

            async for raw in ws:
                tick_count += 1
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    print(f"[RAW] {raw}")
                    continue

                print(json.dumps(message, indent=2, sort_keys=True))
                print("-" * 80)

                if tick_count % 20 == 0:
                    print(f"[info] Received {tick_count} messages")
                    print("-" * 80)

    except KeyboardInterrupt:
        print(f"\n✓ Test stopped by user. Received {tick_count} messages.")
    except Exception as exc:
        print(f"\n✗ Error: {exc}")


if __name__ == "__main__":
    asyncio.run(run_coinbase_orderbook_test())
