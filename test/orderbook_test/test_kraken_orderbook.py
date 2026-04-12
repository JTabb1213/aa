#!/usr/bin/env python3
"""
Kraken WebSocket orderbook test.

Connects to Kraken WebSocket v2 and subscribes to the order book channel
for a small set of USD pairs. Prints every orderbook message received.

Usage:
    cd /Users/jacktabb/Desktop/app
    python test/test_kraken_orderbook.py

Press Ctrl+C to stop.
"""

import asyncio
import json
import sys
import time

import websockets

KRAKEN_WS_URL = "wss://ws.kraken.com/v2"
TEST_PAIRS = [
    "BTC/USD",
    "ETH/USD",
    "SOL/USD",
]


async def run_orderbook_test():
    print("=" * 80)
    print("  Kraken WebSocket Orderbook Test")
    print("=" * 80)
    print(f"URL: {KRAKEN_WS_URL}")
    print(f"Pairs: {TEST_PAIRS}")
    print("=" * 80)
    print()

    subscribe_payload = {
        "method": "subscribe",
        "params": {
            "channel": "book",
            "symbol": TEST_PAIRS,
        },
    }

    tick_count = 0
    try:
        async with websockets.connect(KRAKEN_WS_URL, ping_interval=30) as ws:
            await ws.send(json.dumps(subscribe_payload))
            print("✓ Sent orderbook subscribe message")
            print("Waiting for orderbook events... (Ctrl+C to stop)")
            print("-" * 80)

            async for raw_message in ws:
                tick_count += 1
                try:
                    message = json.loads(raw_message)
                except json.JSONDecodeError:
                    print(f"[RAW] {raw_message}")
                    continue

                # Print every message
                print(json.dumps(message, indent=2, sort_keys=True))
                print("-" * 80)

                if tick_count % 20 == 0:
                    print(f"[info] Received {tick_count} orderbook messages so far")
                    print("-" * 80)

    except websockets.ConnectionClosed as e:
        print(f"\n✗ Connection closed: {e}")
    except KeyboardInterrupt:
        print(f"\n✓ Test stopped by user. Received {tick_count} messages.")


if __name__ == "__main__":
    asyncio.run(run_orderbook_test())
