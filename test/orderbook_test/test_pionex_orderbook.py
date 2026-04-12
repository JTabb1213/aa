#!/usr/bin/env python3
"""
Pionex orderbook test.

Connects to the Pionex US WebSocket and subscribes to DEPTH updates for
a few USDT pairs. Prints every received message.

Usage:
    python test/orderbook_test/test_pionex_orderbook.py
"""

import asyncio
import json
import time
import websockets

PIONEX_WS_URL = "wss://ws.pionex.us/wsPub"
TEST_PAIRS = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]


async def run_pionex_orderbook_test():
    print("=" * 80)
    print("  Pionex Orderbook Test")
    print("=" * 80)
    print(f"URL: {PIONEX_WS_URL}")
    print(f"Pairs: {TEST_PAIRS}")
    print("=" * 80)
    print()

    tick_count = 0
    try:
        async with websockets.connect(PIONEX_WS_URL, ping_interval=None) as ws:
            for pair in TEST_PAIRS:
                subscribe_msg = {
                    "op": "SUBSCRIBE",
                    "topic": "DEPTH",
                    "symbol": pair,
                }
                await ws.send(json.dumps(subscribe_msg))

            print("✓ Sent DEPTH subscribe messages")
            print("Waiting for depth messages... (Ctrl+C to stop)")
            print("-" * 80)

            async for raw in ws:
                tick_count += 1
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    print(f"[RAW] {raw}")
                    continue

                # Handle server PING for custom PIONEX ping/pong protocol
                if message.get("op") == "PING":
                    pong = json.dumps({"op": "PONG", "timestamp": int(time.time() * 1000)})
                    await ws.send(pong)
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
    asyncio.run(run_pionex_orderbook_test())
