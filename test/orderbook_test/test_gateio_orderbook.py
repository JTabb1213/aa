#!/usr/bin/env python3
"""
Gate.io orderbook test.

Connects to Gate.io WebSocket v4 and subscribes to the spot.order_book
channel for a few USDT pairs. Prints every received message.

Usage:
    python test/orderbook_test/test_gateio_orderbook.py
"""

import asyncio
import json
import time
import websockets

GATEIO_WS_URL = "wss://api.gateio.ws/ws/v4/"
TEST_PAIRS = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]


async def run_gateio_orderbook_test():
    print("=" * 80)
    print("  Gate.io Orderbook Test")
    print("=" * 80)
    print(f"URL: {GATEIO_WS_URL}")
    print(f"Pairs: {TEST_PAIRS}")
    print("=" * 80)
    print()

    subscribe_msg = {
        "time": int(time.time()),
        "channel": "spot.order_book",
        "event": "subscribe",
        "payload": TEST_PAIRS,
    }

    tick_count = 0
    try:
        async with websockets.connect(GATEIO_WS_URL, ping_interval=None) as ws:
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
    asyncio.run(run_gateio_orderbook_test())
