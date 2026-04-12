#!/usr/bin/env python3
"""
MEXC orderbook test.

Connects to MEXC WebSocket and subscribes to depth updates for a few USDT pairs.
Prints every received message.

Usage:
    python test/orderbook_test/test_mexc_orderbook.py
"""

import asyncio
import json
import websockets

MEXC_WS_URL = "wss://wbs.mexc.com/ws"
TEST_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


async def run_mexc_orderbook_test():
    print("=" * 80)
    print("  MEXC Orderbook Test")
    print("=" * 80)
    print(f"URL: {MEXC_WS_URL}")
    print(f"Pairs: {TEST_PAIRS}")
    print("=" * 80)
    print()

    subscribe_msg = {
        "method": "SUBSCRIBE",
        "params": [
            f"spot@public.depth.v3.api@{pair}"
            for pair in TEST_PAIRS
        ],
        "id": 1,
    }

    tick_count = 0
    try:
        async with websockets.connect(MEXC_WS_URL, ping_interval=30) as ws:
            await ws.send(json.dumps(subscribe_msg))
            print("✓ Sent subscribe message")
            print("Waiting for depth messages... (Ctrl+C to stop)")
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
    asyncio.run(run_mexc_orderbook_test())
