#!/usr/bin/env python3
"""
Binance US orderbook test.

Connects to Binance.US and subscribes to depth updates for a few USDT pairs.
Prints every received message.

Usage:
    python test/orderbook_test/test_binance_orderbook.py
"""

import asyncio
import json
import websockets

BINANCE_WS_URL = "wss://stream.binance.us:9443/ws"
TEST_PAIRS = ["btcusdt", "ethusdt", "solusdt"]


async def run_binance_orderbook_test():
    streams = "/".join(f"{pair}@depth5@100ms" for pair in TEST_PAIRS)
    ws_url = f"{BINANCE_WS_URL}?streams={streams}"

    print("=" * 80)
    print("  Binance US Orderbook Test")
    print("=" * 80)
    print(f"URL: {ws_url}")
    print(f"Pairs: {TEST_PAIRS}")
    print("=" * 80)
    print()

    tick_count = 0
    try:
        async with websockets.connect(ws_url, ping_interval=30) as ws:
            print("✓ Connected")
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
    asyncio.run(run_binance_orderbook_test())
