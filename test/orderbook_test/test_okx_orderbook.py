#!/usr/bin/env python3
"""
OKX orderbook test.

Connects to OKX and subscribes to the books channel for a few USDT pairs.
Prints every received message.

Usage:
    python test/orderbook_test/test_okx_orderbook.py
"""

import asyncio
import json
import websockets

OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
TEST_PAIRS = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]


async def run_okx_orderbook_test():
    print("=" * 80)
    print("  OKX Orderbook Test")
    print("=" * 80)
    print(f"URL: {OKX_WS_URL}")
    print(f"Pairs: {TEST_PAIRS}")
    print("=" * 80)
    print()

    subscribe_msg = {
        "op": "subscribe",
        "args": [
            {"channel": "books", "instId": pair}
            for pair in TEST_PAIRS
        ],
    }

    tick_count = 0
    try:
        async with websockets.connect(OKX_WS_URL, ping_interval=20) as ws:
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
    asyncio.run(run_okx_orderbook_test())
