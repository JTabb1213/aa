#!/usr/bin/env python3
"""
Bybit orderbook test.

Connects to Bybit spot websocket and subscribes to orderbook updates for a
small sample of symbols. Prints received messages so you can verify the
subscription format and live payload structure.

Usage:
    python3 test/orderbook_test/test_bybit_orderbook.py
"""

import asyncio
import json
import time

import websockets

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"
TEST_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
DEPTH = 50


async def run_bybit_orderbook_test():
    print("=" * 80)
    print("  Bybit Orderbook Test")
    print("=" * 80)
    print(f"URL: {BYBIT_WS_URL}")
    print(f"Symbols: {TEST_SYMBOLS}")
    print(f"Depth: {DEPTH}")
    print("=" * 80)
    print()

    tick_count = 0
    try:
        async with websockets.connect(BYBIT_WS_URL, ping_interval=20, open_timeout=10) as ws:
            subscribe = {
                "op": "subscribe",
                "args": [f"orderbook.{DEPTH}.{symbol}" for symbol in TEST_SYMBOLS],
            }
            await ws.send(json.dumps(subscribe))

            print("✓ Sent orderbook subscribe message")
            print("Waiting for orderbook messages... (Ctrl+C to stop)")
            print("-" * 80)

            async for raw in ws:
                tick_count += 1
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    print(f"[RAW] {raw}")
                    continue

                if message.get("op") == "pong" or message.get("ret_msg") == "pong":
                    continue

                print(json.dumps(message, indent=2, sort_keys=True))
                print("-" * 80)

                if tick_count % 10 == 0:
                    print(f"[info] Received {tick_count} messages")
                    print("-" * 80)

    except KeyboardInterrupt:
        print(f"\n✓ Test stopped by user. Received {tick_count} messages.")
    except Exception as exc:
        print(f"\n✗ Error: {exc}")


if __name__ == "__main__":
    asyncio.run(run_bybit_orderbook_test())
