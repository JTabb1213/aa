#!/usr/bin/env python3
"""
Simple OKX WebSocket Test.

Connects to OKX's v5 WebSocket API and prints live ticker data.
This verifies the connection works before integrating into the pipeline.

OKX WebSocket docs:
    https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-tickers-channel

Usage:
    python test_okx_ws_simple.py

Press Ctrl+C to stop.
"""

import asyncio
import json
import websockets
from datetime import datetime


# OKX public WebSocket endpoint
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

# Symbols to subscribe to (OKX uses "BTC-USDT" format)
TEST_SYMBOLS = [
    "BTC-USDT",
    "ETH-USDT",
    "SOL-USDT",
    "XRP-USDT",
    "ADA-USDT",
    "DOGE-USDT",
    "AVAX-USDT",
    "DOT-USDT",
    "LINK-USDT",
    "LTC-USDT",
]


async def test_okx_ws():
    print("=" * 70)
    print("  OKX WebSocket Test")
    print("=" * 70)
    print(f"  URL: {OKX_WS_URL}")
    print(f"  Symbols: {TEST_SYMBOLS}")
    print("=" * 70)
    print()

    subscribe_msg = json.dumps({
        "op": "subscribe",
        "args": [
            {"channel": "tickers", "instId": sym} for sym in TEST_SYMBOLS
        ],
    })

    try:
        async with websockets.connect(OKX_WS_URL, ping_interval=20) as ws:
            print("✓ Connected to OKX WebSocket")
            await ws.send(subscribe_msg)
            print("✓ Subscription sent")
            print()
            print("Waiting for ticker updates (Ctrl+C to stop)...")
            print("-" * 70)

            tick_count = 0
            async for message in ws:
                data = json.loads(message)

                # Handle subscription confirmations
                if "event" in data:
                    event = data.get("event", "")
                    if event == "subscribe":
                        print(f"✓ Subscribed to {data.get('arg', {}).get('instId', '?')}")
                    elif event == "error":
                        print(f"✗ Error: {data.get('msg', 'unknown')}")
                    continue

                arg = data.get("arg", {})
                if arg.get("channel") != "tickers":
                    continue

                for item in data.get("data", []):
                    inst_id = item.get("instId", "")
                    last_price = item.get("last", "0")
                    bid = item.get("bidPx", "0")
                    ask = item.get("askPx", "0")
                    volume = item.get("vol24h", "0")

                    # OKX provides open24h so we can calculate change
                    open_24h = float(item.get("open24h", 0) or 0)
                    last_f = float(last_price)
                    change = ((last_f - open_24h) / open_24h * 100) if open_24h > 0 else 0

                    tick_count += 1
                    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

                    if change > 0:
                        color = "\033[92m"
                        arrow = "↑"
                    elif change < 0:
                        color = "\033[91m"
                        arrow = "↓"
                    else:
                        color = "\033[0m"
                        arrow = "→"
                    reset = "\033[0m"

                    print(
                        f"[{timestamp}] #{tick_count:>5}  "
                        f"{inst_id:<12}  "
                        f"${last_f:>12,.4f}  "
                        f"bid: {float(bid):>12,.4f}  "
                        f"ask: {float(ask):>12,.4f}  "
                        f"{color}{arrow} {change:+.2f}%{reset}"
                    )

    except websockets.exceptions.ConnectionClosed as e:
        print(f"\n✗ Connection closed: {e}")
    except Exception as e:
        print(f"\n✗ Error: {e}")


if __name__ == "__main__":
    print()
    try:
        asyncio.run(test_okx_ws())
    except KeyboardInterrupt:
        print("\n\n✓ Test stopped by user")
